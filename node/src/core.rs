use crate::aggregator::Aggregator;
use crate::config::{Committee, Parameters};
use crate::crypto::Hash as _;
use crate::crypto::{Digest, PublicKey, SignatureService};
use crate::error::{ConsensusError, ConsensusResult};
use crate::leader::LeaderElector;
use crate::mempool::Mempool;
use crate::messages::{Block, GenericQC, Vote, QC, TC};
use crate::network::NetMessage;
use crate::store::Store;
use crate::synchronizer::Synchronizer;
use crate::timer::{TimerId, TimerManager};
use futures::future::FutureExt as _;
use futures::select;
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use std::cmp::max;
use tokio::sync::mpsc::{channel, Receiver, Sender};

#[cfg(test)]
#[path = "tests/core_tests.rs"]
pub mod core_tests;

pub type RoundNumber = u64;

#[derive(Serialize, Deserialize, Debug)]
pub enum CoreMessage {
    Propose(Block),
    Vote(Vote),
    LoopBack(Block),
    SyncRequest(Digest, PublicKey),
}

pub struct Core {
    name: PublicKey,
    committee: Committee,
    parameters: Parameters,
    store: Store,
    signature_service: SignatureService,
    leader_elector: LeaderElector,
    mempool: Mempool,
    loopback_channel: Sender<CoreMessage>,
    timer_channel: Sender<TimerId>,
    network_channel: Sender<NetMessage>,
    commit_channel: Sender<Block>,
    round: RoundNumber,
    last_voted_round: RoundNumber,
    preferred_round: RoundNumber,
    highest_qc: QC,
    synchronizer: Synchronizer,
    aggregator: Aggregator,
    timer_manager: TimerManager,
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub async fn make(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        store: Store,
        signature_service: SignatureService,
        leader_elector: LeaderElector,
        mempool: Mempool,
        network_channel: Sender<NetMessage>,
        commit_channel: Sender<Block>,
    ) -> Sender<CoreMessage> {
        let (tx_core, rx_core) = channel(1000);

        // Make a timer manager instance allowing to schedule and cancel timers.
        // We communicate with the timer manager with a dedicated channel.
        let timer_manager = TimerManager::new().await;
        let (tx_timer, rx_timer) = channel(100);

        // Make the synchronizer. This instance runs in a background thread
        // and asks other nodes for any block that we may be missing.
        let synchronizer = Synchronizer::new(
            name,
            store.clone(),
            network_channel.clone(),
            tx_core.clone(),
            timer_manager.clone(),
            parameters.sync_retry_delay,
        )
        .await;

        // Make a votes aggregator. This is the instance that keeps track
        // of incoming votes and aggregates them into QCs.
        let aggregator = Aggregator::new(committee.clone());

        // Run the core in a separate thread.
        let loopback_channel = tx_core.clone();
        tokio::spawn(async move {
            let mut core = Self {
                name,
                committee,
                parameters,
                store,
                signature_service,
                leader_elector,
                mempool,
                loopback_channel,
                timer_channel: tx_timer,
                network_channel,
                commit_channel,
                round: 0,
                last_voted_round: 0,
                preferred_round: 0,
                highest_qc: QC::genesis(),
                synchronizer,
                aggregator,
                timer_manager,
            };
            core.run(rx_core, rx_timer).await;
        });

        // Return sender channel. The network receiver will use it to
        // send us new messages to process.
        tx_core
    }

    async fn store_block(&mut self, block: &Block) -> ConsensusResult<()> {
        let key = block.digest().to_vec();
        let value = bincode::serialize(block).expect("Failed to serialize block");
        self.store
            .write(key, value)
            .await
            .map_err(ConsensusError::from)
    }

    async fn schedule_timer(&mut self) {
        let timer_id = format!("core:{}", self.round);
        self.timer_manager
            .schedule(
                self.parameters.timeout_delay,
                timer_id,
                self.timer_channel.clone(),
            )
            .await;
    }

    async fn make_block(
        &mut self,
        qc: QC,
        tc: Option<TC>,
        round: RoundNumber,
    ) -> ConsensusResult<()> {
        let block = Block::new(
            qc,
            tc,
            self.name,
            round,
            self.mempool.get_payload().await,
            self.signature_service.clone(),
        )
        .await;
        let message = CoreMessage::LoopBack(block.clone());
        if let Err(e) = self.loopback_channel.send(message).await {
            panic!("Core failed to loopback message to itself: {}", e);
        }
        let message = NetMessage::Block(block);
        if let Err(e) = self.network_channel.send(message).await {
            panic!("Core failed to send block to the network: {}", e);
        }
        Ok(())
    }

    async fn handle_propose(&mut self, block: &Block) -> ConsensusResult<()> {
        // Reject old blocks.
        if block.round <= self.round {
            return Ok(());
        }

        // Check the block's round number is as expected. This prevents bad leaders
        // from proposing blocks with very high round numbers which may cause overflows.
        let ok = match block.tc {
            Some(ref tc) => block.round == tc.round + 1,
            None => block.round == block.qc.round + 1,
        };
        ensure!(ok, ConsensusError::MalformedBlock(block.digest()));

        // Ensure the block proposer is the right leader for the round.
        ensure!(
            block.author == self.leader_elector.get_leader(block.round),
            ConsensusError::WrongLeader {
                digest: block.digest(),
                leader: block.author,
                round: block.round
            }
        );

        // Check the block is correctly signed.
        block.signature.verify(&block.digest(), &block.author)?;

        // Check that the QC embedded in the block is valid.
        if block.qc != QC::genesis() {
            block.qc.verify(&self.committee)?;
        }

        // Check the TC embedded in the block if any.
        if let Some(tc) = &block.tc {
            tc.verify(&self.committee)?;
        }

        // If all check pass, process the block.
        self.process_block(&block).await
    }

    async fn process_block(&mut self, block: &Block) -> ConsensusResult<()> {
        // Let's see if we have the block's data. If we don't, the mempool
        // will get it and them make us resume processing this block.
        if !self.mempool.ready(&block.payload).await {
            return Ok(());
        }

        // Let's see if we have the last three ancestors of the block, that is:
        //      b0 <- |qc0; b1| <- |qc1; b2| <- |qc2; block|
        // If we don't, the synchronizer asks for them to other nodes. It will
        // then ensure we process all three ancestors in the correct order, and
        // finally make us resume processing this block.
        let (b0, b1, b2) = match self.synchronizer.get_ancestors(block).await? {
            Some(ancestors) => ancestors,
            None => return Ok(()),
        };

        // If we have all ancestors we 'deliver' the block by adding it to store.
        // Delivering a block means we already processed all its ancestors.
        self.store_block(block).await?;

        // Enter the new round.
        let possible_new_round = match block.tc {
            Some(ref tc) => tc.round + 1,
            None => block.qc.round + 1,
        };
        if self.round < possible_new_round {
            // Cancel the timeout timer for this round and update the round number.
            let timer_id = format!("core:{}", self.round);
            self.timer_manager.cancel(timer_id).await;
            self.round = possible_new_round;
            info!("Moved to round {}", self.round);

            // Cleanup the vote aggregator.
            self.aggregator.cleanup(&self.round);

            // Schedule a new timer for this round.
            self.schedule_timer().await;
        }

        // Update the highest QC we know.
        if block.qc.round > self.highest_qc.round {
            self.highest_qc = block.qc.clone();
        }

        // Check if the last three ancestors of the block form a 3-chain.
        // If so, we commit b0.
        let mut commit_rule = b0.round + 1 == b1.round;
        commit_rule &= b1.round + 1 == b2.round;
        commit_rule &= b2.round + 1 == block.round;
        if commit_rule {
            info!("Committed {:?}", b0);
            if let Err(e) = self.commit_channel.send(b0.clone()).await {
                warn!("Failed to send block through the commit channel: {}", e);
            }
        }

        // Check the safety rules to see if we can vote for this new block. If we can,
        // we send our vote to the next leader.
        let safety_rule_1 = b2.round >= self.preferred_round;
        let safety_rule_2 = block.round > self.last_voted_round;
        if safety_rule_1 && safety_rule_2 {
            debug!("Voting for block {:?}", block);

            let vote = Vote::new(&block, self.name, self.signature_service.clone()).await;
            let next_leader = self.leader_elector.get_leader(self.round + 1);
            if next_leader == self.name {
                let message = CoreMessage::Vote(vote.clone());
                if let Err(e) = self.loopback_channel.send(message).await {
                    panic!("Core failed to loopback message to itself: {}", e);
                }
            } else {
                let message = NetMessage::Vote(vote, next_leader);
                if let Err(e) = self.network_channel.send(message).await {
                    panic!("Core failed to send vote to the network: {}", e);
                }
            }

            // Finally, update our state to ensure we won't vote for conflicting blocks.
            self.preferred_round = max(self.preferred_round, b1.round);
            self.last_voted_round = block.round;
        }

        Ok(())
    }

    async fn handle_vote(&mut self, vote: Vote) -> ConsensusResult<()> {
        if vote.round < self.round {
            return Ok(());
        }
        // Add the new vote to our aggregator and see if we have a quorum.
        if let Some(quorum) = self.aggregator.add_vote(vote.clone())? {
            // We propose a new block if we have a QC or TC, and if we are
            // the leader of the next round.
            let next_round = vote.round + 1;
            if self.name == self.leader_elector.get_leader(next_round) {
                let (qc, tc) = if vote.timeout() {
                    let tc = TC {
                        round: vote.round,
                        votes: quorum,
                    };
                    (self.highest_qc.clone(), Some(tc))
                } else {
                    let qc = QC {
                        hash: vote.hash,
                        round: vote.round,
                        votes: quorum,
                    };
                    (qc, None)
                };
                self.make_block(qc, tc, next_round).await?;
            }
        }
        Ok(())
    }

    async fn make_timeout(&mut self) {
        self.round += 1;
        info!("Moved to round {}", self.round);
        let timeout =
            Vote::new_timeout(self.round, self.name, self.signature_service.clone()).await;
        let next_leader = self.leader_elector.get_leader(self.round + 1);
        if next_leader == self.name {
            let message = CoreMessage::Vote(timeout.clone());
            if let Err(e) = self.loopback_channel.send(message).await {
                panic!("Core failed to loopback message to itself: {}", e);
            }
        } else {
            let message = NetMessage::Vote(timeout, next_leader);
            if let Err(e) = self.network_channel.send(message).await {
                panic!("Core failed to send vote to the network: {}", e);
            }
        }
        self.schedule_timer().await;
    }

    async fn handle_sync_request(
        &mut self,
        digest: Digest,
        sender: PublicKey,
    ) -> ConsensusResult<()> {
        if let Some(bytes) = self.store.read(digest.to_vec()).await? {
            let block = bincode::deserialize(&bytes)?;
            let message = NetMessage::SyncReply(block, sender);
            if let Err(e) = self.network_channel.send(message).await {
                panic!("Core failed to send sync reply to the network: {}", e);
            }
        }
        Ok(())
    }

    async fn run(&mut self, mut rx_core: Receiver<CoreMessage>, mut rx_timer: Receiver<TimerId>) {
        // Upon booting, send the very first block (if we are the leader).
        // and schedule a timer in case we don't hear from the leader.
        self.schedule_timer().await;
        if self.name == self.leader_elector.get_leader(1) {
            self.make_block(self.highest_qc.clone(), None, 1)
                .await
                .expect("Failed to send the first block");
        }

        // This is the main loop: it processes incoming blocks and votes.
        loop {
            select! {
                message = rx_core.recv().fuse() => {
                    if let Some(message) = message {
                        debug!("Received {:?}", message);
                        let result = match message {
                            CoreMessage::Propose(block) => self.handle_propose(&block).await,
                            CoreMessage::Vote(vote) => self.handle_vote(vote).await,
                            CoreMessage::LoopBack(block) => self.process_block(&block).await,
                            CoreMessage::SyncRequest(digest, sender) => self.handle_sync_request(digest, sender).await
                        };
                        match result {
                            Ok(()) => (),
                            Err(ConsensusError::StoreError(e)) => error!("{}", e),
                            Err(ConsensusError::SerializationError(e)) => error!("Store corrupted. {}", e),
                            Err(e) => warn!("{}", e),
                        }
                    }
                },
                message = rx_timer.recv().fuse() => {
                    if message.is_some() {
                        warn!("Timing out for round {}!", self.round);
                        self.make_timeout().await
                    }
                }
            }
        }
    }
}