use crate::config::{Committee, Stake};
use crate::consensus::{ConsensusMessage, Round};
use crate::messages::{Block, QC, TC};
use bytes::Bytes;
//se rand::Rng;
use crypto::{Digest, PublicKey, SignatureService};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, info};
use network::{CancelHandler, ReliableSender};
use std::collections::HashSet;
use tokio::sync::mpsc::{Receiver, Sender};


#[derive(Debug)]
pub enum ProposerMessage {
    Make(Round, QC, Option<TC>),
    Cleanup(Vec<Digest>),
}

pub struct Proposer {
    name: PublicKey,
    committee: Committee,
    //parameters: Parameters,
    signature_service: SignatureService,
    rx_mempool: Receiver<Digest>,
    rx_message: Receiver<ProposerMessage>,
    tx_loopback: Sender<Block>,
    buffer: HashSet<Digest>,
    data_buffer: HashSet<Digest>,
    max_block_data_bytes: usize,
    target_payload_size: usize,
    network: ReliableSender,
}

impl Proposer {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        max_block_data_bytes: usize,
        target_payload_size: usize,
        signature_service: SignatureService,
        rx_mempool: Receiver<Digest>,
        rx_message: Receiver<ProposerMessage>,
        tx_loopback: Sender<Block>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                signature_service,
                rx_mempool,
                rx_message,
                tx_loopback,
                buffer: HashSet::new(),
                data_buffer: HashSet::new(),
                max_block_data_bytes,
                target_payload_size,
                network: ReliableSender::new(),
            }
            .run()
            .await;
        });
    }

    /// Helper function. It waits for a future to complete and then delivers a value.
    async fn waiter(wait_for: CancelHandler, deliver: Stake) -> Stake {
        let _ = wait_for.await;
        deliver
    }

    async fn make_block(&mut self, round: Round, qc: QC, tc: Option<TC>) {

        // want to put in负载总大小
        let mut current_data_size: usize = 0;

        // 生成虚拟的无用数据，填充到目标大小
        while current_data_size < self.max_block_data_bytes {
            // 生成一个随机的伪 Digest（填充数据）
            let fake_data = Digest(rand::random::<[u8; 32]>());
            self.data_buffer.insert(fake_data.clone()); // 插入到缓冲区
            current_data_size += fake_data.size(); // 更新当前负载大小
        }

        let mut current_payload_size: usize = self.buffer.iter().map(|d| d.size()).sum();

        while current_payload_size < self.target_payload_size {
        // 生成一个随机的伪 Digest（填充数据）
            let fake_data = Digest(rand::random::<[u8; 32]>());
            self.buffer.insert(fake_data.clone()); // 插入到缓冲区
            current_payload_size += fake_data.size(); // 更新当前负载大小
        }

            // Generate a new block.
            let block = Block::new(
                qc,
                tc,
                self.name,
                round,
                /* payload */ self.buffer.drain().collect(),
                /* data */ self.data_buffer.drain().collect(),
                self.signature_service.clone(),
            )
            .await;
        
        if !block.payload.is_empty() {
            info!("Created {}", block);

            #[cfg(feature = "benchmark")]
            for x in &block.payload {
                // NOTE: This log entry is used to compute performance.
                info!("Created {} -> {:?}", block, x);
            }
        }
        debug!("Created {:?}", block);

        // Broadcast our new block.
        debug!("Broadcasting {:?}", block);
        let (names, addresses): (Vec<_>, _) = self
            .committee
            .broadcast_addresses(&self.name)
            .iter()
            .cloned()
            .unzip();
        let message = bincode::serialize(&ConsensusMessage::Propose(block.clone()))
            .expect("Failed to serialize block");
        let handles = self
            .network
            .broadcast(addresses, Bytes::from(message))
            .await;

        // Send our block to the core for processing.
        self.tx_loopback
            .send(block)
            .await
            .expect("Failed to send block");

        self.data_buffer.clear();
        //self.buffer.clear();
        // Control system: Wait for 2f+1 nodes to acknowledge our block before continuing.
        let mut wait_for_quorum: FuturesUnordered<_> = names
            .into_iter()
            .zip(handles.into_iter())
            .map(|(name, handler)| {
                let stake = self.committee.stake(&name);
                Self::waiter(handler, stake)
            })
            .collect();

        let mut total_stake = self.committee.stake(&self.name);
        while let Some(stake) = wait_for_quorum.next().await {
            total_stake += stake;
            if total_stake >= self.committee.quorum_threshold() {
                break;
            }
        }
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(digest) = self.rx_mempool.recv() => {
                    //if self.buffer.len() < 82 {
                        self.buffer.insert(digest);
                        //debug!("buffer_size: {}", self.buffer.len());
                    //}
                },
                Some(message) = self.rx_message.recv() => match message {
                    ProposerMessage::Make(round, qc, tc) => self.make_block(round, qc, tc).await,
                    ProposerMessage::Cleanup(digests) => {
                        for x in &digests {
                            self.buffer.remove(x);
                        }
                    }
                }
            }
        }
    }
}
