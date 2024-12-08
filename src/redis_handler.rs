use async_trait::async_trait;
use inevents_redis::RedisEventStream;
use inindexer::near_indexer_primitives::types::BlockHeight;
use intear_events::events::transactions::{
    tx_receipt::TxReceiptEvent, tx_transaction::TxTransactionEvent,
};
use redis::aio::ConnectionManager;

use crate::TxEventHandler;

pub struct PushToRedisStream {
    transaction_stream: RedisEventStream<TxTransactionEvent>,
    receipt_stream: RedisEventStream<TxReceiptEvent>,
    max_stream_size: usize,
}

impl PushToRedisStream {
    pub async fn new(connection: ConnectionManager, max_stream_size: usize, testnet: bool) -> Self {
        Self {
            transaction_stream: RedisEventStream::new(
                connection.clone(),
                if testnet {
                    format!("{}_testnet", TxTransactionEvent::ID)
                } else {
                    TxTransactionEvent::ID.to_string()
                },
            ),
            receipt_stream: RedisEventStream::new(
                connection.clone(),
                if testnet {
                    format!("{}_testnet", TxReceiptEvent::ID)
                } else {
                    TxReceiptEvent::ID.to_string()
                },
            ),
            max_stream_size,
        }
    }
}

#[async_trait]
impl TxEventHandler for PushToRedisStream {
    async fn handle_transaction(&mut self, event: TxTransactionEvent) {
        self.transaction_stream.add_event(event);
    }

    async fn handle_receipt(&mut self, event: TxReceiptEvent) {
        self.receipt_stream.add_event(event);
    }

    async fn flush_events(&mut self, block_height: BlockHeight) {
        self.transaction_stream
            .flush_events(block_height, self.max_stream_size)
            .await
            .expect("Failed to flush transaction stream");
        self.receipt_stream
            .flush_events(block_height, self.max_stream_size)
            .await
            .expect("Failed to flush receipt stream");
    }
}
