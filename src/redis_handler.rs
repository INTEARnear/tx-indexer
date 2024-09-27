use async_trait::async_trait;
use inevents_redis::RedisEventStream;
use intear_events::events::transactions::tx_receipt::{TxReceiptEvent, TxReceiptEventData};
use intear_events::events::transactions::tx_transaction::{
    TxTransactionEvent, TxTransactionEventData,
};
use redis::aio::ConnectionManager;

use crate::TxEventHandler;

pub struct PushToRedisStream {
    transaction_stream: RedisEventStream<TxTransactionEventData>,
    receipt_stream: RedisEventStream<TxReceiptEventData>,
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
    async fn handle_transaction(&mut self, event: TxTransactionEventData) {
        self.transaction_stream
            .emit_event(event.block_height, event, self.max_stream_size)
            .await
            .expect("Failed to emit text event");
    }

    async fn handle_receipt(&mut self, event: TxReceiptEventData) {
        self.receipt_stream
            .emit_event(event.block_height, event, self.max_stream_size)
            .await
            .expect("Failed to emit nep297 event");
    }
}
