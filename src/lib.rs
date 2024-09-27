pub mod redis_handler;

use async_trait::async_trait;
use inindexer::near_indexer_primitives::views::ExecutionStatusView;
use inindexer::near_indexer_primitives::StreamerMessage;
use inindexer::{CompleteTransaction, IncompleteTransaction, Indexer, TransactionReceipt};
use intear_events::events::transactions::tx_receipt::TxReceiptEventData;
use intear_events::events::transactions::tx_transaction::TxTransactionEventData;

#[async_trait]
pub trait TxEventHandler: Send + Sync {
    async fn handle_transaction(&mut self, event: TxTransactionEventData);
    async fn handle_receipt(&mut self, event: TxReceiptEventData);
}

pub struct TxIndexer<T: TxEventHandler + Send + Sync + 'static>(pub T);

#[async_trait]
impl<T: TxEventHandler + Send + Sync + 'static> Indexer for TxIndexer<T> {
    type Error = String;

    async fn on_receipt(
        &mut self,
        receipt: &TransactionReceipt,
        transaction: &IncompleteTransaction,
        _block: &StreamerMessage,
    ) -> Result<(), Self::Error> {
        let receipt_event = TxReceiptEventData {
            block_timestamp_nanosec: receipt.block_timestamp_nanosec,
            block_height: receipt.block_height,
            receipt_id: receipt.receipt.receipt.receipt_id,
            transaction_id: transaction.transaction.transaction.hash,
            executor_id: receipt
                .receipt
                .execution_outcome
                .outcome
                .executor_id
                .clone(),
            success: match receipt.receipt.execution_outcome.outcome.status {
                ExecutionStatusView::SuccessValue(_) | ExecutionStatusView::SuccessReceiptId(_) => {
                    Some(true)
                }
                ExecutionStatusView::Failure(_) => Some(false),
                ExecutionStatusView::Unknown => None,
            },
        };
        self.0.handle_receipt(receipt_event).await;
        Ok(())
    }

    async fn on_transaction(
        &mut self,
        transaction: &CompleteTransaction,
        block: &StreamerMessage,
    ) -> Result<(), Self::Error> {
        let transaction_event = TxTransactionEventData {
            block_timestamp_nanosec: block.block.header.timestamp_nanosec as u128,
            block_height: block.block.header.height,
            transaction_id: transaction.transaction.transaction.hash,
            signer_id: transaction.transaction.transaction.signer_id.clone(),
            receiver_id: transaction.transaction.transaction.receiver_id.clone(),
            public_key: transaction.transaction.transaction.public_key.to_string(),
            nonce: transaction.transaction.transaction.nonce,
            actions: transaction
                .transaction
                .transaction
                .actions
                .iter()
                .map(|action| serde_json::to_value(action).unwrap())
                .collect(),
            priority_fee: None,
            signature: transaction.transaction.transaction.signature.to_string(),
        };
        self.0.handle_transaction(transaction_event).await;
        Ok(())
    }
}
