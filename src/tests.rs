use std::collections::HashMap;

use async_trait::async_trait;
use inindexer::near_indexer_primitives::{types::BlockHeight, CryptoHash};
use inindexer::{
    neardata::NeardataProvider, run_indexer, BlockRange, IndexerOptions,
    PreprocessTransactionsSettings,
};
use intear_events::events::transactions::{
    tx_receipt::TxReceiptEvent, tx_transaction::TxTransactionEvent,
};
use tx_indexer::{TxEventHandler, TxIndexer};

#[derive(Default)]
struct TestIndexer {
    transaction_logs: HashMap<CryptoHash, Vec<TxTransactionEvent>>,
    receipt_logs: HashMap<CryptoHash, Vec<TxReceiptEvent>>,
}

#[async_trait]
impl TxEventHandler for TestIndexer {
    async fn handle_transaction(&mut self, event: TxTransactionEvent) {
        self.transaction_logs
            .entry(event.transaction_id)
            .or_default()
            .push(event);
    }

    async fn handle_receipt(&mut self, event: TxReceiptEvent) {
        self.receipt_logs
            .entry(event.receipt_id)
            .or_default()
            .push(event);
    }

    async fn flush_events(&mut self, _block_height: BlockHeight) {
        // No need to flush in tests
    }
}

#[tokio::test]
async fn handles_transactions() {
    let mut indexer = TxIndexer(TestIndexer::default());

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 124099140,
                end_exclusive: Some(124099143),
            })
        },
    )
    .await
    .unwrap();

    assert_eq!(
        indexer
            .0
            .transaction_logs
            .get(
                &"5ZkqyN3pfRyfyJg67M6U6yS8xQrb2SnLgaruQ2fyHBUk"
                    .parse::<CryptoHash>()
                    .unwrap()
            )
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn handles_receipts() {
    let mut indexer = TxIndexer(TestIndexer::default());

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 124099140,
                end_exclusive: Some(124099142),
            })
        },
    )
    .await
    .unwrap();

    assert_eq!(
        indexer
            .0
            .receipt_logs
            .get(
                &"4YCAaFSM4c4S7fUNvXw2fbBjtu3VdTWiaWV4DkDYGNEF"
                    .parse::<CryptoHash>()
                    .unwrap()
            )
            .unwrap()
            .len(),
        1
    );
}
