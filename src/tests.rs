use std::collections::HashMap;

use async_trait::async_trait;
use inindexer::near_indexer_primitives::CryptoHash;
use inindexer::{
    neardata_server::NeardataServerProvider, run_indexer, BlockIterator, IndexerOptions,
    PreprocessTransactionsSettings,
};
use intear_events::events::transactions::tx_receipt::TxReceiptEventData;
use intear_events::events::transactions::tx_transaction::TxTransactionEventData;
use tx_indexer::{TxEventHandler, TxIndexer};

#[derive(Default)]
struct TestIndexer {
    transaction_logs: HashMap<CryptoHash, Vec<TxTransactionEventData>>,
    receipt_logs: HashMap<CryptoHash, Vec<TxReceiptEventData>>,
}

#[async_trait]
impl TxEventHandler for TestIndexer {
    async fn handle_transaction(&mut self, event: TxTransactionEventData) {
        self.transaction_logs
            .entry(event.transaction_id)
            .or_default()
            .push(event);
    }

    async fn handle_receipt(&mut self, event: TxReceiptEventData) {
        self.receipt_logs
            .entry(event.receipt_id)
            .or_default()
            .push(event);
    }
}

#[tokio::test]
async fn handles_transactions() {
    let mut indexer = TxIndexer(TestIndexer::default());

    run_indexer(
        &mut indexer,
        NeardataServerProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(124099140..=124099142),
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..Default::default()
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
        NeardataServerProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(124099140..=124099142),
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..Default::default()
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
