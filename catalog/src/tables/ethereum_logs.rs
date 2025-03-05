use arrow_schema::{SchemaRef, TimeUnit};
use core::panic;
use datafusion::arrow::array::{
    ArrayRef, BinaryBuilder, FixedSizeBinaryBuilder, ListBuilder, RecordBatch,
    TimestampNanosecondBuilder, UInt32Builder, UInt64Builder,
};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::error::Result;
use serde::{Deserialize, Serialize};

use std::sync::Arc;

use crate::block_source::Block;

use super::block_based_table::{BlockBasedTableProvider, BlockFetcher, BlockProcessor};

pub type EthereumLogsFetcher = BlockFetcher<EthereumLogsProcessor>;
pub type EthereumLogsTableProvider = BlockBasedTableProvider<EthereumLogsProcessor>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EthereumLogsProcessor {}

#[async_trait::async_trait]
impl BlockProcessor for EthereumLogsProcessor {
    fn fields(&self) -> Vec<Field> {
        vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("block_number", DataType::UInt64, false),
            Field::new("hash", DataType::FixedSizeBinary(32), false),
            Field::new("from", DataType::FixedSizeBinary(20), false),
            Field::new("to", DataType::FixedSizeBinary(20), false),
            Field::new("log_index", DataType::UInt32, false),
            Field::new("address", DataType::FixedSizeBinary(20), false),
            Field::new_list(
                "topics",
                Field::new_list_field(DataType::FixedSizeBinary(32), true),
                false,
            ),
            Field::new("data", DataType::Binary, false),
        ]
    }

    async fn process(self, blocks: Vec<Block>, projected_schema: SchemaRef) -> Result<RecordBatch> {
        let total_logs: usize = blocks
            .iter()
            .map(|block| match block {
                Block::Ethereum { block } => block
                    .transaction_traces
                    .iter()
                    .map(|trace| {
                        trace
                            .receipt
                            .as_ref()
                            .map(|receipt| receipt.logs.len())
                            .unwrap_or(0)
                    })
                    .sum::<usize>(),
                _ => panic!("Got wrong block type"),
            })
            .sum::<usize>();

        let mut timestamp_builder = TimestampNanosecondBuilder::with_capacity(total_logs);
        let mut block_number_builder = UInt64Builder::with_capacity(total_logs);
        let mut hash_builder = FixedSizeBinaryBuilder::with_capacity(total_logs, 32);
        let mut from_builder = FixedSizeBinaryBuilder::with_capacity(total_logs, 20);
        let mut to_builder = FixedSizeBinaryBuilder::with_capacity(total_logs, 20);
        let mut log_index_builder = UInt32Builder::with_capacity(total_logs);
        let mut address_builder = FixedSizeBinaryBuilder::with_capacity(total_logs, 20);

        let mut topics_builder =
            ListBuilder::with_capacity(FixedSizeBinaryBuilder::with_capacity(4, 32), total_logs);

        let mut data_builder = BinaryBuilder::with_capacity(total_logs, 1024 * total_logs);

        for block in blocks {
            match block {
                Block::Ethereum { block } => {
                    let timestamp = std::time::SystemTime::try_from(
                        block
                            .header
                            .and_then(|header| header.timestamp)
                            .unwrap_or_default(),
                    )
                    .unwrap_or(std::time::SystemTime::UNIX_EPOCH);

                    log::trace!("Got logs from block {}", block.number);

                    for transaction in block.transaction_traces {
                        for log in transaction
                            .receipt
                            .as_ref()
                            .map(|receipt| receipt.logs.clone())
                            .unwrap_or_default()
                        {
                            timestamp_builder.append_value(
                                timestamp
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_nanos() as i64,
                            );
                            block_number_builder.append_value(block.number);
                            hash_builder.append_value(&transaction.hash).unwrap();
                            from_builder.append_value(&transaction.from).unwrap();
                            to_builder.append_value(&transaction.to).unwrap();
                            log_index_builder.append_value(log.index);
                            address_builder.append_value(log.address).unwrap();
                            data_builder.append_value(&log.data);
                            for topic in log.topics {
                                topics_builder.values().append_value(&topic).unwrap();
                            }
                            topics_builder.append(true);
                        }
                    }
                }
                _ => panic!("Got wrong block type"),
            }
        }

        let mut columns: Vec<ArrayRef> = vec![];

        projected_schema.fields().iter().for_each(|field| {
            match field.name().as_str() {
                "timestamp" => columns.push(Arc::new(timestamp_builder.finish())),
                "block_number" => columns.push(Arc::new(block_number_builder.finish())),
                "hash" => columns.push(Arc::new(hash_builder.finish())),
                "from" => columns.push(Arc::new(from_builder.finish())),
                "to" => columns.push(Arc::new(to_builder.finish())),
                "log_index" => columns.push(Arc::new(log_index_builder.finish())),
                "address" => columns.push(Arc::new(address_builder.finish())),
                "topics" => columns.push(Arc::new(topics_builder.finish())),
                "data" => columns.push(Arc::new(data_builder.finish())),
                other => {
                    panic!("Unknown field: {}", other);
                }
            };
        });

        let result = RecordBatch::try_new(projected_schema, columns)?;

        Ok(result)
    }
}

pub fn ethereum_mainnet_logs_table(floor: i64, ceiling: Option<u64>) -> EthereumLogsTableProvider {
    BlockBasedTableProvider::new_ethereum_mainnet_firehose(floor, ceiling, EthereumLogsProcessor {})
}
