use arrow_schema::TimeUnit;
use core::panic;
use datafusion::arrow::array::{
    ArrayRef, BinaryBuilder, FixedSizeBinaryBuilder, ListBuilder, RecordBatch,
    TimestampNanosecondBuilder, UInt64Builder,
};
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::common::Result;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;

use crate::block_source::Block;

use super::block_based_table::{BlockBasedTableProvider, BlockFetcher, BlockProcessor};

pub type SolanaInnerInstructionsFetcher = BlockFetcher<SolanaInnerInstructiosProcessor>;
pub type SolanaInnerInstructionsTableProvider =
    BlockBasedTableProvider<SolanaInnerInstructiosProcessor>;

pub fn solana_mainnet_logs_table(
    floor: i64,
    ceiling: Option<u64>,
) -> SolanaInnerInstructionsTableProvider {
    BlockBasedTableProvider::new_solana_mainnet_firehose(
        floor,
        ceiling,
        SolanaInnerInstructiosProcessor {},
    )
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SolanaInnerInstructiosProcessor {}

#[async_trait::async_trait]
impl BlockProcessor for SolanaInnerInstructiosProcessor {
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
        let total_logs = blocks
            .iter()
            .map(|block| match block {
                Block::Solana { block } => block
                    .transactions
                    .iter()
                    .map(|trace| {
                        trace
                            .meta
                            .as_ref()
                            .map(|meta| meta.inner_instructions.len())
                            .unwrap_or(0)
                    })
                    .sum::<usize>(),
                Block::Ethereum { block: _ } => panic!("Got wrong block type"),
            })
            .sum::<usize>();

        let mut timestamp_builder = TimestampNanosecondBuilder::with_capacity(total_logs);
        let mut block_number_builder = UInt64Builder::with_capacity(total_logs);
        let mut hash_builder = FixedSizeBinaryBuilder::with_capacity(total_logs, 32);
        let mut from_builder = FixedSizeBinaryBuilder::with_capacity(total_logs, 20);
        let mut to_builder = FixedSizeBinaryBuilder::with_capacity(total_logs, 20);
        let mut address_builder = FixedSizeBinaryBuilder::with_capacity(total_logs, 20);

        let mut topics_builder =
            ListBuilder::with_capacity(FixedSizeBinaryBuilder::with_capacity(4, 32), total_logs);

        let mut data_builder = BinaryBuilder::with_capacity(total_logs, 1024 * total_logs);

        for block in blocks {
            match block {
                Block::Solana { block } => {
                    let _timestamp = std::time::SystemTime::UNIX_EPOCH
                        + std::time::Duration::from_secs(
                            block.block_time.unwrap_or_default().timestamp as u64,
                        );

                    log::trace!(
                        "Got logs from block {}",
                        block
                            .block_height
                            .map(|height| height.block_height)
                            .unwrap_or_default()
                    );

                    for transaction in block.transactions {
                        for inner_instruction in transaction
                            .meta
                            .as_ref()
                            .map(|meta| meta.inner_instructions.clone())
                            .unwrap_or_default()
                        {
                            todo!("Parse inner instruction: {:?}", inner_instruction);
                            // timestamp_builder.append_value(
                            //     timestamp
                            //         .duration_since(std::time::UNIX_EPOCH)
                            //         .unwrap()
                            //         .as_nanos() as i64,
                            // );
                            // block_number_builder.append_value(block.number);
                            // hash_builder.append_value(&transaction.hash).unwrap();
                            // from_builder.append_value(&transaction.from).unwrap();
                            // to_builder.append_value(&transaction.to).unwrap();
                            // address_builder.append_value(log.address).unwrap();
                            // data_builder.append_value(&log.data);
                            // for topic in log.topics {
                            //     topics_builder.values().append_value(&topic).unwrap();
                            // }
                            // topics_builder.append(true);
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
