use arrow_schema::TimeUnit;
use core::panic;
use datafusion::arrow::array::{
    ArrayRef, FixedSizeBinaryBuilder, RecordBatch, TimestampNanosecondBuilder, UInt64Builder,
};
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::common::Result;
use firehose_client::solana::{
    CompiledInstruction, ConfirmedTransaction, InnerInstruction, TransactionStatusMeta,
};
use serde::{Deserialize, Serialize};
use sologger_log_context::programs_selector::ProgramsSelector;
use sologger_log_context::sologger_log_context::LogContext;
use std::fmt::Debug;
use std::sync::Arc;

use crate::block_source::Block;

use base64::prelude::*;
use idl_decoder::anchor_lang::prelude::*;

use super::block_based_table::{BlockBasedTableProvider, BlockFetcher, BlockProcessor};

const PROGRAM_ID: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JupiterV6SwapsProcessor {
    programs_selector: ProgramsSelector,
}

impl JupiterV6SwapsProcessor {
    fn _process_inner_instruction(
        meta: &TransactionStatusMeta,
        transaction: &ConfirmedTransaction,
        inner_instruction: &InnerInstruction,
    ) -> Vec<idl_decoder::jupiter_v6::client::args::Route> {
        let mut routes = Vec::new();
        let program_id = transaction
            .transaction
            .as_ref()
            .and_then(|transaction| transaction.message.as_ref())
            .map(|message: &firehose_client::solana::Message| {
                if message.account_keys.len() > inner_instruction.program_id_index as usize {
                    return message.account_keys[inner_instruction.program_id_index as usize]
                        .as_slice();
                }
                let index =
                    inner_instruction.program_id_index as usize - message.account_keys.len();
                if index < meta.loaded_writable_addresses.len() {
                    return meta.loaded_writable_addresses[index].as_slice();
                }

                let index = index - meta.loaded_writable_addresses.len();
                if index < meta.loaded_readonly_addresses.len() {
                    return meta.loaded_readonly_addresses[index].as_slice();
                }

                panic!("Could not look up program id");
            })
            .expect("Could not look up program id");
        if program_id == &bs58::decode(PROGRAM_ID).into_vec().unwrap() {
            let slice_u8: &[u8] = &inner_instruction.data[..];
            match &slice_u8[0..8] {
                idl_decoder::jupiter_v6::client::args::Route::DISCRIMINATOR => {
                    match idl_decoder::jupiter_v6::client::args::Route::deserialize(
                        &mut &slice_u8[8..],
                    ) {
                        Ok(route) => {
                            routes.push(route);
                        }
                        Err(e) => {
                            log::warn!("Error deserializing Route: {:?}", e);
                        }
                    }
                }
                _ => {
                    log::debug!("Unknown instruction: {:?}", &slice_u8[0..8]);
                }
            }
        }

        routes
    }

    fn _process_compiled_instruction(
        meta: &TransactionStatusMeta,
        transaction: &ConfirmedTransaction,
        compiled_instruction: &CompiledInstruction,
    ) -> Vec<idl_decoder::jupiter_v6::client::args::Route> {
        {
            let mut routes = vec![];
            let program_id = transaction
                .transaction
                .as_ref()
                .and_then(|transaction| transaction.message.as_ref())
                .map(|message: &firehose_client::solana::Message| {
                    if message.account_keys.len() > compiled_instruction.program_id_index as usize {
                        return message.account_keys
                            [compiled_instruction.program_id_index as usize]
                            .as_slice();
                    }
                    let index =
                        compiled_instruction.program_id_index as usize - message.account_keys.len();
                    if index < meta.loaded_writable_addresses.len() {
                        return meta.loaded_writable_addresses[index].as_slice();
                    }

                    let index = index - meta.loaded_writable_addresses.len();
                    if index < meta.loaded_readonly_addresses.len() {
                        return meta.loaded_readonly_addresses[index].as_slice();
                    }

                    panic!("Could not look up program id");
                })
                .expect("Could not look up program id");
            if program_id == &bs58::decode(PROGRAM_ID).into_vec().unwrap() {
                let slice_u8: &[u8] = &compiled_instruction.data[..];
                match &slice_u8[0..8] {
                    idl_decoder::jupiter_v6::client::args::Route::DISCRIMINATOR => {
                        match idl_decoder::jupiter_v6::client::args::Route::deserialize(
                            &mut &slice_u8[8..],
                        ) {
                            Ok(route) => {
                                routes.push(route);
                            }
                            Err(e) => {
                                log::debug!("Error deserializing Route: {:?}", e);
                            }
                        }
                    }
                    _ => {
                        log::debug!("Unknown instruction: {:?}", &slice_u8[0..8]);
                    }
                }
            }
            routes
        }
    }
}

#[async_trait::async_trait]
impl BlockProcessor for JupiterV6SwapsProcessor {
    fn fields(&self) -> Vec<Field> {
        vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("block_number", DataType::UInt64, false),
            Field::new("input_mint", DataType::FixedSizeBinary(32), false),
            Field::new("input_amount", DataType::UInt64, false),
            Field::new("output_mint", DataType::FixedSizeBinary(32), false),
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
        let mut input_mint_builder = FixedSizeBinaryBuilder::with_capacity(total_logs, 32);
        let mut input_amount_builder = UInt64Builder::with_capacity(total_logs);
        let mut output_mint_builder = FixedSizeBinaryBuilder::with_capacity(total_logs, 32);

        for block in blocks {
            match block {
                Block::Solana { block } => {
                    let timestamp = std::time::SystemTime::UNIX_EPOCH
                        + std::time::Duration::from_secs(
                            block.block_time.unwrap_or_default().timestamp as u64,
                        );

                    log::trace!(
                        "Got {} transctions from block {} slot {}",
                        block.transactions.len(),
                        block
                            .block_height
                            .map(|height| height.block_height)
                            .unwrap_or_default(),
                        block.slot
                    );

                    for transaction in block.transactions {
                        let meta_wrapped = &transaction.meta;
                        let meta = meta_wrapped.as_ref().unwrap();

                        // ------------- EVENTS -------------
                        let log_contexts = LogContext::parse_logs_basic(
                            &meta.log_messages,
                            &self.programs_selector,
                        );

                        for context in log_contexts {
                            for data in context.data_logs.iter() {
                                if let Ok(decoded) = BASE64_STANDARD.decode(data) {
                                    let slice_u8: &mut &[u8] = &mut &decoded[..];
                                    let slice_discriminator: &[u8] =
                                        slice_u8[0..8].try_into().expect("error");
                                    match slice_discriminator {
                                            idl_decoder::jupiter_v6::events::SwapEvent::DISCRIMINATOR => {
                                                match idl_decoder::jupiter_v6::events::SwapEvent::deserialize(&mut &slice_u8[8..]) {
                                                        Ok(event) => {
                                                            log::debug!("Got swap: {:?}", event);
                                                            timestamp_builder.append_value(
                                                                timestamp
                                                                    .duration_since(std::time::UNIX_EPOCH)
                                                                    .unwrap()
                                                                    .as_nanos() as i64,
                                                            );
                                                            block_number_builder.append_value(block.slot);
                                                            input_amount_builder.append_value(event.input_amount);
                                                            input_mint_builder.append_value(event.input_mint).unwrap();
                                                            output_mint_builder.append_value(event.output_mint).unwrap();
                                                        }
                                                        Err(e) => {
                                                            log::debug!("Error deserializing SwapEvent: {:?}", e);
                                                        }
                                                    }
                                                },
                                            other => {
                                                log::debug!("Unknown event: {:?}", other);
                                            }
                                        }
                                } else {
                                    panic!("Error decoding data");
                                }
                            }
                        }

                        // // Instructions
                        // if let Some(ref meta) = transaction.meta {
                        //     let mut routes = vec![];
                        //     for instruction in transaction
                        //         .transaction
                        //         .as_ref()
                        //         .map(|transaction| {
                        //             transaction.message.as_ref().unwrap().instructions.iter()
                        //         })
                        //         .unwrap()
                        //     {
                        //         for route in Self::process_compiled_instruction(
                        //             meta,
                        //             &transaction,
                        //             instruction,
                        //         ) {
                        //             routes.push(route);
                        //         }
                        //     }
                        //     for instruction in meta.inner_instructions.iter() {
                        //         for inner_instruction in instruction.instructions.iter() {
                        //             for route in Self::process_inner_instruction(
                        //                 meta,
                        //                 &transaction,
                        //                 inner_instruction,
                        //             ) {
                        //                 routes.push(route);
                        //             }
                        //         }
                        //     }

                        //     for route in routes {
                        //         timestamp_builder.append_value(
                        //             timestamp
                        //                 .duration_since(std::time::UNIX_EPOCH)
                        //                 .unwrap()
                        //                 .as_nanos() as i64,
                        //         );
                        //         block_number_builder.append_value(block.slot);
                        //         input_amount_builder.append_value(route.in_amount);
                        //         input_mint_builder.append_value(event.input_mint).unwrap();
                        //         output_mint_builder.append_value(event.output_mint).unwrap();
                        //     }
                        // }
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
                "output_mint" => columns.push(Arc::new(output_mint_builder.finish())),
                "input_mint" => columns.push(Arc::new(input_mint_builder.finish())),
                "input_amount" => columns.push(Arc::new(input_amount_builder.finish())),
                other => {
                    panic!("Unknown field: {}", other);
                }
            };
        });

        let result = RecordBatch::try_new(projected_schema, columns)?;

        Ok(result)
    }
}

pub type JupiterV6SwapsFetcher = BlockFetcher<JupiterV6SwapsProcessor>;
pub type JupiterV6SwapsTableProvider = BlockBasedTableProvider<JupiterV6SwapsProcessor>;

pub fn jupiter_v6_swaps_table(floor: i64, ceiling: Option<u64>) -> JupiterV6SwapsTableProvider {
    BlockBasedTableProvider::new_solana_mainnet_firehose(
        floor,
        ceiling,
        JupiterV6SwapsProcessor {
            programs_selector: ProgramsSelector::new(&[PROGRAM_ID.to_string()]),
        },
    )
}
