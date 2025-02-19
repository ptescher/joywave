use std::sync::Arc;

use ballista_core::serde::BallistaPhysicalExtensionCodec;
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::error::Result;
use datafusion::logical_expr::Extension;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion::{arrow::datatypes::SchemaRef, catalog::TableProvider};
use datafusion_expr::{AggregateUDF, LogicalPlan, ScalarUDF, WindowUDF};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use serde::{Deserialize, Serialize};

use crate::tables::ethereum_logs::{EthereumLogsFetcher, EthereumLogsTableProvider};
use crate::tables::jupiter_v6_swaps::{JupiterV6SwapsFetcher, JupiterV6SwapsTableProvider};
use crate::tables::solana_instructions::{
    SolanaInnerInstructionsFetcher, SolanaInnerInstructionsTableProvider,
};

#[derive(Debug)]
pub struct Codec {
    ballista: BallistaPhysicalExtensionCodec,
}

impl Codec {
    pub fn new() -> Self {
        Self {
            ballista: BallistaPhysicalExtensionCodec::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum LogicalExtensionCodecMessage {
    EthereumLogsTableProvider(EthereumLogsTableProvider),
    JupiverV6SwapsTableProvider(JupiterV6SwapsTableProvider),
    SolanaInnerInstructionsTableProvider(SolanaInnerInstructionsTableProvider),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum PhusicalExtensionCodecMessage {
    EthereumLogs {
        data_source: EthereumLogsTableProvider,
        projected_schema: SchemaRef,
        current_block: u64,
        first_block: u64,
        last_block: Option<u64>,
    },
    SolanaInnerInstructions {
        data_source: SolanaInnerInstructionsTableProvider,
        projected_schema: SchemaRef,
        current_block: u64,
        first_block: u64,
        last_block: Option<u64>,
    },
    JupiterV6Swaps {
        data_source: JupiterV6SwapsTableProvider,
        projected_schema: SchemaRef,
        current_block: u64,
        first_block: u64,
        last_block: Option<u64>,
    },
    Ballista {
        message: Vec<u8>,
    },
}

impl LogicalExtensionCodec for Codec {
    fn try_encode_file_format(
        &self,
        _buf: &mut Vec<u8>,
        _node: Arc<dyn FileFormatFactory>,
    ) -> Result<(), datafusion::error::DataFusionError> {
        todo!()
    }

    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[LogicalPlan],
        _ctx: &datafusion::execution::context::SessionContext,
    ) -> Result<Extension> {
        todo!()
    }

    fn try_encode(
        &self,
        _node: &datafusion_expr::Extension,
        _buf: &mut Vec<u8>,
    ) -> Result<(), datafusion::error::DataFusionError> {
        todo!()
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        _table_ref: &TableReference,
        _schema: SchemaRef,
        _ctx: &SessionContext,
    ) -> Result<std::sync::Arc<dyn TableProvider>> {
        let message = serde_json::from_slice::<LogicalExtensionCodecMessage>(buf).unwrap();
        match message {
            LogicalExtensionCodecMessage::EthereumLogsTableProvider(provider) => {
                Ok(Arc::new(provider))
            }
            LogicalExtensionCodecMessage::JupiverV6SwapsTableProvider(provider) => {
                Ok(Arc::new(provider))
            }
            LogicalExtensionCodecMessage::SolanaInnerInstructionsTableProvider(provider) => {
                Ok(Arc::new(provider))
            }
        }
    }

    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        node: std::sync::Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        if let Some(provider) = node.as_any().downcast_ref::<EthereumLogsTableProvider>() {
            let encoded = serde_json::to_vec(
                &LogicalExtensionCodecMessage::EthereumLogsTableProvider(provider.clone()),
            )
            .unwrap();
            buf.extend_from_slice(&encoded);
            Ok(())
        } else if let Some(provider) = node.as_any().downcast_ref::<JupiterV6SwapsTableProvider>() {
            let encoded = serde_json::to_vec(
                &LogicalExtensionCodecMessage::JupiverV6SwapsTableProvider(provider.clone()),
            )
            .unwrap();
            buf.extend_from_slice(&encoded);
            Ok(())
        } else {
            Err(datafusion::error::DataFusionError::Execution(
                "Could not encode table provider".to_owned(),
            ))
        }
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<datafusion_expr::ScalarUDF>> {
        log::debug!("Decoding UDF: {}", name);
        self.ballista.try_decode_udf(name, buf)
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        let name = node.name();
        log::debug!("Encoding UDF: {}", name);
        self.ballista.try_encode_udf(node, buf)
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<()> {
        let name = node.name();
        log::debug!("Encoding UDAF: {}", name);
        self.ballista.try_encode_udaf(node, buf)
    }

    fn try_decode_udwf(&self, name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>> {
        log::debug!("Decoding UDWF");
        self.ballista.try_decode_udwf(name, buf)
    }

    fn try_encode_udwf(&self, node: &WindowUDF, buf: &mut Vec<u8>) -> Result<()> {
        log::debug!("Encoding UDWF");
        self.ballista.try_encode_udwf(node, buf)
    }
}

impl PhysicalExtensionCodec for Codec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn datafusion::physical_plan::ExecutionPlan>],
        registry: &dyn datafusion::execution::FunctionRegistry,
    ) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        let message = serde_json::from_slice::<PhusicalExtensionCodecMessage>(buf).unwrap();
        match message {
            PhusicalExtensionCodecMessage::EthereumLogs {
                data_source,
                projected_schema,
                current_block,
                first_block,
                last_block,
            } => {
                let plan = EthereumLogsFetcher::new(
                    data_source,
                    projected_schema,
                    current_block,
                    first_block,
                    last_block,
                );
                Ok(Arc::new(plan))
            }
            PhusicalExtensionCodecMessage::SolanaInnerInstructions {
                data_source,
                projected_schema,
                current_block,
                first_block,
                last_block,
            } => {
                let plan = SolanaInnerInstructionsFetcher::new(
                    data_source,
                    projected_schema,
                    current_block,
                    first_block,
                    last_block,
                );
                Ok(Arc::new(plan))
            }
            PhusicalExtensionCodecMessage::JupiterV6Swaps {
                data_source,
                projected_schema,
                current_block,
                first_block,
                last_block,
            } => {
                let plan = JupiterV6SwapsFetcher::new(
                    data_source,
                    projected_schema,
                    current_block,
                    first_block,
                    last_block,
                );
                Ok(Arc::new(plan))
            }
            PhusicalExtensionCodecMessage::Ballista { message } => {
                self.ballista.try_decode(&message, inputs, registry)
            }
        }
    }

    fn try_encode(
        &self,
        node: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        if let Some(exec) = node.as_any().downcast_ref::<EthereumLogsFetcher>() {
            let encoded = serde_json::to_vec(&PhusicalExtensionCodecMessage::EthereumLogs {
                data_source: exec.data_source.clone(),
                projected_schema: exec.projected_schema.clone(),
                current_block: exec.current_block,
                first_block: exec.first_block,
                last_block: exec.last_block,
            })
            .unwrap();
            buf.extend_from_slice(&encoded);
            Ok(())
        } else if let Some(exec) = node.as_any().downcast_ref::<JupiterV6SwapsFetcher>() {
            let encoded = serde_json::to_vec(&PhusicalExtensionCodecMessage::JupiterV6Swaps {
                data_source: exec.data_source.clone(),
                projected_schema: exec.projected_schema.clone(),
                current_block: exec.current_block,
                first_block: exec.first_block,
                last_block: exec.last_block,
            })
            .unwrap();
            buf.extend_from_slice(&encoded);
            Ok(())
        } else if let Some(exec) = node
            .as_any()
            .downcast_ref::<SolanaInnerInstructionsFetcher>()
        {
            let encoded =
                serde_json::to_vec(&PhusicalExtensionCodecMessage::SolanaInnerInstructions {
                    data_source: exec.data_source.clone(),
                    projected_schema: exec.projected_schema.clone(),
                    current_block: exec.current_block,
                    first_block: exec.first_block,
                    last_block: exec.last_block,
                })
                .unwrap();
            buf.extend_from_slice(&encoded);
            Ok(())
        } else {
            let mut child_buf: Vec<u8> = vec![];
            self.ballista.try_encode(node, &mut child_buf)?;

            let encoded =
                serde_json::to_vec(&PhusicalExtensionCodecMessage::Ballista { message: child_buf })
                    .unwrap();
            buf.extend_from_slice(&encoded);
            Ok(())
        }
    }
}
