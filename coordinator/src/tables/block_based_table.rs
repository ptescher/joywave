use core::panic;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{project_schema, Result};
use datafusion::datasource::TableType;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::prelude::Expr;
use datafusion_expr::{Operator, TableProviderFilterPushDown};
use futures::{FutureExt, StreamExt};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use crate::block_source::{
    AccessTokenSource, Block, BlockProvider, BlockSource, BlockSourceConfig, BlockType,
    FirehoseEndpoint,
};

const TARGET_PARTITIONS: usize = 16;

#[async_trait::async_trait]
pub trait BlockProcessor:
    DeserializeOwned + Serialize + Debug + Clone + Send + Sync + 'static
{
    async fn process(self, blocks: Vec<Block>, schema: SchemaRef) -> Result<RecordBatch>;
    fn fields(&self) -> Vec<Field>;
}

#[derive(Debug, Clone, Serialize)]
pub struct BlockBasedTableProvider<P: BlockProcessor> {
    pub block_source: BlockSourceConfig,
    pub floor: i64,
    pub ceiling: Option<u64>,
    pub processor: P,
}

impl<'de, P: BlockProcessor> Deserialize<'de> for BlockBasedTableProvider<P> {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper<P> {
            block_source: BlockSourceConfig,
            floor: i64,
            ceiling: Option<u64>,
            processor: P,
        }

        let helper = Helper::deserialize(deserializer)?;
        Ok(BlockBasedTableProvider {
            block_source: helper.block_source,
            floor: helper.floor,
            ceiling: helper.ceiling,
            processor: helper.processor,
        })
    }
}

impl<P: BlockProcessor> BlockBasedTableProvider<P> {
    pub fn new_ethereum_mainnet_firehose(floor: i64, ceiling: Option<u64>, processor: P) -> Self {
        Self {
            block_source: BlockSourceConfig {
                block_type: BlockType::Ethereum,
                provider: BlockProvider::Firehose {
                    endpoint: FirehoseEndpoint::EthereumMainnet,
                    access_token_source: AccessTokenSource::Env,
                },
            },
            floor,
            ceiling,
            processor,
        }
    }

    pub fn new_solana_mainnet_firehose(floor: i64, ceiling: Option<u64>, processor: P) -> Self {
        Self {
            block_source: BlockSourceConfig {
                block_type: BlockType::Solana,
                provider: BlockProvider::Firehose {
                    endpoint: FirehoseEndpoint::SolanaMainnet,
                    access_token_source: AccessTokenSource::Env,
                },
            },
            floor,
            ceiling,
            processor,
        }
    }

    async fn block_range_from_filters(&self, filters: &[Expr]) -> (u64, u64, Option<u64>) {
        let latest_block_number =
            BlockSource::get_latest_block_number(self.block_source.clone()).await;

        let mut floor: i64 = self.floor;
        let mut ceiling = self.ceiling;

        for filter in filters {
            match filter {
                Expr::BinaryExpr(binary_expr) => {
                    match (
                        binary_expr.left.as_ref(),
                        binary_expr.op,
                        binary_expr.right.as_ref(),
                    ) {
                        (Expr::Column(column), Operator::GtEq, Expr::Literal(value)) => {
                            if column.name == "block_number" {
                                if let datafusion::scalar::ScalarValue::UInt64(Some(value)) = value
                                {
                                    floor = *value as i64;
                                } else {
                                    panic!("Unsupported filter {:?}", filter);
                                }
                            } else {
                                panic!("Unsupported filter {:?}", filter);
                            }
                        }
                        (Expr::Column(column), Operator::Gt, Expr::Literal(value)) => {
                            if column.name == "block_number" {
                                if let datafusion::scalar::ScalarValue::UInt64(Some(value)) = value
                                {
                                    floor = *value as i64 + 1;
                                } else {
                                    panic!("Unsupported filter {:?}", filter);
                                }
                            } else {
                                panic!("Unsupported filter {:?}", filter);
                            }
                        }
                        (Expr::Column(column), Operator::LtEq, Expr::Literal(value)) => {
                            if column.name == "block_number" {
                                if let datafusion::scalar::ScalarValue::UInt64(Some(value)) = value
                                {
                                    ceiling = Some(*value);
                                } else {
                                    panic!("Unsupported filter {:?}", filter);
                                }
                            } else {
                                panic!("Unsupported filter {:?}", filter);
                            }
                        }
                        (Expr::Column(column), Operator::Lt, Expr::Literal(value)) => {
                            if column.name == "block_number" {
                                if let datafusion::scalar::ScalarValue::UInt64(Some(value)) = value
                                {
                                    ceiling = Some(value - 1);
                                }
                            } else {
                                panic!("Unsupported filter {:?}", filter);
                            }
                        }
                        _ => {
                            panic!("Unsupported filter {:?}", filter);
                        }
                    }
                }
                other => {
                    panic!("Unsupported filter {:?}", other);
                }
            }
        }

        let first_block = if floor < 0 {
            latest_block_number - (-floor) as u64
        } else {
            floor as u64
        };

        log::debug!("Filter: {filters:?}");

        log::debug!(
            "Getting blocks, first_block {first_block} latest_block_number {latest_block_number} ceiling: {ceiling:?}");

        (first_block, latest_block_number, ceiling)
    }

    pub(crate) async fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: SchemaRef,
        filters: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = project_schema(&schema, projections).unwrap();

        let (floor, latest_block_number, ceiling) = self.block_range_from_filters(filters).await;

        log::debug!(
            "Creating physical plan for logs from block {} to {}. Filters: {:?}",
            floor,
            latest_block_number,
            filters,
        );

        Ok(Arc::new(BlockFetcher::new(
            self.clone(),
            projected_schema,
            latest_block_number,
            floor,
            ceiling,
        )))
    }
}

#[async_trait::async_trait]
impl<P: BlockProcessor> TableProvider for BlockBasedTableProvider<P> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        SchemaRef::new(Schema::new(self.processor.fields()))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if let Expr::BinaryExpr(binary_expr) = filter {
                    if let Expr::Column(column) = *binary_expr.left.clone() {
                        if column.name == "block_number" {
                            return TableProviderFilterPushDown::Exact;
                        }
                    }

                    if let Expr::Column(column) = *binary_expr.right.clone() {
                        if column.name == "block_number" {
                            return TableProviderFilterPushDown::Exact;
                        }
                    }
                }

                log::debug!("Not pushing down unsupported filter: {:?}", filter);
                TableProviderFilterPushDown::Unsupported
            })
            .collect::<Vec<TableProviderFilterPushDown>>())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        log::debug!("Session: {}", state.session_id());
        let schema = self.schema().clone();
        return self.create_physical_plan(projection, schema, filters).await;
    }
}

#[derive(Clone, Debug)]
pub struct BlockFetcher<P: BlockProcessor> {
    pub data_source: BlockBasedTableProvider<P>,
    pub projected_schema: SchemaRef,
    plan_properties: PlanProperties,
    pub current_block: u64,
    pub first_block: u64,
    pub last_block: Option<u64>,
}

impl<P: BlockProcessor> BlockFetcher<P> {
    pub fn new(
        data_source: BlockBasedTableProvider<P>,
        projected_schema: SchemaRef,
        current_block: u64,
        first_block: u64,
        last_block: Option<u64>,
    ) -> Self {
        let backfill_block_count = current_block - first_block;

        // 16 backfill partitions, 1 live partition
        // TODO: break out backfill and streaming to children
        let partitions = if backfill_block_count > 100 {
            TARGET_PARTITIONS + 1
        } else {
            1
        };

        Self {
            data_source,
            projected_schema: projected_schema.clone(),
            plan_properties: PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
                Partitioning::UnknownPartitioning(partitions),
                EmissionType::Incremental,
                if last_block.is_none() {
                    Boundedness::Unbounded {
                        requires_infinite_memory: false,
                    }
                } else {
                    Boundedness::Bounded
                },
            ),
            current_block,
            first_block,
            last_block,
        }
    }

    fn range_for_partition(&self, partition: usize) -> (u64, u64) {
        let first_block = self.first_block;
        let last_block = self.last_block.unwrap_or(self.current_block);
        let total_backfill = last_block - self.first_block;
        let partition_size = total_backfill.div_ceil(10);
        let partition_first_block = first_block + partition as u64 * partition_size;
        let partition_last_block = partition_first_block + (partition_size - 1);
        log::debug!(
            "Getting logs from block {} to {} out of ({} to {})",
            partition_first_block,
            partition_last_block,
            first_block,
            last_block
        );
        (partition_first_block, partition_last_block)
    }

    fn stream(&self, partition: usize) -> impl futures::stream::Stream<Item = Result<RecordBatch>> {
        let block_source = if partition == 0 {
            BlockSource::new(
                self.data_source.block_source.clone(),
                self.first_block,
                self.last_block,
            )
        } else {
            let (first_block, last_block) = self.range_for_partition(partition - 1);
            BlockSource::new(
                self.data_source.block_source.clone(),
                first_block,
                Some(last_block),
            )
        };

        let schema = self.schema();
        let processor: P = self.data_source.processor.clone();
        let stream = block_source
            .map(|block_source| block_source.stream_batches(10))
            .flatten_stream()
            .then(move |blocks| processor.clone().process(blocks, schema.clone()));

        stream
    }
}

impl<P: BlockProcessor> DisplayAs for BlockFetcher<P> {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Logs from {} to {}",
            self.first_block,
            self.last_block
                .map(|block| block.to_string())
                .unwrap_or_else(|| "latest".to_string())
        )
    }
}

impl<P: BlockProcessor> ExecutionPlan for BlockFetcher<P> {
    fn name(&self) -> &str {
        "Pull Ethereum Logs From Firehose"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self.stream(partition);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}
