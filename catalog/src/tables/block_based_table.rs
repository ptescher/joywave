use core::panic;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{project_schema, Result};
use datafusion::datasource::TableType;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::prelude::Expr;
use datafusion_expr::{BinaryExpr, Operator, TableProviderFilterPushDown};
use futures::{FutureExt, StreamExt};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::cmp::min;
use std::fmt::Debug;
use std::i64;
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

    fn block_range_from_filter(&self, filter: &Expr) -> (i64, Option<u64>) {
        let mut floor: i64 = i64::MIN;
        let mut ceiling = None;
        match filter {
            Expr::Cast(cast) => {
                let (cast_first_block, cast_ceiling) = self.block_range_from_filter(&cast.expr);
                floor = std::cmp::max(floor, cast_first_block as i64);
                if let Some(cast_ceiling) = cast_ceiling {
                    ceiling = Some(std::cmp::min(ceiling.unwrap_or(u64::MAX), cast_ceiling));
                }
            }
            Expr::BinaryExpr(binary_expr) => {
                match (
                    binary_expr.left.as_ref(),
                    binary_expr.op,
                    binary_expr.right.as_ref(),
                ) {
                    (Expr::Column(column), Operator::GtEq, Expr::Literal(value)) => {
                        if column.name == "block_number" {
                            if let datafusion::scalar::ScalarValue::UInt64(Some(value)) = value {
                                floor = *value as i64;
                            } else if let datafusion::scalar::ScalarValue::Int64(Some(value)) =
                                value
                            {
                                floor = *value;
                            } else {
                                panic!("Unsupported filter {:?}", filter);
                            }
                        } else {
                            panic!("Unsupported filter {:?}", filter);
                        }
                    }
                    (Expr::Column(column), Operator::Gt, Expr::Literal(value)) => {
                        if column.name == "block_number" {
                            if let datafusion::scalar::ScalarValue::UInt64(Some(value)) = value {
                                floor = *value as i64 + 1;
                            } else if let datafusion::scalar::ScalarValue::Int64(Some(value)) =
                                value
                            {
                                if *value < 0 {
                                    floor = *value;
                                } else {
                                    floor = *value + 1;
                                }
                            } else {
                                panic!("Unsupported filter {:?}", filter);
                            }
                        } else {
                            panic!("Unsupported filter {:?}", filter);
                        }
                    }
                    (Expr::Column(column), Operator::LtEq, Expr::Literal(value)) => {
                        if column.name == "block_number" {
                            if let datafusion::scalar::ScalarValue::UInt64(Some(value)) = value {
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
                            if let datafusion::scalar::ScalarValue::UInt64(Some(value)) = value {
                                ceiling = Some(value - 1);
                            }
                        } else {
                            panic!("Unsupported filter {:?}", filter);
                        }
                    }
                    (Expr::Cast(cast), op, right) => {
                        let cast_filter = Expr::BinaryExpr(BinaryExpr {
                            left: cast.expr.clone(),
                            op,
                            right: Box::new(right.clone()),
                        });
                        let (cast_first_block, cast_ceiling) =
                            self.block_range_from_filter(&cast_filter);
                        floor = std::cmp::max(floor, cast_first_block as i64);
                        if let Some(cast_ceiling) = cast_ceiling {
                            ceiling =
                                Some(std::cmp::min(ceiling.unwrap_or(u64::MAX), cast_ceiling));
                        }
                    }
                    (left, op, Expr::Cast(cast)) => {
                        let cast_filter = Expr::BinaryExpr(BinaryExpr {
                            left: Box::new(left.clone()),
                            op,
                            right: cast.expr.clone(),
                        });
                        let (cast_first_block, cast_ceiling) =
                            self.block_range_from_filter(&cast_filter);
                        floor = std::cmp::max(floor, cast_first_block as i64);
                        if let Some(cast_ceiling) = cast_ceiling {
                            ceiling =
                                Some(std::cmp::min(ceiling.unwrap_or(u64::MAX), cast_ceiling));
                        }
                    }

                    (left, op, right) => {
                        panic!("Unsupported filter {left:?} {op:?} {right:?}",);
                    }
                }
            }
            other => {
                panic!("Unsupported filter {:?}", other);
            }
        }
        (floor, ceiling)
    }

    async fn block_range_from_filters(&self, filters: &[Expr]) -> (u64, u64, Option<u64>) {
        let latest_block_number =
            BlockSource::get_latest_block_number(self.block_source.clone()).await;

        let mut floor: i64 = i64::MIN;
        let mut ceiling = self.ceiling;

        for filter in filters {
            let (filter_floor, filter_ceiling) = self.block_range_from_filter(filter);
            floor = std::cmp::max(floor, filter_floor);
            if let Some(filter_ceiling) = filter_ceiling {
                ceiling = Some(std::cmp::min(ceiling.unwrap_or(u64::MAX), filter_ceiling));
            }
        }

        let first_block: u64 = if floor == i64::MIN {
            self.floor as u64
        } else if floor < 0 {
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

    fn supports_expr_pushdown(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Column(column) => {
                if column.name == "block_number" {
                    true
                } else {
                    false
                }
            }
            Expr::Cast(cast) => self.supports_expr_pushdown(cast.expr.as_ref()),
            other => {
                log::debug!("Unsupported expr pushdown: {:?}", other);
                false
            }
        }
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
            .map(|filter| match filter {
                Expr::Cast(expr) => {
                    if self.supports_expr_pushdown(expr.expr.as_ref()) {
                        TableProviderFilterPushDown::Exact
                    } else {
                        TableProviderFilterPushDown::Unsupported
                    }
                }
                Expr::BinaryExpr(expr) => {
                    if self.supports_expr_pushdown(expr.left.as_ref())
                        || self.supports_expr_pushdown(expr.right.as_ref())
                    {
                        TableProviderFilterPushDown::Exact
                    } else {
                        TableProviderFilterPushDown::Unsupported
                    }
                }
                other => {
                    log::debug!("Not pushing down unsupported filter: {:?}", other);
                    TableProviderFilterPushDown::Unsupported
                }
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
        let backfill_block_count = if let Some(last_block) = last_block {
            last_block - first_block
        } else {
            current_block - min(first_block, current_block)
        };

        // 16 backfill partitions, 1 live partition
        // TODO: break out backfill and streaming to children
        let partitions = if backfill_block_count > 100 {
            TARGET_PARTITIONS + 1
        } else {
            1
        };

        // Create sort expressions for block_number and timestamp
        let sort_exprs = projected_schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(index, field)| match field.name().as_str() {
                "block_number" => Some(
                    PhysicalSortExpr::new_default(Arc::new(Column::new(field.name(), index))).asc(),
                ),
                "timestamp" => Some(
                    PhysicalSortExpr::new_default(Arc::new(Column::new(field.name(), index))).asc(),
                ),
                _ => None,
            })
            .collect::<Vec<_>>();

        // Create a LexOrdering with the sort expressions
        let ordering = LexOrdering::new(sort_exprs);

        // Create EquivalenceProperties with the ordering
        let eq_properties = if !ordering.is_empty() {
            EquivalenceProperties::new_with_orderings(projected_schema.clone(), &[ordering])
        } else {
            EquivalenceProperties::new(projected_schema.clone())
        };

        Self {
            data_source,
            projected_schema: projected_schema.clone(),
            plan_properties: PlanProperties::new(
                eq_properties,
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
