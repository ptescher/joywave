use crate::aggregations::uint256::*;
use crate::tables::ethereum_logs::ethereum_mainnet_logs_table;
use crate::tables::jupiter_v6_swaps::jupiter_v6_swaps_table;
use crate::tables::solana_instructions::solana_mainnet_logs_table;
use crate::udfs::byte_parsers::*;
use ballista::prelude::*;
use datafusion::{execution::SessionStateBuilder, prelude::*};
use datafusion_table_providers::{
    common::DatabaseCatalogProvider, postgres::PostgresTableFactory,
    sql::db_connection_pool::postgrespool::PostgresConnectionPool, util::secrets::to_secret_map,
};
use std::{collections::HashMap, sync::Arc};

pub const ENABLE_BALLISTA: bool = false;
pub const ENABLE_POSTGRES: bool = false;

pub async fn create_default_context() -> datafusion::error::Result<SessionContext> {
    let ctx: SessionContext = if ENABLE_BALLISTA {
        let session_config = SessionConfig::new_with_ballista()
            .with_information_schema(true)
            .with_ballista_job_name("Coordinator example")
            .with_ballista_logical_extension_codec(Arc::new(crate::encoder::Codec::new()))
            .with_ballista_physical_extension_codec(Arc::new(crate::encoder::Codec::new()));

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(session_config)
            .build();

        SessionContext::standalone_with_state(state).await?
    } else {
        let state = datafusion_federation::default_session_state();
        SessionContext::new_with_state(state)
    };

    if ENABLE_POSTGRES {
        let postgres_params = to_secret_map(HashMap::from([
            ("host".to_string(), "localhost".to_string()),
            ("user".to_string(), "root".to_string()),
            ("db".to_string(), "defaultdb".to_string()),
            ("pass".to_string(), "".to_string()),
            ("port".to_string(), "26257".to_string()),
            ("sslmode".to_string(), "disable".to_string()),
        ]));

        // Create PostgreSQL connection pool
        let postgres_pool = Arc::new(
            PostgresConnectionPool::new(postgres_params)
                .await
                .expect("unable to create PostgreSQL connection pool"),
        );

        let _table_factory = PostgresTableFactory::new(postgres_pool.clone());
        let catalog = DatabaseCatalogProvider::try_new(postgres_pool)
            .await
            .unwrap();
        ctx.register_catalog("postgres", Arc::new(catalog));
    }

    ctx.register_udf(uint64_from_bytes());
    ctx.register_udf(decimal256_from_bytes());
    ctx.register_udf(string_from_uint256_bytes());
    ctx.register_udf(b58_string_from_bytes());
    ctx.register_udaf(uint256_sum());

    let _ = ctx
        .register_table(
            "ethereum_mainnet_logs",
            Arc::new(ethereum_mainnet_logs_table(0, None)),
        )
        .expect("Could not register table");

    let _ = ctx
        .register_table(
            "solana_mainnet_inner_instructions",
            Arc::new(solana_mainnet_logs_table(0, None)),
        )
        .expect("Could not register table");

    let _ = ctx
        .register_table(
            "jupiter_v6_swaps",
            Arc::new(jupiter_v6_swaps_table(0, None)),
        )
        .expect("Could not register table");

    ctx.sql(
        r#"
        CREATE VIEW erc20_transfers AS
            SELECT timestamp, topics[2] AS transfer_from, topics[3] AS transfer_to, data as transfer_amount, address as token_address, block_number, hash as transaction_hash
            FROM ethereum_mainnet_logs
            WHERE topics[1]::BYTEA = X'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        "#,
    )
    .await
    .expect("Error registering erc20 transfers view");

    Ok(ctx)
}
