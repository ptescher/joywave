use std::{collections::HashMap, sync::Arc};

use ballista_core::error::Result;
use datafusion_table_providers::{
    postgres::PostgresTableFactory,
    sql::db_connection_pool::{
        dbconnection::AsyncDbConnection, postgrespool::PostgresConnectionPool,
    },
    util::secrets::to_secret_map,
};
use futures::StreamExt;

#[tokio::main()]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let ctx = catalog::default_context::create_default_context(false).await?;

    ctx.sql("SET datafusion.execution.batch_size to 100")
        .await?;
    ctx.sql("SET datafusion.execution.target_partitions to 1")
        .await?;

    let postgres_params = to_secret_map(HashMap::from([
        ("host".to_string(), "localhost".to_string()),
        ("user".to_string(), "root".to_string()),
        ("db".to_string(), "defaultdb".to_string()),
        // ("pass".to_string(), "".to_string()),
        ("port".to_string(), "26257".to_string()),
        ("sslmode".to_string(), "disable".to_string()),
    ]));

    // Create PostgreSQL connection pool
    let postgres_pool: Arc<PostgresConnectionPool> = PostgresConnectionPool::new(postgres_params)
        .await
        .expect("unable to create PostgreSQL connection pool")
        .into();

    let conn = postgres_pool.connect_direct().await.unwrap();
    conn.execute(" DROP TABLE IF EXISTS token_transfers", &[])
        .await
        .unwrap();
    conn.execute(
        r#"
    CREATE TABLE token_transfers (
        id STRING PRIMARY KEY,
        transaction_hash BYTES NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        transfer_from BYTES NOT NULL,
        transfer_to BYTES NOT NULL,
        token_address BYTES NOT NULL,
        amount TEXT NOT NULL
    )
    "#,
        &[],
    )
    .await
    .unwrap();

    let table_provider = PostgresTableFactory::new(postgres_pool)
        .read_write_table_provider("token_transfers".into())
        .await
        .unwrap();

    ctx.register_table("token_transfers", table_provider)
        .unwrap();

    let df: datafusion::prelude::DataFrame = ctx
        .sql(
            r#"
    INSERT INTO token_transfers
        SELECT
            id,
            transaction_hash,
            timestamp,
            transfer_from,
            transfer_to,
            token_address,
            string_from_uint256_bytes(transfer_amount) as transfer_amount_string
        FROM erc20_transfers
        WHERE  block_number > 21840000
            AND block_number < 21840010
        LIMIT 10000;
"#,
        )
        .await
        .expect("Error making query");

    let start_time = std::time::Instant::now();

    let mut stream = df.execute_stream().await.expect("Could not execute query");

    let mut total_batches = 0;
    while let Some(Ok(batch)) = stream.next().await {
        total_batches = total_batches + batch.num_rows();
        log::info!("\rProcessing {}%...", total_batches);
    }

    let total_transfers = conn
        .execute("SELECT COUNT(*) FROM token_transfers", &[])
        .await
        .unwrap();

    let duration = std::time::Instant::now().duration_since(start_time);
    log::info!("Total time: {:?}", duration);
    log::info!("Total erc20 transfers: {}", total_transfers);
    log::info!(
        "Transfers / second: {}",
        (total_transfers as f64 / duration.as_millis() as f64) * 1000.0
    );

    Ok(())
}
