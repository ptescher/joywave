use ballista_core::error::Result;
use datafusion::arrow::util::pretty;
use futures::StreamExt;

#[tokio::main()]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    log::info!("Counting swaps by block number");

    let ctx = coordinator::default_context::create_default_context(false).await?;

    ctx.sql("SET datafusion.execution.batch_size to 5").await?;
    ctx.sql("SET datafusion.execution.target_partitions to 1")
        .await?;

    let df: datafusion::prelude::DataFrame = ctx
        .sql(
            r#"
            SELECT DATE_TRUNC('second', timestamp) AS time, MAX(block_number) as block_number, count(1) as swap_count FROM jupiter_v6_swaps
            WHERE block_number > -100
            GROUP BY DATE_TRUNC('second', timestamp)
            "#,
        )
        .await
        .unwrap();

    let _ = df
        .clone()
        .explain(true, false)
        .unwrap()
        .show()
        .await
        .unwrap();

    let mut stream = df.execute_stream().await?;

    while let Some(batch) = stream.next().await {
        match batch {
            Ok(batch) => {
                pretty::print_batches(&[batch])?;
            }
            Err(e) => {
                log::error!("Error processing batch: {}", e);
                break;
            }
        }
    }

    Ok(())
}
