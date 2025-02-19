use ballista_core::error::Result;
use futures::StreamExt;

#[tokio::main()]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let ctx = coordinator::default_context::create_default_context(false).await?;

    let df: datafusion::prelude::DataFrame = ctx
        .sql(
            r#"
SELECT timestamp,
       transaction_hash::BYTEA,
       transfer_from::BYTEA,
       transfer_to::BYTEA,
       String_from_uint256_bytes(transfer_amount) AS transfer_amount_string
FROM   erc20_transfers
WHERE  block_number > 21840000
       AND block_number < 21841000
"#,
        )
        .await
        .expect("Error making query");
    // let explanation = df.clone().explain(true, false)?;
    // explanation.show().await?;

    let mut stream = df.execute_stream().await.expect("Could not execute query");
    let mut log_count = 0;
    let mut update_count = 0;
    let start_time = std::time::Instant::now();
    loop {
        match stream.next().await {
            Some(Ok(batch)) => {
                log_count += batch.num_rows();

                if update_count > 1000 {
                    log::info!("Total erc20 transfers: {}", log_count);
                    update_count = 0;
                } else {
                    update_count += batch.num_rows();
                }

                // pretty::print_batches(&[batch])?
            }
            Some(Err(e)) => {
                eprintln!("Error: {:?}", e);
                break;
            }
            None => {
                break;
            }
        }
    }
    let duration = std::time::Instant::now().duration_since(start_time);
    log::info!("Total time: {:?}", duration);
    log::info!("Total erc20 transfers: {}", log_count);
    log::info!(
        "Transfers / second: {}",
        (log_count as f64 / duration.as_millis() as f64) * 1000.0
    );
    Ok(())
}
