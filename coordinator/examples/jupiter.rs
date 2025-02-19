use ballista_core::error::Result;

#[tokio::main()]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let ctx = coordinator::default_context::create_default_context().await?;

    let df = ctx.sql("SELECT * FROM jupiter_v6_swaps WHERE block_number > 300023000 AND block_number < 300023010").await?;
    df.show().await?;

    Ok(())
}
