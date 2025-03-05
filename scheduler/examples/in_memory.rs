use scheduler::{storage::InMemory, CheckpointManager, Scheduler};

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let in_memory = InMemory::new();
    let ctx = catalog::default_context::create_default_context(false)
        .await
        .unwrap();

    let checkpoint_manager = CheckpointManager::new(in_memory.clone());

    let scheduler = Scheduler::new(ctx, in_memory, checkpoint_manager);

    scheduler
        .execute(
            "token_transfers_batch",
            "SELECT timestamp,
       transaction_hash::BYTEA,
       transfer_from::BYTEA,
       transfer_to::BYTEA,
       String_from_uint256_bytes(transfer_amount) AS transfer_amount_string
FROM   erc20_transfers
WHERE  block_number > 21840000
       AND block_number < 21840020
",
        )
        .await
        .unwrap();

    scheduler
        .run(|batch| {
            log::info!("Batch with {} rows", batch.num_rows());
        })
        .await
        .unwrap();

    log::info!("Done");
}
