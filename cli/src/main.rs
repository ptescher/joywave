use argh::FromArgs;
use datafusion::error::Result;
use datafusion_postgres::{DfSessionService, HandlerFactory};
use pgwire::tokio::process_socket;
use std::sync::Arc;
use tokio::net::TcpListener;

#[derive(FromArgs)]
/// Reach new heights.
struct Args {
    /// start a PostgreSQL compatable server
    #[argh(switch, short = 's')]
    serve: bool,

    /// start a CLI
    #[argh(switch, short = 'c')]
    cli: bool,

    /// enable ballista
    #[argh(switch, short = 'b')]
    enable_ballista: bool,
}

#[tokio::main()]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args: Args = argh::from_env();

    let ctx = catalog::default_context::create_default_context(args.enable_ballista).await?;

    let factory = Arc::new(HandlerFactory(Arc::new(DfSessionService::new(ctx))));

    if args.serve {
        let listener: TcpListener = TcpListener::bind("0.0.0.0:5432").await.unwrap();
        loop {
            let incoming_socket: (tokio::net::TcpStream, std::net::SocketAddr) =
                listener.accept().await.unwrap();
            let factory_ref = factory.clone();

            tokio::spawn(async move { process_socket(incoming_socket.0, None, factory_ref).await });
        }
    } else if args.cli {
        panic!("CLI not implemented. need to get datafusion-dft working first.");
    } else {
        Ok(())
    }
}
