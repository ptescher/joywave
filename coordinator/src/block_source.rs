use firehose_client::ethereum::Block as EthereumBlock;
use firehose_client::firehose::{Request, Response};
use firehose_client::prost::Message;
use firehose_client::solana::Block as SolanaBlock;
use firehose_client::tonic::Streaming;
use firehose_client::{
    tonic::codec::CompressionEncoding, tonic::metadata::MetadataValue,
    tonic::Request as TonicRequest,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum BlockType {
    Ethereum,
    Solana,
}

#[derive(Debug, Clone)]
pub enum Block {
    Ethereum { block: EthereumBlock },
    Solana { block: SolanaBlock },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FirehoseEndpoint {
    EthereumMainnet,
    SolanaMainnet,
}

impl FirehoseEndpoint {
    fn as_str(&self) -> &'static str {
        match self {
            FirehoseEndpoint::EthereumMainnet => "https://mainnet.eth.streamingfast.io:443",
            FirehoseEndpoint::SolanaMainnet => "https://mainnet.sol.streamingfast.io:443",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AccessTokenSource {
    Env,
    Static(String),
}

impl AccessTokenSource {
    pub fn as_str(&self) -> String {
        match self {
            AccessTokenSource::Env => std::env::var("SF_API_TOKEN").expect("SF_API_TOKEN not set"),
            AccessTokenSource::Static(string) => string.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum BlockProvider {
    Firehose {
        endpoint: FirehoseEndpoint,
        access_token_source: AccessTokenSource,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlockSourceConfig {
    pub block_type: BlockType,
    pub provider: BlockProvider,
}

#[derive(Debug)]
pub enum BlockSource {
    Firehose {
        response: Streaming<Response>,
        block_type: BlockType,
    },
}

impl BlockSource {
    pub async fn get_latest_block_number(config: BlockSourceConfig) -> u64 {
        match config.provider {
            BlockProvider::Firehose {
                endpoint,
                access_token_source,
            } => {
                let client: firehose_client::tonic::transport::Channel =
                    firehose_client::tonic::transport::Channel::from_static(endpoint.as_str())
                        .tls_config(
                            firehose_client::tonic::transport::ClientTlsConfig::new()
                                .with_native_roots(),
                        )
                        .expect("Invalid TLS config")
                        .connect()
                        .await
                        .expect("Count not connect");

                let mut stream =
                    firehose_client::firehose::stream_client::StreamClient::with_interceptor(
                        client,
                        |mut req: TonicRequest<()>| {
                            let token: MetadataValue<_> =
                                format!("Bearer {}", access_token_source.as_str())
                                    .parse()
                                    .unwrap();
                            req.metadata_mut().insert("authorization", token.clone());

                            Ok(req)
                        },
                    );

                stream = stream.accept_compressed(CompressionEncoding::Gzip);

                let request: Request = Request {
                    start_block_num: -1,
                    ..Default::default()
                };
                let response = stream
                    .blocks(request)
                    .await
                    .expect("Could not stream blocks");

                let next_block_response = response.into_inner().next().await;

                match next_block_response {
                    Some(Ok(response)) => match response.block {
                        Some(block) => match config.block_type {
                            BlockType::Ethereum => {
                                let block = EthereumBlock::decode(block.value.as_slice())
                                    .expect("Could not decode ethereum block");
                                block.number
                            }
                            BlockType::Solana => {
                                let block = SolanaBlock::decode(block.value.as_slice())
                                    .expect("Could not decode solana block");
                                block.slot
                            }
                        },
                        None => {
                            panic!("No block in response");
                        }
                    },
                    Some(Err(e)) => {
                        panic!("Error streaming blocks: {:?}", e);
                    }
                    None => {
                        panic!("No block in response");
                    }
                }
            }
        }
    }

    pub async fn new(config: BlockSourceConfig, first_block: u64, last_block: Option<u64>) -> Self {
        match config.provider {
            BlockProvider::Firehose {
                endpoint,
                access_token_source,
            } => {
                let client: firehose_client::tonic::transport::Channel =
                    firehose_client::tonic::transport::Channel::from_static(endpoint.as_str())
                        .tls_config(
                            firehose_client::tonic::transport::ClientTlsConfig::new()
                                .with_native_roots(),
                        )
                        .expect("Invalid TLS config")
                        .connect()
                        .await
                        .expect("Count not connect");

                let mut stream =
                    firehose_client::firehose::stream_client::StreamClient::with_interceptor(
                        client,
                        |mut req: TonicRequest<()>| {
                            let token: MetadataValue<_> =
                                format!("Bearer {}", access_token_source.as_str())
                                    .parse()
                                    .unwrap();
                            req.metadata_mut().insert("authorization", token.clone());

                            Ok(req)
                        },
                    );

                stream = stream.accept_compressed(CompressionEncoding::Gzip);

                let request: Request = Request {
                    start_block_num: first_block as i64,
                    stop_block_num: last_block.unwrap_or_default(),
                    ..Default::default()
                };
                let response = stream
                    .blocks(request)
                    .await
                    .expect("Could not stream blocks");

                BlockSource::Firehose {
                    response: response.into_inner(),
                    block_type: config.block_type,
                }
            }
        }
    }

    fn response(self) -> Streaming<Response> {
        match self {
            BlockSource::Firehose {
                response,
                block_type: _,
            } => response,
        }
    }

    fn block_type(&self) -> BlockType {
        match self {
            BlockSource::Firehose {
                response: _,
                block_type,
            } => block_type.clone(),
        }
    }

    pub fn stream_blocks(self) -> impl futures::stream::Stream<Item = Block> {
        let block_type: BlockType = self.block_type();
        self.response().map(move |response| match response {
            Err(e) => {
                log::warn!("Error streaming blocks: {:?}", e);
                match block_type {
                    BlockType::Ethereum => Block::Ethereum {
                        block: EthereumBlock::default(),
                    },
                    BlockType::Solana => Block::Solana {
                        block: SolanaBlock::default(),
                    },
                }
            }
            Ok(response) => {
                let block = match response.block {
                    Some(block) => block,
                    None => {
                        return Block::Ethereum {
                            block: EthereumBlock::default(),
                        }
                    }
                };

                let block = match block_type {
                    BlockType::Ethereum => Block::Ethereum {
                        block: EthereumBlock::decode(block.value.as_slice())
                            .expect("Could not decode ethereum block"),
                    },
                    BlockType::Solana => Block::Solana {
                        block: SolanaBlock::decode(block.value.as_slice())
                            .expect("Could not decode solana block"),
                    },
                };

                block
            }
        })
    }

    pub fn stream_batches(
        self,
        capacity: usize,
    ) -> impl futures::stream::Stream<Item = Vec<Block>> {
        self.stream_blocks().chunks(capacity)
    }
}
