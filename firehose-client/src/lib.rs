pub use prost;
pub use tonic;

pub mod firehose {
    include!(concat!(env!("OUT_DIR"), "/sf.firehose.v2.rs"));
}

pub mod ethereum {
    include!(concat!(env!("OUT_DIR"), "/sf.ethereum.r#type.v2.rs"));
}

pub mod solana {
    include!(concat!(env!("OUT_DIR"), "/sf.solana.r#type.v1.rs"));
}
