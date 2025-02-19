fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/firehose.proto")?;
    tonic_build::compile_protos("proto/ethereum.proto")?;
    tonic_build::compile_protos("proto/solana.proto")?;
    Ok(())
}
