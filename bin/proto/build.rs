use std::path::PathBuf;

// protox parses the .proto in Rust, so no protoc install is needed
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let fds = protox::compile(["feed.proto"], ["."])?;

    // the messages and the client
    tonic_prost_build::configure()
        .build_server(false)
        .compile_fds(fds.clone())?;

    // the server alone, sending responses the hub already encoded
    let encoded = PathBuf::from(std::env::var("OUT_DIR")?).join("encoded");
    std::fs::create_dir_all(&encoded)?;
    tonic_prost_build::configure()
        .build_client(false)
        .extern_path(".shredwire", "crate")
        .extern_path(".shredwire.SubscribeResponse", "crate::encoded::Encoded")
        .out_dir(encoded)
        .compile_fds(fds)?;

    println!("cargo:rerun-if-changed=feed.proto");
    Ok(())
}
