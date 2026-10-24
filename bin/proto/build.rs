// protox parses the .proto in Rust, so no protoc install is needed
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let fds = protox::compile(["feed.proto"], ["."])?;
    tonic_prost_build::configure().compile_fds(fds)?;
    println!("cargo:rerun-if-changed=feed.proto");
    Ok(())
}
