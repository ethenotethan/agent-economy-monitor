fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc);
    config.compile_protos(
        &[
            "proto/agent_economy/observation/v1/observation.proto",
            "proto/agent_economy/observation/v2/observation.proto",
        ],
        &["proto"],
    )?;
    Ok(())
}
