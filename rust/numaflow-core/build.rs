// Build script to generate Rust gRPC code from protobuf

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_file = "../../kafka-reconciler-sidecar/api/reconciler/v1/reconciler.proto";
    
    if std::path::Path::new(proto_file).exists() {
        // tonic-prost-build 0.14 - correct API
        tonic_prost_build::configure()
            .build_server(false)
            .compile_protos(&[proto_file], &["../../kafka-reconciler-sidecar/api"])?;
        
        println!("cargo:rerun-if-changed={}", proto_file);
    } else {
        println!("cargo:warning=Sidecar proto file not found at {}", proto_file);
    }
    
    Ok(())
}
