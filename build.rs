fn main() {
    println!("cargo:rerun-if-changed=proto/offline_wire_formats.proto");
    prost_build::compile_protos(
        &["proto/offline_wire_formats.proto"],
        &["proto"],
    )
    .expect("failed to compile offline_wire_formats.proto");
}
