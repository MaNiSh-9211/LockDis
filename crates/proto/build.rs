fn main() {
    // SAFETY: build scripts run this before spawning any other threads that
    // read the environment.
    unsafe {
        std::env::set_var(
            "PROTOC",
            protoc_bin_vendored::protoc_bin_path().expect("vendored protoc binary"),
        );
    }
    tonic_prost_build::compile_protos("proto/palisade.v1.proto").expect("proto compilation");
}
