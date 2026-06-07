fn main() {
    let fds = protox::compile(["proto/hermesmq.proto"], ["proto"]).expect("protox: compile protos");
    prost_build::Config::new()
        .compile_fds(fds)
        .expect("prost-build: generate code");
    println!("cargo:rerun-if-changed=proto/hermesmq.proto");
}
