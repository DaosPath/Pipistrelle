use std::fs;

fn main() {
    println!("cargo:rerun-if-changed=VERSION");
    let version = fs::read_to_string("VERSION")
        .expect("VERSION file must exist")
        .trim()
        .to_string();
    println!("cargo:rustc-env=PIPISTRELLE_RELEASE_VERSION={}", version);
}
