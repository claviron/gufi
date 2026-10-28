//! Preserve the runner's Python link metadata for existing application builds.
//!
//! The engine owns Python selection and ABI validation. The runner forwards
//! its shared-library directory as `DEP_GUFI_PYTHON_LIB_DIR`, so existing
//! application build scripts keep the same rpath contract.
fn main() {
    if let Some(directory) = std::env::var_os("DEP_GUFI_ASGI_PYTHON_LIB_DIR") {
        println!(
            "cargo:python_lib_dir={}",
            std::path::PathBuf::from(directory).display()
        );
    }
}
