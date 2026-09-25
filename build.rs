//! Hand the application's binary the directory of the Python it links.
//!
//! The binary that embeds gufi is the application's, not gufi's, and a link
//! argument given here would not reach it. So the directory travels as
//! `links` metadata instead: an application's own `build.rs` reads it as
//! `DEP_GUFI_PYTHON_LIB_DIR` and writes it into its binary as the rpath. See
//! the starter's `build.rs` for why the binary needs it.
//!
//! Only for a shared libpython; a static one is inside the binary already.
fn main() {
    println!("cargo:rerun-if-env-changed=PYO3_PYTHON");
    println!("cargo:rerun-if-env-changed=PYO3_CONFIG_FILE");

    let python = pyo3_build_config::get();
    if python.shared()
        && let Some(directory) = python.lib_dir()
    {
        println!("cargo:python_lib_dir={directory}");
    }
}
