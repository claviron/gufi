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
    assert_eq!(
        python.implementation(),
        pyo3_build_config::PythonImplementation::CPython,
        "Gufi embeds the selected CPython build, not an alternative interpreter"
    );
    assert_eq!(
        python.version(),
        pyo3_build_config::PythonVersion { major: 3, minor: 15 },
        "Gufi requires CPython 3.15; select the owned interpreter with PYO3_PYTHON"
    );
    assert!(
        matches!(
            python.target_abi().kind(),
            pyo3_build_config::PythonAbiKind::VersionSpecific(_)
        ),
        "Gufi uses the selected CPython 3.15 ABI, not a multi-version abi3/abi3t build"
    );
    let executable = python.executable().expect(
        "Gufi requires an explicit interpreter executable; set PYO3_PYTHON or executable in PYO3_CONFIG_FILE",
    );
    println!("cargo:rustc-env=GUFI_PYTHON_EXECUTABLE={executable}");
    if python.shared()
        && let Some(directory) = python.lib_dir()
    {
        println!("cargo:python_lib_dir={directory}");
    }
}
