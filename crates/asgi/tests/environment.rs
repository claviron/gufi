use super::is_python_315_environment;

#[test]
fn only_declared_python_315_environments_are_accepted() {
    assert!(is_python_315_environment(
        "home = /opt/example/bin\nversion = 3.15.0\n"
    ));
    assert!(is_python_315_environment("version_info = 3.15.1.final.0\n"));
    for config in [
        "",
        "version = 3.14.7",
        "version = 3.16.0",
        "version = 3.150.0",
        "home = /opt/python-3.15/bin",
        "version = 3.14.7\nversion_info = 3.15.0.final.0",
    ] {
        assert!(!is_python_315_environment(config));
    }
}
