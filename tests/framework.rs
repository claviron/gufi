use super::*;

#[test]
fn runtime_dependencies_identify_known_frameworks() {
    for (text, expected) in [
        (
            "[project]\ndependencies = ['FastAPI[standard]>=0.1; python_version >= \"3.12\"']",
            vec!["FastAPI"],
        ),
        (
            "[project]\ndependencies = ['django @ https://example.test/django.whl']",
            vec!["Django"],
        ),
        (
            "[project]\ndependencies = ['fastapi', 'Django>=5']",
            vec!["FastAPI", "Django"],
        ),
        (
            "[project]\ndependencies = ['fastapi-cli', 'jinja2']\n[dependency-groups]\ndev = ['django']\n[project.optional-dependencies]\nweb = ['fastapi']",
            vec![],
        ),
        ("[project]\nname = 'django-example'", vec![]),
        (
            "[tool.poetry.dependencies]\npython = '^3.12'\nfastapi = { version = '*' }\nflask = { version = '*', optional = true }\n[tool.poetry.group.dev.dependencies]\ndjango = '*'",
            vec!["FastAPI"],
        ),
        (
            "[project]\ndependencies = ['fastapi']\n[tool.poetry.dependencies]\nfastapi = '*'",
            vec!["FastAPI"],
        ),
    ] {
        assert_eq!(detect(text).expect("valid pyproject"), expected);
    }
}

#[test]
fn explicit_framework_is_preserved_and_missing_metadata_stays_unknown() {
    let directory = tempfile::tempdir().expect("temporary root");
    let mut framework = None;
    resolve(directory.path(), &mut framework);
    assert_eq!(framework, None);

    std::fs::write(
        directory.path().join("pyproject.toml"),
        "[project]\ndependencies = ['fastapi']",
    )
    .expect("write pyproject");
    framework = Some(vec!["Custom".to_owned(), "Private".to_owned()]);
    resolve(directory.path(), &mut framework);
    assert_eq!(framework.unwrap(), ["Custom", "Private"]);

    framework = Some(vec![]);
    resolve(directory.path(), &mut framework);
    assert_eq!(framework, Some(vec![]));

    framework = None;
    resolve(directory.path(), &mut framework);
    assert_eq!(framework.unwrap(), ["FastAPI"]);
}

#[test]
fn invalid_metadata_does_not_prevent_launch() {
    let directory = tempfile::tempdir().expect("temporary root");
    std::fs::write(directory.path().join("pyproject.toml"), "[project")
        .expect("write invalid pyproject");
    let mut framework = None;
    resolve(directory.path(), &mut framework);
    assert_eq!(framework, None);
}
