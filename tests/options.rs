use super::*;

/// What a worker is handed: the launch, through the envelope's encoding.
fn envelope(options: GufiOptions) -> Launch {
    serde_json::from_slice(&serde_json::to_vec(&options.into_launch()).unwrap()).unwrap()
}

fn launch() -> GufiOptions {
    GufiOptions {
        runner: RunnerOptions {
            name: Some("checkout".to_owned()),
            orbit: true,
            uds: Some("/run/checkout.sock".into()),
            h2: true,
            trusted: vec!["10.0.0.0/8".parse().unwrap(), "::1".parse().unwrap()],
            ..Default::default()
        },
        root: Some("/srv/checkout".into()),
        ..Default::default()
    }
}

/// A worker is started with no command line, so everything it serves has to
/// survive this round trip exactly.
#[test]
fn a_launch_survives_the_envelope() {
    let sent = launch();
    let back = envelope(sent.clone());

    assert_eq!(back.runner.name, sent.runner.name);
    assert_eq!(back.runner.orbit, sent.runner.orbit);
    assert_eq!(back.runner.uds, sent.runner.uds);
    assert_eq!(back.own.root, sent.root);
    assert_eq!(back.runner.h2, sent.runner.h2);
    assert_eq!(
        back.runner.trusted.iter().map(ToString::to_string).collect::<Vec<_>>(),
        ["10.0.0.0/8", "::1"]
    );
}

/// The same launch must produce the same bytes: a master and its workers are
/// compared by what they were started with, and a launch that encoded
/// differently each time could never match.
#[test]
fn the_same_launch_encodes_the_same_bytes() {
    assert_eq!(
        serde_json::to_vec(&launch().into_launch()).unwrap(),
        serde_json::to_vec(&launch().into_launch()).unwrap()
    );
}

/// A trusted network is carried as the text it was written as, so what an
/// operator typed is what a worker inherits.
#[test]
fn a_trusted_network_keeps_its_spelling() {
    let sent = GufiOptions {
        runner: RunnerOptions { trusted: vec!["192.0.2.1".parse().unwrap()], ..Default::default() },
        ..Default::default()
    };
    let back = envelope(sent.clone());
    assert_eq!(back.runner.trusted[0].to_string(), "192.0.2.1");
}

/// The file is the deployment; the command line is what someone typed over
/// it just now. A launch that read a file and ignored the shell would be the
/// wrong way round.
#[test]
fn the_command_line_stands_over_the_file() {
    let directory = tempfile::tempdir().expect("temporary directory");
    std::fs::write(
        directory.path().join(super::CONFIG_FILE),
        "workers: 4\norbit: true\nuds: /run/written.sock\n"
    )
    .expect("write configuration");

    let resolved = GufiOptions {
        runner: RunnerOptions { uds: Some("/run/typed.sock".into()), ..Default::default() },
        config: Some(directory.path().join(super::CONFIG_FILE)),
        ..Default::default()
    }
    .resolve()
    .expect("resolve");

    assert_eq!(resolved.runner.uds.as_deref(), Some(std::path::Path::new("/run/typed.sock")));
    // Untouched by the command line, so the file still decides.
    assert_eq!(resolved.runner.workers, Some(4));
    assert!(resolved.runner.orbit);
}

/// The directory a repository is checked out into is not always what the
/// application is called, so the file may say — and the command line still
/// outranks it.
#[test]
fn the_file_may_name_the_application() {
    let directory = tempfile::tempdir().expect("temporary directory");
    std::fs::write(directory.path().join(super::CONFIG_FILE), "name: written\n")
        .expect("write configuration");

    let read = GufiOptions {
        config: Some(directory.path().join(super::CONFIG_FILE)),
        ..Default::default()
    }
    .resolve()
    .expect("resolve");
    assert_eq!(read.runner.name.as_deref(), Some("written"));

    let typed = GufiOptions {
        runner: RunnerOptions { name: Some("typed".to_owned()), ..Default::default() },
        config: Some(directory.path().join(super::CONFIG_FILE)),
        ..Default::default()
    }
    .resolve()
    .expect("resolve");
    assert_eq!(typed.runner.name.as_deref(), Some("typed"));
}

/// An application nobody configured runs on its defaults; one whose file was
/// named and is not there does not start.
#[test]
fn a_missing_file_is_silent_and_a_named_one_is_not() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let resolved = GufiOptions { root: Some(directory.path().to_path_buf()), ..Default::default() }
        .resolve()
        .expect("an application nobody configured still starts");
    assert_eq!(resolved.runner.workers, None);

    let named =
        GufiOptions { config: Some(directory.path().join("absent.yaml")), ..Default::default() }
            .resolve();
    assert!(named.is_err(), "a named file that is not there is an error");
}

/// The directory the launch was read from is the one Claviron reads
/// `supervisor.yaml` from, so resolving has to report it back.
#[test]
fn resolving_reports_the_file_it_read() {
    let root = tempfile::tempdir().expect("temporary directory");
    let file = root.path().join(super::CONFIG_FILE);
    std::fs::write(&file, "workers: 2\n").expect("write configuration");

    let resolved = GufiOptions { root: Some(root.path().to_path_buf()), ..Default::default() }
        .resolve()
        .expect("resolve");

    assert_eq!(resolved.config.as_deref(), Some(file.as_path()));
    assert_eq!(resolved.runner.workers, Some(2));
}

/// The supervisor's policy rides in the same file and the same envelope, so
/// a worker is supervised by what its master read rather than by a file of
/// Claviron's own.
#[test]
fn the_supervisor_policy_travels_with_the_launch() {
    let root = tempfile::tempdir().expect("temporary directory");
    std::fs::write(
        root.path().join(super::CONFIG_FILE),
        "workers: 2\nsupervisor:\n  heartbeat:\n    interval: 2s\n"
    )
    .expect("write configuration");

    let resolved = GufiOptions { root: Some(root.path().to_path_buf()), ..Default::default() }
        .resolve()
        .expect("resolve");
    let policy = resolved.runner.supervisor.clone().expect("the file stated a policy");
    assert_eq!(policy.heartbeat.interval, std::time::Duration::from_secs(2));

    let back = envelope(resolved.clone());
    assert_eq!(
        back.runner.supervisor.expect("the envelope carried it").heartbeat.interval,
        std::time::Duration::from_secs(2)
    );
}

/// gufi's keys and the runner's share one flat file, and a key neither knows
/// is refused rather than ignored.
#[test]
fn a_key_nobody_knows_is_refused() {
    let read: Launch = serde_yaml::from_str("orbit: true\nvenv: .venv\n").unwrap();
    assert!(read.runner.orbit);
    assert_eq!(read.own.venv.as_deref(), Some(std::path::Path::new(".venv")));

    let typo = serde_yaml::from_str::<Launch>("orbitt: true\n");
    assert!(typo.is_err(), "a mistyped key is an error");
}
