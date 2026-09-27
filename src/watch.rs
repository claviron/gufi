//! Restart the workers when the application's Python changes: development's
//! `--reload`, done by the master.
//!
//! An interpreter is not reloaded in place. Finalizing CPython and starting it
//! again in the same process is not safe with extension modules loaded, and
//! re-importing a changed module leaves every other module holding the old
//! one — so a change means a new process. The fleet already has the way to
//! get one without dropping a request: the supervisor's rolling restart, one
//! worker at a time, each drained before its replacement starts, the same one
//! SIGUSR1 asks for. The master watches and asks; it runs no Python itself.
//!
//! Hence a fleet of two or more is required. With `workers: 0` there is no
//! master to outlive the interpreter, and with one there is nothing to roll
//! over to — the supervisor does not stop the last running worker — so the
//! launch is refused rather than restarting by cutting requests.
//!
//! The watching itself is the runner shell's (`claviron::runner::watch`);
//! what is Python's is what counts as source and what a change does.

use std::path::PathBuf;
use std::sync::Arc;

use claviron::runner::watch::{Reaction, Source, SourceWatcher};

/// Python source; what the application root holds that is not source; and,
/// with no paths written, every directory of the root but those.
const PYTHON: Source = Source {
    extensions: &["py"],
    skip: &[
        ".venv",
        ".venv-linux",
        "__pycache__",
        "target",
        "storage",
        "public",
        "node_modules",
        ".git"
    ],
    defaults: &[]
};

/// The master's watch: a change asks the supervisor for a rolling restart.
pub(crate) fn watcher(
    root: PathBuf,
    paths: Vec<PathBuf>
) -> SourceWatcher {
    let restart: Reaction = Arc::new(|state| {
        claviron::supervisor::WorkerManager::new(state)
            .and_then(|manager| manager.restart_workers())
            .map(|command| command.to_string())
            .map_err(|error| error.to_string())
    });
    SourceWatcher::new(root, paths, PYTHON, restart)
}
