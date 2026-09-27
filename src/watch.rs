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
//! Only native notifications are used — FSEvents, inotify, kqueue. A platform
//! that would fall back to polling is refused: a poll is a guess at how often
//! to look.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use claviron::runtime::registry::{self, ProviderOrder};
use claviron::runtime::{AppState, Provider, RunContext, Runnable};
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::Notify;

/// What the application root holds that is not the application's source.
const NOT_SOURCE: &[&str] =
    &[".venv", ".venv-linux", "__pycache__", "target", "storage", "public", "node_modules", ".git"];

/// Every entry of the root that may hold source, when no paths were written:
/// its directories but the ones above, recursively, and its own `.py` files.
/// The root itself is watched without recursion, for a directory added later.
fn default_paths(root: &Path) -> std::io::Result<Vec<(PathBuf, RecursiveMode)>> {
    let mut paths = vec![(root.to_owned(), RecursiveMode::NonRecursive)];
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        if NOT_SOURCE.iter().any(|skip| name == *skip) || name.to_string_lossy().starts_with('.') {
            continue;
        }
        if entry.file_type()?.is_dir() {
            paths.push((entry.path(), RecursiveMode::Recursive));
        }
    }
    Ok(paths)
}

/// A change worth a restart: Python source, or a directory appearing,
/// disappearing or moving, which can carry source without an event per file.
fn relevant(event: &Event) -> bool {
    if matches!(event.kind, EventKind::Access(_)) {
        return false;
    }
    let structural = matches!(
        event.kind,
        EventKind::Create(notify::event::CreateKind::Folder)
            | EventKind::Remove(notify::event::RemoveKind::Folder)
            | EventKind::Modify(notify::event::ModifyKind::Name(_))
    );
    event.need_rescan()
        || event.paths.iter().any(|path| {
            let hidden = path.components().any(|part| {
                let part = part.as_os_str();
                NOT_SOURCE.iter().any(|skip| part == *skip)
            });
            !hidden && (structural || path.extension().is_some_and(|ext| ext == "py"))
        })
}

#[derive(Default)]
struct Changes {
    wake: Notify,
    error: Mutex<Option<String>>
}

/// The watcher's side of [`Changes`], called on its own thread.
struct Handler(Arc<Changes>);

impl notify::EventHandler for Handler {
    fn handle_event(
        &mut self,
        event: notify::Result<Event>
    ) {
        match event {
            Ok(event) if relevant(&event) => self.0.wake.notify_one(),
            Ok(_) => {}
            Err(error) => {
                self.0.error.lock().unwrap().get_or_insert_with(|| error.to_string());
                self.0.wake.notify_one();
            }
        }
    }
}

/// The master's watch over the application's source.
pub(crate) struct SourceWatcher {
    root: PathBuf,
    paths: Vec<PathBuf>
}

impl SourceWatcher {
    pub(crate) fn new(
        root: PathBuf,
        paths: Vec<PathBuf>
    ) -> Self {
        Self { root, paths }
    }

    fn start(&self) -> registry::Result<(RecommendedWatcher, Arc<Changes>, Vec<PathBuf>)> {
        if matches!(RecommendedWatcher::kind(), notify::WatcherKind::PollWatcher) {
            return Err(failure("watch needs native filesystem notifications on this platform"));
        }
        let paths = if self.paths.is_empty() {
            default_paths(&self.root)
                .map_err(|error| failure(format!("watch: read {}: {error}", self.root.display())))?
        } else {
            self.paths
                .iter()
                .map(|path| {
                    let path = if path.is_absolute() { path.clone() } else { self.root.join(path) };
                    let mode = if path.is_dir() {
                        RecursiveMode::Recursive
                    } else {
                        RecursiveMode::NonRecursive
                    };
                    (path, mode)
                })
                .collect()
        };
        let changes = Arc::new(Changes::default());
        let mut watcher =
            RecommendedWatcher::new(Handler(Arc::clone(&changes)), notify::Config::default())
                .map_err(failure)?;
        for (path, mode) in &paths {
            watcher
                .watch(path, *mode)
                .map_err(|error| failure(format!("watch {}: {error}", path.display())))?;
        }
        Ok((watcher, changes, paths.into_iter().map(|(path, _)| path).collect()))
    }
}

#[async_trait]
impl Provider<AppState> for SourceWatcher {
    fn name(&self) -> &'static str {
        "GufiSourceWatcher"
    }

    fn order(&self) -> ProviderOrder {
        ProviderOrder::new()
    }

    fn as_runnable(self: Arc<Self>) -> Option<Arc<dyn Runnable<AppState>>> {
        Some(self)
    }
}

#[async_trait]
impl Runnable<AppState> for SourceWatcher {
    async fn run(
        self: Arc<Self>,
        state: AppState,
        context: RunContext
    ) -> registry::Result<()> {
        let (watcher, changes, paths) = self.start()?;
        tracing::info!(
            ?paths,
            "🐍 watching the application's Python; a change restarts the workers"
        );
        loop {
            tokio::select! {
                biased;
                () = context.cancelled() => break,
                () = changes.wake.notified() => {}
            }
            if let Some(error) = changes.error.lock().unwrap().take() {
                drop(watcher);
                return Err(failure(format!("source watch failed: {error}")));
            }
            // A burst of saves is one wake: the permit is held until taken.
            match claviron::supervisor::WorkerManager::new(&state)
                .and_then(|manager| manager.restart_workers())
            {
                Ok(command) => tracing::info!(%command, "🐍 source changed; workers restarting"),
                Err(error) => tracing::warn!(%error, "🐍 source changed; restart not requested")
            }
        }
        drop(watcher);
        Ok(())
    }
}

fn failure(error: impl std::fmt::Display) -> registry::Error {
    registry::Error::msg(error.to_string())
}
