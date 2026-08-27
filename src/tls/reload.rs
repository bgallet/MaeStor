use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arc_swap::ArcSwap;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use rustls::ServerConfig;

use super::{load_server_config, TlsConfig, TlsError};

/// Wraps a `rustls::ServerConfig` kept up to date with the files named in a
/// `TlsConfig`, without needing to restart whatever's using it. A
/// background task watches the *parent directories* of those files, not the
/// files themselves — common rotation tools (certbot, `kubectl cp`) replace
/// a file via rename, which an inode-level watch would miss. A reload that
/// fails to parse (e.g. caught mid-rewrite) is logged and discarded; the
/// last-known-good config keeps serving.
pub struct ReloadableConfig {
    current: Arc<ArcSwap<ServerConfig>>,
    // Held only to keep the watcher alive — notify stops delivering events
    // once its `Watcher` is dropped.
    _watcher: RecommendedWatcher,
}

impl ReloadableConfig {
    pub fn start(config: TlsConfig) -> Result<Self, TlsError> {
        let initial = load_server_config(&config)?;
        let current = Arc::new(ArcSwap::new(Arc::new(initial)));

        let mut dirs: HashSet<PathBuf> = HashSet::new();
        for path in [Some(&config.cert_chain_path), Some(&config.private_key_path), config.client_ca_path.as_ref()]
            .into_iter()
            .flatten()
        {
            if let Some(dir) = parent_dir(path) {
                dirs.insert(dir);
            }
        }

        let (tx, rx) = std::sync::mpsc::channel();
        let mut watcher = notify::recommended_watcher(tx)
            .map_err(|err| TlsError::Io(std::io::Error::other(err)))?;
        for dir in &dirs {
            watcher
                .watch(dir, RecursiveMode::NonRecursive)
                .map_err(|err| TlsError::Io(std::io::Error::other(err)))?;
        }

        let reload_current = Arc::clone(&current);
        tokio::task::spawn_blocking(move || {
            while let Ok(event_result) = rx.recv() {
                match event_result {
                    Ok(_event) => match load_server_config(&config) {
                        Ok(new_config) => {
                            reload_current.store(Arc::new(new_config));
                            tracing::info!("TLS configuration reloaded");
                        }
                        Err(err) => {
                            tracing::warn!(error = %err, "TLS reload failed; keeping previous configuration");
                        }
                    },
                    Err(err) => {
                        tracing::warn!(error = %err, "file watcher error");
                    }
                }
            }
        });

        Ok(ReloadableConfig { current, _watcher: watcher })
    }

    pub fn current(&self) -> Arc<ServerConfig> {
        self.current.load_full()
    }
}

/// The directory to watch for changes to `path`.
///
/// `Path::parent()` on a bare filename with no directory component (e.g.
/// `"chain.pem"`) returns `Some("")`, not `None` — and `Watcher::watch("")`
/// fails outright, which used to make `ReloadableConfig::start` (and thus the
/// whole server) refuse to start on a config that pointed at files in the
/// current directory. Treat an empty parent as "the current directory"
/// instead.
fn parent_dir(path: &Path) -> Option<PathBuf> {
    let parent = path.parent()?;
    if parent.as_os_str().is_empty() {
        Some(PathBuf::from("."))
    } else {
        Some(parent.to_path_buf())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_filename_parent_dir_is_current_dir() {
        assert_eq!(parent_dir(Path::new("chain.pem")), Some(PathBuf::from(".")));
    }

    #[test]
    fn nested_relative_path_parent_dir_is_its_directory() {
        assert_eq!(parent_dir(Path::new("certs/chain.pem")), Some(PathBuf::from("certs")));
    }

    #[test]
    fn absolute_path_parent_dir_is_its_directory() {
        assert_eq!(parent_dir(Path::new("/etc/tls/chain.pem")), Some(PathBuf::from("/etc/tls")));
    }
}
