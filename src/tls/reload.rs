use std::collections::HashSet;
use std::path::PathBuf;
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
            if let Some(parent) = path.parent() {
                dirs.insert(parent.to_path_buf());
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
