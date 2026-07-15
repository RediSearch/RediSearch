/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Spawns and supervises a `redis-server` with `redisearch.so` loaded. Stdout
//! and stderr are captured to a logfile so ASan/UBSan reports are visible.

use std::net::TcpListener;
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};

#[derive(Clone)]
pub struct ServerConfig {
    pub redis_server: String,
    pub module: PathBuf,
    pub module_args: Vec<String>,
    /// Additional modules to load before RediSearch (e.g. RedisJSON), each with
    /// its own load args.
    pub extra_modules: Vec<(PathBuf, Vec<String>)>,
    /// Extra environment for the server process (e.g. ASAN_OPTIONS, an ASan
    /// runtime preload).
    pub env: Vec<(String, String)>,
}

pub struct Server {
    child: Child,
    port: u16,
    log_path: PathBuf,
    _tmp: PathBuf,
}

impl Server {
    /// Start a server, retrying on transient port collisions. A crashed server's
    /// fork-GC child can briefly hold the listening socket, so a freshly chosen
    /// ephemeral port occasionally fails to bind; pick a new one and retry.
    pub fn start(cfg: &ServerConfig) -> Result<Server> {
        if !cfg.module.exists() {
            return Err(anyhow!("module not found: {}", cfg.module.display()));
        }
        let mut last_err = None;
        for _ in 0..8 {
            match Self::try_start(cfg) {
                Ok(s) => return Ok(s),
                Err(e) if e.to_string().contains("Address already in use") => {
                    last_err = Some(e);
                    std::thread::sleep(Duration::from_millis(200));
                }
                Err(e) => return Err(e),
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("server start failed")))
    }

    fn try_start(cfg: &ServerConfig) -> Result<Server> {
        let port = free_port()?;
        let tmp = std::env::temp_dir().join(format!("rsfuzz-{port}"));
        std::fs::create_dir_all(&tmp)?;
        let log_path = tmp.join("server.log");
        let log = std::fs::File::create(&log_path)?;
        let log_err = log.try_clone()?;

        let mut command = Command::new(&cfg.redis_server);
        command
            .arg("--port")
            .arg(port.to_string())
            .arg("--save")
            .arg("")
            .arg("--appendonly")
            .arg("no")
            .arg("--dir")
            .arg(&tmp)
            .arg("--daemonize")
            .arg("no");
        for (path, margs) in &cfg.extra_modules {
            command.arg("--loadmodule").arg(path);
            for a in margs {
                command.arg(a);
            }
        }
        command.arg("--loadmodule").arg(&cfg.module);
        for a in &cfg.module_args {
            command.arg(a);
        }
        for (k, v) in &cfg.env {
            command.env(k, v);
        }
        command
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(log_err));
        // Own process group so teardown can reap fork-GC children too.
        #[cfg(unix)]
        command.process_group(0);

        let child = command
            .spawn()
            .with_context(|| format!("spawning {}", cfg.redis_server))?;

        let mut server = Server {
            child,
            port,
            log_path,
            _tmp: tmp,
        };
        server.wait_ready()?;
        Ok(server)
    }

    pub fn url(&self) -> String {
        format!("redis://127.0.0.1:{}/", self.port)
    }

    /// True while the child process is still running.
    pub fn is_alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    /// Wait up to `dur` for the process to exit (e.g. while ASan is slowly
    /// symbolizing a crash report). Returns true if it exited.
    pub fn wait_exit(&mut self, dur: Duration) -> bool {
        let deadline = Instant::now() + dur;
        while Instant::now() < deadline {
            if matches!(self.child.try_wait(), Ok(Some(_))) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        matches!(self.child.try_wait(), Ok(Some(_)))
    }

    /// The entire captured server log (stdout+stderr): the ASan/assert report for
    /// a crash, saved alongside the finding and parsed for its signature.
    pub fn read_full_log(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
    }

    /// The last `n` bytes of the log, for concise startup-error messages.
    fn log_tail(&self, n: usize) -> String {
        let content = std::fs::read_to_string(&self.log_path).unwrap_or_default();
        let start = content.len().saturating_sub(n);
        content[start..].to_string()
    }

    pub fn stop(&mut self) {
        // Kill the whole process group (the server is a group leader, so its
        // fork-GC children share its pgid) before reaping the leader itself.
        #[cfg(unix)]
        {
            let pgid = self.child.id();
            let _ = Command::new("kill")
                .arg("-KILL")
                .arg(format!("-{pgid}"))
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
        // Reclaim the per-port working dir (log + any dump) so /tmp does not fill
        // up over a long campaign. Callers save the full log before stopping.
        let _ = std::fs::remove_dir_all(&self._tmp);
    }

    fn wait_ready(&mut self) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(15);
        let url = self.url();
        while Instant::now() < deadline {
            if let Ok(Some(status)) = self.child.try_wait() {
                return Err(anyhow!(
                    "server exited during startup ({status}); log tail:\n{}",
                    self.log_tail(4000)
                ));
            }
            if let Ok(client) = redis::Client::open(url.as_str()) {
                if let Ok(mut con) = client.get_connection() {
                    if redis::cmd("PING").query::<String>(&mut con).is_ok() {
                        return Ok(());
                    }
                }
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        Err(anyhow!(
            "server not ready within timeout; log tail:\n{}",
            self.log_tail(4000)
        ))
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop();
    }
}

fn free_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    Ok(port)
}
