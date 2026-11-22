//! The threads client connections are served on.
//!
//! Each connection thread runs an async runtime of its own, with its own
//! I/O event loop, and a connection stays on the thread it was given. One
//! runtime for all connections has one event loop, and for short requests
//! the loop hands work to the runtime's threads almost one at a time:
//! measured, a few cores were busy however many clients were connected.
//! With an event loop per thread, connections on different threads are
//! served in parallel from the moment their requests arrive.
//!
//! A new connection goes to the thread with the fewest open connections.
//! Connections that share a thread take turns: a statement that computes
//! for long delays the others on its thread, not those on other threads.

use std::future::Future;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::net::TcpStream;
use tokio::runtime::{Builder, Handle, Runtime};

struct ConnectionThread {
    /// The runtime itself; taken at shutdown.
    runtime: Option<Runtime>,
    handle: Handle,
    /// Connections open on this thread.
    open: Arc<AtomicUsize>,
}

/// One less open connection when the connection's task ends, however it
/// ends.
struct Open(Arc<AtomicUsize>);

impl Drop for Open {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

pub struct ConnectionThreads {
    threads: Vec<ConnectionThread>,
}

impl ConnectionThreads {
    /// Start `count` connection threads; 0 means one per core.
    pub fn start(count: usize) -> io::Result<Self> {
        let count = if count == 0 {
            std::thread::available_parallelism().map_or(1, |n| n.get())
        } else {
            count
        };
        let mut threads = Vec::with_capacity(count);
        for index in 0..count {
            let runtime = Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name(format!("roodb-conn-{index}"))
                .enable_all()
                .build()?;
            threads.push(ConnectionThread {
                handle: runtime.handle().clone(),
                runtime: Some(runtime),
                open: Arc::new(AtomicUsize::new(0)),
            });
        }
        Ok(Self { threads })
    }

    /// How many threads there are.
    pub fn count(&self) -> usize {
        self.threads.len()
    }

    /// Connections open on each thread.
    pub fn open(&self) -> Vec<usize> {
        self.threads
            .iter()
            .map(|thread| thread.open.load(Ordering::Relaxed))
            .collect()
    }

    /// Serve an accepted connection on the thread with the fewest open
    /// connections. `serve` is given the stream, registered with that
    /// thread's event loop.
    pub fn serve<F, Fut>(&self, stream: TcpStream, serve: F) -> io::Result<()>
    where
        F: FnOnce(TcpStream) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let thread = self
            .threads
            .iter()
            .min_by_key(|thread| thread.open.load(Ordering::Relaxed))
            .expect("at least one connection thread");
        // The stream was accepted on another runtime: it is taken off that
        // one's event loop and put on this thread's.
        let stream = stream.into_std()?;
        thread.open.fetch_add(1, Ordering::Relaxed);
        let open = Open(thread.open.clone());
        thread.handle.spawn(async move {
            let _open = open;
            match TcpStream::from_std(stream) {
                Ok(stream) => serve(stream).await,
                Err(e) => tracing::warn!(error = %e, "Could not serve an accepted connection"),
            }
        });
        Ok(())
    }
}

impl Drop for ConnectionThreads {
    /// Stop the threads without waiting for their connections: a runtime
    /// may not be waited for from inside another, which is where a server
    /// is dropped.
    fn drop(&mut self) {
        for thread in &mut self.threads {
            if let Some(runtime) = thread.runtime.take() {
                runtime.shutdown_background();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// Connections are spread over the threads, each served on a thread of
    /// the pool, and a closed connection frees its place.
    #[tokio::test]
    async fn connections_go_to_the_least_loaded_thread() {
        let threads = ConnectionThreads::start(3).unwrap();
        assert_eq!(threads.count(), 3);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let mut clients = Vec::new();
        for _ in 0..6 {
            let client = TcpStream::connect(addr).await.unwrap();
            let (stream, _) = listener.accept().await.unwrap();
            threads
                .serve(stream, |mut stream| async move {
                    // Tell the client which thread serves it, then echo
                    // until the client closes.
                    let name = std::thread::current().name().unwrap_or("").to_string();
                    stream
                        .write_all(format!("{name}\n").as_bytes())
                        .await
                        .unwrap();
                    let mut byte = [0u8; 1];
                    while stream.read(&mut byte).await.unwrap_or(0) == 1 {
                        stream.write_all(&byte).await.unwrap();
                    }
                })
                .unwrap();
            clients.push(client);
        }
        assert_eq!(threads.open(), vec![2, 2, 2]);

        let mut names = Vec::new();
        for client in &mut clients {
            let mut name = Vec::new();
            loop {
                let mut byte = [0u8; 1];
                client.read_exact(&mut byte).await.unwrap();
                if byte[0] == b'\n' {
                    break;
                }
                name.push(byte[0]);
            }
            names.push(String::from_utf8(name).unwrap());
            client.write_all(b"x").await.unwrap();
            let mut echoed = [0u8; 1];
            client.read_exact(&mut echoed).await.unwrap();
            assert_eq!(&echoed, b"x");
        }
        names.sort();
        assert_eq!(
            names,
            [
                "roodb-conn-0",
                "roodb-conn-0",
                "roodb-conn-1",
                "roodb-conn-1",
                "roodb-conn-2",
                "roodb-conn-2"
            ]
        );

        // Closing connections frees their places, on the threads they were.
        drop(clients);
        while threads.open().iter().sum::<usize>() > 0 {
            tokio::task::yield_now().await;
        }
    }
}
