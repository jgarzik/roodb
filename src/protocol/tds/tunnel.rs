//! TLS handshake tunnelling for TDS 7.x.
//!
//! A TDS 7.x client sends PRELOGIN in cleartext and then starts TLS, but
//! until the handshake completes each side wraps its TLS records in TDS
//! PRELOGIN packets. Once the handshake is done, TLS records travel
//! directly on the socket and TDS packets move inside TLS.
//!
//! [`PreloginTlsStream`] sits between the socket and the TLS acceptor. In
//! tunnelling mode it strips the TDS packet headers from what the TLS
//! layer reads and adds them to what it writes; after
//! [`PreloginTlsStream::finish_handshake`] it passes bytes through.

use std::io;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::codec::{PacketType, DEFAULT_PACKET_SIZE, HEADER_SIZE, MAX_PACKET_SIZE, STATUS_EOM};

/// Stream wrapper that frames the TLS handshake in TDS PRELOGIN packets.
pub struct PreloginTlsStream<S> {
    inner: S,
    tunnelling: bool,

    /// Header of the TDS packet being read
    header: [u8; HEADER_SIZE],
    header_filled: usize,
    /// Body bytes of the current TDS packet not yet handed to TLS
    body_remaining: usize,

    /// TLS bytes written since the last flush, not yet framed
    pending: Vec<u8>,
    /// Framed packets being written to the socket
    framed: Vec<u8>,
    framed_written: usize,
}

impl<S> PreloginTlsStream<S> {
    /// Wrap `inner`, starting in tunnelling mode.
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            tunnelling: true,
            header: [0; HEADER_SIZE],
            header_filled: 0,
            body_remaining: 0,
            pending: Vec::new(),
            framed: Vec::new(),
            framed_written: 0,
        }
    }

    /// Switch to pass-through once the TLS handshake has completed.
    ///
    /// Fails if handshake bytes are still buffered in either direction,
    /// which would mean the peer did not follow the protocol.
    pub fn finish_handshake(&mut self) -> io::Result<()> {
        let idle = self.body_remaining == 0
            && self.header_filled == 0
            && self.pending.is_empty()
            && self.framed_written == self.framed.len();
        if !idle {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "TLS handshake data left in the TDS tunnel",
            ));
        }
        self.tunnelling = false;
        self.framed = Vec::new();
        self.framed_written = 0;
        Ok(())
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

impl<S: AsyncRead + Unpin> AsyncRead for PreloginTlsStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if !this.tunnelling {
            return Pin::new(&mut this.inner).poll_read(cx, buf);
        }
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        // Read packet headers until one has a body. Only the header and
        // then exactly the body are taken from the socket, so nothing that
        // follows the handshake is consumed here.
        while this.body_remaining == 0 {
            while this.header_filled < HEADER_SIZE {
                let mut header_buf = ReadBuf::new(&mut this.header[this.header_filled..]);
                ready!(Pin::new(&mut this.inner).poll_read(cx, &mut header_buf))?;
                let n = header_buf.filled().len();
                if n == 0 {
                    // EOF. Clean only between packets.
                    return if this.header_filled == 0 {
                        Poll::Ready(Ok(()))
                    } else {
                        Poll::Ready(Err(io::ErrorKind::UnexpectedEof.into()))
                    };
                }
                this.header_filled += n;
            }
            this.header_filled = 0;

            if this.header[0] != PacketType::PreLogin as u8 {
                return Poll::Ready(Err(invalid(
                    "expected a PRELOGIN packet during the TLS handshake",
                )));
            }
            let length = u16::from_be_bytes([this.header[2], this.header[3]]) as usize;
            if !(HEADER_SIZE..=MAX_PACKET_SIZE).contains(&length) {
                return Poll::Ready(Err(invalid("invalid TDS packet length")));
            }
            this.body_remaining = length - HEADER_SIZE;
        }

        let limit = this.body_remaining.min(buf.remaining());
        let mut body_buf = ReadBuf::new(buf.initialize_unfilled_to(limit));
        ready!(Pin::new(&mut this.inner).poll_read(cx, &mut body_buf))?;
        let n = body_buf.filled().len();
        if n == 0 {
            return Poll::Ready(Err(io::ErrorKind::UnexpectedEof.into()));
        }
        buf.advance(n);
        this.body_remaining -= n;
        Poll::Ready(Ok(()))
    }
}

impl<S: AsyncWrite + Unpin> PreloginTlsStream<S> {
    /// Frame everything written since the last flush as one PRELOGIN
    /// message, then write it out.
    fn poll_flush_tunnel(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            // Finish the frame in progress before framing more: bytes
            // written after a partial flush wait in `pending`.
            while self.framed_written < self.framed.len() {
                let n =
                    ready!(Pin::new(&mut self.inner)
                        .poll_write(cx, &self.framed[self.framed_written..]))?;
                if n == 0 {
                    return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
                }
                self.framed_written += n;
            }
            if self.pending.is_empty() {
                return Poll::Ready(Ok(()));
            }

            self.framed.clear();
            self.framed_written = 0;
            let body_size = DEFAULT_PACKET_SIZE - HEADER_SIZE;
            let mut chunks = self.pending.chunks(body_size).peekable();
            let mut packet_number = 1u8;
            while let Some(body) = chunks.next() {
                let length = ((HEADER_SIZE + body.len()) as u16).to_be_bytes();
                let status = if chunks.peek().is_none() {
                    STATUS_EOM
                } else {
                    0
                };
                self.framed.extend_from_slice(&[
                    PacketType::PreLogin as u8,
                    status,
                    length[0],
                    length[1],
                    0,
                    0,
                    packet_number,
                    0,
                ]);
                self.framed.extend_from_slice(body);
                packet_number = packet_number.wrapping_add(1);
            }
            self.pending.clear();
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for PreloginTlsStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if !this.tunnelling {
            return Pin::new(&mut this.inner).poll_write(cx, buf);
        }
        // Collect the TLS flight; it is framed and sent on flush.
        this.pending.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.tunnelling {
            ready!(this.poll_flush_tunnel(cx))?;
        }
        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.tunnelling {
            ready!(this.poll_flush_tunnel(cx))?;
        }
        Pin::new(&mut this.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn packet(packet_type: u8, status: u8, body: &[u8]) -> Vec<u8> {
        let len = ((HEADER_SIZE + body.len()) as u16).to_be_bytes();
        let mut p = vec![packet_type, status, len[0], len[1], 0, 0, 1, 0];
        p.extend_from_slice(body);
        p
    }

    #[tokio::test]
    async fn read_strips_headers_and_stops_at_the_packet_boundary() {
        let mut wire = packet(0x12, 0, b"hello ");
        wire.extend(packet(0x12, STATUS_EOM, b"")); // empty packet is skipped
        wire.extend(packet(0x12, STATUS_EOM, b"world"));
        wire.extend_from_slice(b"RAW-TLS-RECORD");

        let (mut client, server) = tokio::io::duplex(1024);
        client.write_all(&wire).await.unwrap();

        let mut stream = PreloginTlsStream::new(server);
        let mut got = vec![0u8; 11];
        stream.read_exact(&mut got).await.unwrap();
        assert_eq!(got, b"hello world");

        // Nothing past the last tunnelled packet was consumed.
        stream.finish_handshake().unwrap();
        let mut raw = vec![0u8; 14];
        stream.read_exact(&mut raw).await.unwrap();
        assert_eq!(raw, b"RAW-TLS-RECORD");
    }

    #[tokio::test]
    async fn read_handles_bytes_arriving_one_at_a_time() {
        let wire = packet(0x12, STATUS_EOM, b"abc");
        let (mut client, server) = tokio::io::duplex(1);
        let writer = tokio::spawn(async move {
            for b in wire {
                client.write_all(&[b]).await.unwrap();
            }
            client
        });
        let mut stream = PreloginTlsStream::new(server);
        let mut got = [0u8; 3];
        stream.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"abc");
        drop(writer.await.unwrap());
    }

    #[tokio::test]
    async fn read_rejects_other_packet_types_and_bad_lengths() {
        for wire in [
            packet(0x01, STATUS_EOM, b"SELECT 1"),
            vec![0x12, 0x01, 0x00, 0x03, 0, 0, 1, 0],
            vec![0x12, 0x01, 0xFF, 0xFF, 0, 0, 1, 0],
        ] {
            let (mut client, server) = tokio::io::duplex(1024);
            client.write_all(&wire).await.unwrap();
            let mut stream = PreloginTlsStream::new(server);
            let err = stream.read_u8().await.unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        }
    }

    #[tokio::test]
    async fn read_reports_truncated_packets() {
        // EOF inside a header
        let (mut client, server) = tokio::io::duplex(1024);
        client.write_all(&[0x12, 0x01, 0x00]).await.unwrap();
        drop(client);
        let mut stream = PreloginTlsStream::new(server);
        let err = stream.read_u8().await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);

        // EOF inside a body
        let mut wire = packet(0x12, STATUS_EOM, b"abcdef");
        wire.truncate(HEADER_SIZE + 2);
        let (mut client, server) = tokio::io::duplex(1024);
        client.write_all(&wire).await.unwrap();
        drop(client);
        let mut stream = PreloginTlsStream::new(server);
        let mut got = [0u8; 6];
        let err = stream.read_exact(&mut got).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn writes_are_framed_on_flush() {
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let mut stream = PreloginTlsStream::new(server);

        stream.write_all(b"server ").await.unwrap();
        stream.write_all(b"hello").await.unwrap();
        stream.flush().await.unwrap();

        let mut got = vec![0u8; HEADER_SIZE + 12];
        client.read_exact(&mut got).await.unwrap();
        assert_eq!(got, packet(0x12, STATUS_EOM, b"server hello"));

        // A flight larger than one packet is split; only the last has EOM.
        let big = vec![0xAB; DEFAULT_PACKET_SIZE];
        stream.write_all(&big).await.unwrap();
        stream.flush().await.unwrap();
        let body = DEFAULT_PACKET_SIZE - HEADER_SIZE;
        let mut first = vec![0u8; DEFAULT_PACKET_SIZE];
        client.read_exact(&mut first).await.unwrap();
        assert_eq!((first[0], first[1], first[6]), (0x12, 0, 1));
        let mut second = vec![0u8; HEADER_SIZE + (big.len() - body)];
        client.read_exact(&mut second).await.unwrap();
        assert_eq!((second[0], second[1], second[6]), (0x12, STATUS_EOM, 2));
    }

    /// A flush interrupted by a full socket must still send what was
    /// written in the meantime.
    #[tokio::test]
    async fn flush_after_partial_write_sends_everything() {
        use std::future::poll_fn;

        // Room for only part of the first flight.
        let (mut client, server) = tokio::io::duplex(64);
        let mut stream = PreloginTlsStream::new(server);

        stream.write_all(&[1u8; 200]).await.unwrap();
        // First flush cannot complete: the pipe is full.
        let first = poll_fn(|cx| Poll::Ready(Pin::new(&mut stream).poll_flush(cx))).await;
        assert!(first.is_pending());
        // More handshake bytes arrive before the flush is retried.
        stream.write_all(&[2u8; 50]).await.unwrap();

        let reader = tokio::spawn(async move {
            let mut got = vec![0u8; (HEADER_SIZE + 200) + (HEADER_SIZE + 50)];
            client.read_exact(&mut got).await.unwrap();
            got
        });
        stream.flush().await.unwrap();
        let got = reader.await.unwrap();

        let mut want = packet(0x12, STATUS_EOM, &[1u8; 200]);
        want.extend(packet(0x12, STATUS_EOM, &[2u8; 50]));
        assert_eq!(got, want);
        stream.finish_handshake().unwrap();
    }

    #[tokio::test]
    async fn after_the_handshake_bytes_pass_through() {
        let (mut client, server) = tokio::io::duplex(1024);
        let mut stream = PreloginTlsStream::new(server);
        stream.finish_handshake().unwrap();

        stream.write_all(b"raw out").await.unwrap();
        stream.flush().await.unwrap();
        let mut got = [0u8; 7];
        client.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"raw out");

        client.write_all(b"raw in").await.unwrap();
        let mut got = [0u8; 6];
        stream.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"raw in");
    }

    #[tokio::test]
    async fn finishing_with_buffered_handshake_data_is_an_error() {
        // Unflushed output
        let (_client, server) = tokio::io::duplex(1024);
        let mut stream = PreloginTlsStream::new(server);
        stream.write_all(b"unsent").await.unwrap();
        assert!(stream.finish_handshake().is_err());

        // Half-read packet
        let (mut client, server) = tokio::io::duplex(1024);
        client
            .write_all(&packet(0x12, STATUS_EOM, b"abcd"))
            .await
            .unwrap();
        let mut stream = PreloginTlsStream::new(server);
        stream.read_u8().await.unwrap();
        assert!(stream.finish_handshake().is_err());
    }
}
