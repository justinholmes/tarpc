//! Zero-copy bulk-payload codec at the tokio-util Codec level.
//!
//! This module adds a new codec (`ZeroCopyForyCodec`) and transport API
//! (`ZeroCopyTransport` / `ZeroCopyIncoming`) that operate directly at the
//! `tokio_util::codec` layer — bypassing tokio-serde — so that large body
//! payloads can be sliced out of the incoming frame buffer without copying.
//!
//! # Zero-copy scope
//!
//! **Decode/receive path**: true zero-copy. [`tokio_util::codec::Decoder::decode`]
//! receives a `&mut BytesMut`; after the length-delimited inner codec extracts a
//! frame, `split_off` is used to obtain the body region as a new `BytesMut` that
//! shares the same underlying allocation. `.freeze()` produces a `Bytes` that
//! aliases the same memory — no copy occurs for the body bytes.
//!
//! **Encode/send path**: performs **two memcpys of the body**. `encode_frame`
//! first copies the body into a staging `BytesMut`, then `LengthDelimitedCodec`
//! copies that into the socket write buffer. Future work: a vectored-write Sink
//! that eliminates send-side copies.
//!
//! The canonical proof of receive-side zero-copy is
//! `tarpc/tests/fory_zerocopy.rs::body_is_aliased`.
//!
//! # Wire format
//!
//! Each length-delimited frame contains:
//!
//! ```text
//! [ fory-encoded envelope (variable) ][ body bytes (variable) ][ body_len: u32 LE ]
//! ```
//!
//! If `body_len == 0` there is no bulk payload and the decoded body is `None`.
//!
//! # Wire-format constraint
//!
//! Senders MUST NOT include trailing padding bytes within the envelope region
//! (i.e. between the end of the fory-serialized envelope and the start of the
//! body). Fory's deserialize API does not expose how many bytes were consumed,
//! so the codec cannot detect intra-envelope padding; such bytes would be
//! silently incorporated into the deserialized envelope and may cause corruption.

use super::fory_envelope::{ForyClientMessage, ForyResponse};
use crate::{ClientMessage, Response};
use fory::Fory;
use futures::{ready, Sink, Stream};
use pin_project::pin_project;
use std::{
    io,
    marker::PhantomData,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio_util::bytes::{Bytes, BytesMut};
use tokio_util::codec::{Encoder as _, Framed, LengthDelimitedCodec};

// Maximum frame length: 64 MiB — matches cloudverve fabric framing.
const MAX_FRAME_LEN: usize = 64 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Server-side codec: decodes (ClientMessage<Req>, Option<Bytes>)
//                    encodes (Response<Resp>, Option<Bytes>)
// ---------------------------------------------------------------------------

/// Tokio-util codec for zero-copy bulk-payload framing on the **server** side.
///
/// - `Decoder::Item` = `(ClientMessage<Req>, Option<Bytes>)`
/// - `Encoder::Item` = `(Response<Resp>, Option<Bytes>)`
pub struct ServerZeroCopyCodec<Req, Resp> {
    fory: Arc<Fory>,
    inner: LengthDelimitedCodec,
    _marker: PhantomData<(Req, Resp)>,
}

impl<Req, Resp> ServerZeroCopyCodec<Req, Resp> {
    /// Create a new server-side codec.
    pub fn new(fory: Arc<Fory>) -> Self {
        let mut inner = LengthDelimitedCodec::new();
        inner.set_max_frame_length(MAX_FRAME_LEN);
        Self { fory, inner, _marker: PhantomData }
    }
}

impl<Req, Resp> tokio_util::codec::Decoder for ServerZeroCopyCodec<Req, Resp>
where
    Req: fory::Serializer + fory::ForyDefault + Send + 'static,
    Resp: fory::Serializer + fory::ForyDefault + Clone + Send + 'static,
{
    type Item = (ClientMessage<Req>, Option<Bytes>);
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let mut frame = match self.inner.decode(src)? {
            Some(f) => f,
            None => return Ok(None),
        };
        let (envelope, body) = split_frame_body(&mut frame)?;
        let wrapper: ForyClientMessage<Req> = self
            .fory
            .deserialize(&envelope[..])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        Ok(Some((ClientMessage::from(wrapper), body)))
    }
}

impl<Req, Resp> tokio_util::codec::Encoder<(Response<Resp>, Option<Bytes>)>
    for ServerZeroCopyCodec<Req, Resp>
where
    Req: fory::Serializer + fory::ForyDefault + Send + 'static,
    Resp: fory::Serializer + fory::ForyDefault + Clone + Send + 'static,
{
    type Error = io::Error;

    fn encode(
        &mut self,
        item: (Response<Resp>, Option<Bytes>),
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        let (resp, body) = item;
        let wrapper = ForyResponse::<Resp>::from(&resp);
        let envelope_bytes = self
            .fory
            .serialize(&wrapper)
            .map(Bytes::from)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        encode_frame(&mut self.inner, envelope_bytes, body, dst)
    }
}

// ---------------------------------------------------------------------------
// Client-side codec: decodes (Response<Resp>, Option<Bytes>)
//                    encodes (ClientMessage<Req>, Option<Bytes>)
// ---------------------------------------------------------------------------

/// Tokio-util codec for zero-copy bulk-payload framing on the **client** side.
///
/// - `Decoder::Item` = `(Response<Resp>, Option<Bytes>)`
/// - `Encoder::Item` = `(ClientMessage<Req>, Option<Bytes>)`
pub struct ClientZeroCopyCodec<Req, Resp> {
    fory: Arc<Fory>,
    inner: LengthDelimitedCodec,
    _marker: PhantomData<(Req, Resp)>,
}

impl<Req, Resp> ClientZeroCopyCodec<Req, Resp> {
    /// Create a new client-side codec.
    pub fn new(fory: Arc<Fory>) -> Self {
        let mut inner = LengthDelimitedCodec::new();
        inner.set_max_frame_length(MAX_FRAME_LEN);
        Self { fory, inner, _marker: PhantomData }
    }
}

impl<Req, Resp> tokio_util::codec::Decoder for ClientZeroCopyCodec<Req, Resp>
where
    Req: fory::Serializer + fory::ForyDefault + Clone + Send + 'static,
    Resp: fory::Serializer + fory::ForyDefault + Send + 'static,
{
    type Item = (Response<Resp>, Option<Bytes>);
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let mut frame = match self.inner.decode(src)? {
            Some(f) => f,
            None => return Ok(None),
        };
        let (envelope, body) = split_frame_body(&mut frame)?;
        let wrapper: ForyResponse<Resp> = self
            .fory
            .deserialize(&envelope[..])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        Ok(Some((Response::from(wrapper), body)))
    }
}

impl<Req, Resp> tokio_util::codec::Encoder<(ClientMessage<Req>, Option<Bytes>)>
    for ClientZeroCopyCodec<Req, Resp>
where
    Req: fory::Serializer + fory::ForyDefault + Clone + Send + 'static,
    Resp: fory::Serializer + fory::ForyDefault + Send + 'static,
{
    type Error = io::Error;

    fn encode(
        &mut self,
        item: (ClientMessage<Req>, Option<Bytes>),
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        let (msg, body) = item;
        let wrapper = ForyClientMessage::<Req>::from(&msg);
        let envelope_bytes = self
            .fory
            .serialize(&wrapper)
            .map(Bytes::from)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        encode_frame(&mut self.inner, envelope_bytes, body, dst)
    }
}

// ---------------------------------------------------------------------------
// Shared encode / decode helpers
// ---------------------------------------------------------------------------

/// Split a frame into (envelope_region, body_region) with zero-copy for the body.
///
/// Frame layout: `[ envelope ][ body ][ body_len: u32 LE ]`
///
/// The `body` `BytesMut` is obtained via `split_off`, which gives an owned
/// `BytesMut` that shares the same underlying allocation as `frame`. Callers
/// call `.freeze()` on it to get a `Bytes` that aliases the frame buffer.
fn split_frame_body(frame: &mut BytesMut) -> Result<(BytesMut, Option<Bytes>), io::Error> {
    if frame.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame too small for body_len suffix",
        ));
    }
    // Read body_len from the trailing 4 bytes.
    let suffix_start = frame.len() - 4;
    let body_len = {
        let mut le = [0u8; 4];
        le.copy_from_slice(&frame[suffix_start..]);
        u32::from_le_bytes(le) as usize
    };
    // Drop the 4-byte suffix by truncating.
    frame.truncate(suffix_start);

    if body_len > frame.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "body_len {} exceeds remaining frame length {}",
                body_len,
                frame.len()
            ),
        ));
    }

    let body = if body_len > 0 {
        // split_off returns a new BytesMut from body_start..end, sharing
        // the same allocation — this is the zero-copy slice.
        let body_start = frame.len() - body_len;
        Some(frame.split_off(body_start).freeze())
    } else {
        None
    };

    Ok((frame.split_to(frame.len()), body))
}

/// Encode an envelope + optional body into a length-delimited frame.
///
/// Writes: `[ envelope ][ body ][ body_len: u32 LE ]` into a single frame.
fn encode_frame(
    inner: &mut LengthDelimitedCodec,
    envelope: Bytes,
    body: Option<Bytes>,
    dst: &mut BytesMut,
) -> Result<(), io::Error> {
    let body_len = body.as_ref().map(|b| b.len()).unwrap_or(0) as u32;
    let total = envelope.len() + body_len as usize + 4;
    let mut payload = BytesMut::with_capacity(total);
    payload.extend_from_slice(&envelope);
    if let Some(b) = body {
        // Follow-up: vectored-write Sink for zero-copy encode to eliminate this memcpy.
        payload.extend_from_slice(&b);
    }
    payload.extend_from_slice(&body_len.to_le_bytes());
    inner
        .encode(payload.freeze(), dst)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
}

// ---------------------------------------------------------------------------
// ZeroCopyTransport — client-side (wraps ClientZeroCopyCodec)
// ---------------------------------------------------------------------------

/// Client-side zero-copy transport.
///
/// Implements `Stream<Item = io::Result<(Response<Resp>, Option<Bytes>)>>` and
/// `Sink<(ClientMessage<Req>, Option<Bytes>)>`.
#[pin_project]
pub struct ZeroCopyTransport<Req, Resp> {
    #[pin]
    inner: Framed<TcpStream, ClientZeroCopyCodec<Req, Resp>>,
}

impl<Req, Resp> Stream for ZeroCopyTransport<Req, Resp>
where
    Req: fory::Serializer + fory::ForyDefault + Clone + Send + 'static,
    Resp: fory::Serializer + fory::ForyDefault + Send + 'static,
{
    type Item = io::Result<(Response<Resp>, Option<Bytes>)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project()
            .inner
            .poll_next(cx)
            .map_err(io::Error::other)
    }
}

impl<Req, Resp> Sink<(ClientMessage<Req>, Option<Bytes>)> for ZeroCopyTransport<Req, Resp>
where
    Req: fory::Serializer + fory::ForyDefault + Clone + Send + 'static,
    Resp: fory::Serializer + fory::ForyDefault + Send + 'static,
{
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().inner.poll_ready(cx).map_err(io::Error::other)
    }

    fn start_send(
        self: Pin<&mut Self>,
        item: (ClientMessage<Req>, Option<Bytes>),
    ) -> io::Result<()> {
        self.project().inner.start_send(item).map_err(io::Error::other)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().inner.poll_flush(cx).map_err(io::Error::other)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().inner.poll_close(cx).map_err(io::Error::other)
    }
}

// ---------------------------------------------------------------------------
// ZeroCopyServerTransport — server-side (wraps ServerZeroCopyCodec)
// ---------------------------------------------------------------------------

/// Server-side zero-copy transport (one accepted connection).
///
/// Implements `Stream<Item = io::Result<(ClientMessage<Req>, Option<Bytes>)>>` and
/// `Sink<(Response<Resp>, Option<Bytes>)>`.
#[pin_project]
pub struct ZeroCopyServerTransport<Req, Resp> {
    #[pin]
    inner: Framed<TcpStream, ServerZeroCopyCodec<Req, Resp>>,
}

impl<Req, Resp> Stream for ZeroCopyServerTransport<Req, Resp>
where
    Req: fory::Serializer + fory::ForyDefault + Send + 'static,
    Resp: fory::Serializer + fory::ForyDefault + Clone + Send + 'static,
{
    type Item = io::Result<(ClientMessage<Req>, Option<Bytes>)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project()
            .inner
            .poll_next(cx)
            .map_err(io::Error::other)
    }
}

impl<Req, Resp> Sink<(Response<Resp>, Option<Bytes>)> for ZeroCopyServerTransport<Req, Resp>
where
    Req: fory::Serializer + fory::ForyDefault + Send + 'static,
    Resp: fory::Serializer + fory::ForyDefault + Clone + Send + 'static,
{
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().inner.poll_ready(cx).map_err(io::Error::other)
    }

    fn start_send(
        self: Pin<&mut Self>,
        item: (Response<Resp>, Option<Bytes>),
    ) -> io::Result<()> {
        self.project().inner.start_send(item).map_err(io::Error::other)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().inner.poll_flush(cx).map_err(io::Error::other)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().inner.poll_close(cx).map_err(io::Error::other)
    }
}

// ---------------------------------------------------------------------------
// ZeroCopyIncoming — server listener
// ---------------------------------------------------------------------------

/// Stream of server-side zero-copy transports, one per accepted TCP connection.
#[pin_project]
pub struct ZeroCopyIncoming<Req, Resp> {
    #[pin]
    listener: TcpListener,
    fory: Arc<Fory>,
    local_addr: SocketAddr,
    _marker: PhantomData<(Req, Resp)>,
}

impl<Req, Resp> ZeroCopyIncoming<Req, Resp> {
    /// Returns the local address this listener is bound to.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

impl<Req, Resp> Stream for ZeroCopyIncoming<Req, Resp>
where
    Req: fory::Serializer + fory::ForyDefault + Send + 'static,
    Resp: fory::Serializer + fory::ForyDefault + Clone + Send + 'static,
{
    type Item = io::Result<ZeroCopyServerTransport<Req, Resp>>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let accept_result = ready!(self.as_mut().project().listener.poll_accept(cx));
        let (stream, _peer) = match accept_result {
            Ok(pair) => pair,
            Err(e) => return Poll::Ready(Some(Err(e))),
        };
        let codec = ServerZeroCopyCodec::new(self.fory.clone());
        let framed = Framed::new(stream, codec);
        Poll::Ready(Some(Ok(ZeroCopyServerTransport { inner: framed })))
    }
}

// ---------------------------------------------------------------------------
// Public API: connect_zerocopy / listen_zerocopy
// ---------------------------------------------------------------------------

/// Connect to `addr` and return a client-side zero-copy transport.
///
/// The `fory` instance must have all required envelope types and the
/// `Req`/`Resp` types registered before calling this function. Use
/// [`super::fory_envelope::register_envelope_types`] for the envelope types.
pub async fn connect_zerocopy<Req, Resp, A>(
    addr: A,
    fory: Arc<Fory>,
) -> io::Result<ZeroCopyTransport<Req, Resp>>
where
    A: ToSocketAddrs,
    Req: fory::Serializer + fory::ForyDefault + Clone + Send + 'static,
    Resp: fory::Serializer + fory::ForyDefault + Send + 'static,
{
    let stream = TcpStream::connect(addr).await?;
    let codec = ClientZeroCopyCodec::new(fory);
    let framed = Framed::new(stream, codec);
    Ok(ZeroCopyTransport { inner: framed })
}

/// Listen on `addr` and return a [`ZeroCopyIncoming`] stream of server-side
/// zero-copy transports.
///
/// The `fory` instance must have all required envelope types and the
/// `Req`/`Resp` types registered before calling this function.
pub async fn listen_zerocopy<Req, Resp, A>(
    addr: A,
    fory: Arc<Fory>,
) -> io::Result<ZeroCopyIncoming<Req, Resp>>
where
    A: ToSocketAddrs,
    Req: fory::Serializer + fory::ForyDefault + Send + 'static,
    Resp: fory::Serializer + fory::ForyDefault + Clone + Send + 'static,
{
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    Ok(ZeroCopyIncoming { listener, fory, local_addr, _marker: PhantomData })
}

// ---------------------------------------------------------------------------
// TLS variant — requires `serde-transport-fory-tls` + `tcp` features
// ---------------------------------------------------------------------------

#[cfg(feature = "serde-transport-fory-tls")]
pub use zerocopy_tls::{
    ZeroCopyTlsIncoming, ZeroCopyTlsTransport, ZeroCopyTlsServerTransport,
    connect_zerocopy_tls, listen_zerocopy_tls,
};

/// TLS-enabled zero-copy transports.
///
/// # Zero-copy and TLS
///
/// On the **receive path**, TLS breaks the body-aliasing property that holds
/// for the plain-TCP variant. rustls decrypts each TLS record into its own
/// internal buffer and then copies the plaintext into the application-supplied
/// buffer. As a result the `Bytes` returned for the body region is allocated
/// from rustls's internal plaintext buffer rather than aliasing the original
/// socket read buffer. **No allocation-level zero-copy is possible through the
/// TLS layer.**
///
/// Functional correctness (body length and content) is preserved. If
/// zero-copy aliasing is critical for a hot path, consider terminating TLS
/// at a sidecar or load-balancer and using the plain-TCP zero-copy transport
/// on the internal leg.
///
/// The **send path** behaviour is unchanged: two memcpys occur (same as the
/// plain-TCP variant).
#[cfg(feature = "serde-transport-fory-tls")]
mod zerocopy_tls {
    use super::{ClientZeroCopyCodec, ServerZeroCopyCodec};
    use crate::{ClientMessage, Response};
    use fory::Fory;
    use futures::{Sink, Stream};
    use pin_project::pin_project;
    use std::{
        io,
        marker::PhantomData,
        net::SocketAddr,
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
    };
    use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
    use tokio::sync::{mpsc, watch};
    use tokio_rustls::rustls::{ClientConfig, ServerConfig, pki_types::ServerName};
    use tokio_rustls::{TlsAcceptor, TlsConnector};
    use tokio_rustls::client::TlsStream as ClientTlsStream;
    use tokio_rustls::server::TlsStream as ServerTlsStream;
    use tokio_util::bytes::Bytes;
    use tokio_util::codec::Framed;

    // -----------------------------------------------------------------------
    // ZeroCopyTlsTransport — client-side
    // -----------------------------------------------------------------------

    /// Client-side TLS zero-copy transport.
    ///
    /// Implements `Stream<Item = io::Result<(Response<Resp>, Option<Bytes>)>>`
    /// and `Sink<(ClientMessage<Req>, Option<Bytes>)>`.
    ///
    /// See the [module-level docs](super::zerocopy_tls) for the note on
    /// zero-copy behaviour through the TLS layer.
    #[pin_project]
    pub struct ZeroCopyTlsTransport<Req, Resp> {
        #[pin]
        inner: Framed<ClientTlsStream<TcpStream>, ClientZeroCopyCodec<Req, Resp>>,
    }

    impl<Req, Resp> Stream for ZeroCopyTlsTransport<Req, Resp>
    where
        Req: fory::Serializer + fory::ForyDefault + Clone + Send + 'static,
        Resp: fory::Serializer + fory::ForyDefault + Send + 'static,
    {
        type Item = io::Result<(Response<Resp>, Option<Bytes>)>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.project().inner.poll_next(cx).map_err(io::Error::other)
        }
    }

    impl<Req, Resp> Sink<(ClientMessage<Req>, Option<Bytes>)> for ZeroCopyTlsTransport<Req, Resp>
    where
        Req: fory::Serializer + fory::ForyDefault + Clone + Send + 'static,
        Resp: fory::Serializer + fory::ForyDefault + Send + 'static,
    {
        type Error = io::Error;

        fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.project().inner.poll_ready(cx).map_err(io::Error::other)
        }

        fn start_send(
            self: Pin<&mut Self>,
            item: (ClientMessage<Req>, Option<Bytes>),
        ) -> io::Result<()> {
            self.project().inner.start_send(item).map_err(io::Error::other)
        }

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.project().inner.poll_flush(cx).map_err(io::Error::other)
        }

        fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.project().inner.poll_close(cx).map_err(io::Error::other)
        }
    }

    // -----------------------------------------------------------------------
    // ZeroCopyTlsServerTransport — server-side (one accepted connection)
    // -----------------------------------------------------------------------

    /// Server-side TLS zero-copy transport (one accepted connection).
    ///
    /// Implements `Stream<Item = io::Result<(ClientMessage<Req>, Option<Bytes>)>>`
    /// and `Sink<(Response<Resp>, Option<Bytes>)>`.
    #[pin_project]
    pub struct ZeroCopyTlsServerTransport<Req, Resp> {
        #[pin]
        inner: Framed<ServerTlsStream<TcpStream>, ServerZeroCopyCodec<Req, Resp>>,
    }

    impl<Req, Resp> Stream for ZeroCopyTlsServerTransport<Req, Resp>
    where
        Req: fory::Serializer + fory::ForyDefault + Send + 'static,
        Resp: fory::Serializer + fory::ForyDefault + Clone + Send + 'static,
    {
        type Item = io::Result<(ClientMessage<Req>, Option<Bytes>)>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.project().inner.poll_next(cx).map_err(io::Error::other)
        }
    }

    impl<Req, Resp> Sink<(Response<Resp>, Option<Bytes>)> for ZeroCopyTlsServerTransport<Req, Resp>
    where
        Req: fory::Serializer + fory::ForyDefault + Send + 'static,
        Resp: fory::Serializer + fory::ForyDefault + Clone + Send + 'static,
    {
        type Error = io::Error;

        fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.project().inner.poll_ready(cx).map_err(io::Error::other)
        }

        fn start_send(
            self: Pin<&mut Self>,
            item: (Response<Resp>, Option<Bytes>),
        ) -> io::Result<()> {
            self.project().inner.start_send(item).map_err(io::Error::other)
        }

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.project().inner.poll_flush(cx).map_err(io::Error::other)
        }

        fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.project().inner.poll_close(cx).map_err(io::Error::other)
        }
    }

    // -----------------------------------------------------------------------
    // ZeroCopyTlsIncoming — mpsc-backed server listener
    // -----------------------------------------------------------------------

    /// Stream of TLS-wrapped server-side zero-copy transports.
    ///
    /// Each item is a fully-handshaked [`ZeroCopyTlsServerTransport`].
    ///
    /// The mpsc-backed design (same as [`crate::serde_transport::fory::tls`])
    /// keeps `poll_next` synchronous while TLS handshakes happen concurrently
    /// in spawned tasks: one per accepted connection, so a slow handshake
    /// cannot stall subsequent accepts.
    pub struct ZeroCopyTlsIncoming<Req, Resp> {
        rx: mpsc::Receiver<io::Result<ZeroCopyTlsServerTransport<Req, Resp>>>,
        local_addr: SocketAddr,
        _marker: PhantomData<(Req, Resp)>,
    }

    impl<Req, Resp> ZeroCopyTlsIncoming<Req, Resp> {
        /// Returns the local address this listener is bound to.
        pub fn local_addr(&self) -> SocketAddr {
            self.local_addr
        }
    }

    impl<Req, Resp> Stream for ZeroCopyTlsIncoming<Req, Resp>
    where
        Req: fory::Serializer + fory::ForyDefault + Send + Unpin + 'static,
        Resp: fory::Serializer + fory::ForyDefault + Clone + Send + Unpin + 'static,
    {
        type Item = io::Result<ZeroCopyTlsServerTransport<Req, Resp>>;

        fn poll_next(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            self.rx.poll_recv(cx)
        }
    }

    // -----------------------------------------------------------------------
    // accept_loop — background task: listen → handshake → push to mpsc
    // -----------------------------------------------------------------------

    async fn accept_loop<Req, Resp>(
        listener: TcpListener,
        fory: Arc<Fory>,
        tls_config: watch::Receiver<Arc<ServerConfig>>,
        tx: mpsc::Sender<io::Result<ZeroCopyTlsServerTransport<Req, Resp>>>,
    ) where
        Req: fory::Serializer + fory::ForyDefault + Send + 'static,
        Resp: fory::Serializer + fory::ForyDefault + Clone + Send + 'static,
    {
        loop {
            let tcp_result = listener.accept().await;
            let (stream, _peer) = match tcp_result {
                Ok(pair) => pair,
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            };

            // Snapshot ServerConfig at handshake time — cert-rotation hook.
            // Each new TLS handshake picks up whatever ServerConfig is current
            // in the watch channel without affecting established sessions.
            let acceptor = TlsAcceptor::from(tls_config.borrow().clone());
            let fory_clone = fory.clone();
            let tx_clone = tx.clone();

            // Spawn each handshake concurrently so a slow client cannot stall
            // subsequent accepts.
            tokio::spawn(async move {
                let result = async {
                    let tls_stream = acceptor.accept(stream).await?;
                    // ServerZeroCopyCodec::new sets max_frame_length = MAX_FRAME_LEN
                    // internally; that constant is defined at the parent module level.
                    let codec = ServerZeroCopyCodec::<Req, Resp>::new(fory_clone);
                    let framed = Framed::new(tls_stream, codec);
                    io::Result::Ok(ZeroCopyTlsServerTransport { inner: framed })
                }
                .await;
                let _ = tx_clone.send(result).await;
            });
        }
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Connect to `addr` and return a client-side TLS zero-copy transport.
    ///
    /// Snapshots the current [`ClientConfig`] from `tls_config` at connect
    /// time. Callers rotate client credentials by updating the watch channel
    /// sender — each new `connect_zerocopy_tls` call picks up the latest config.
    ///
    /// The `fory` instance must have all required envelope types and the
    /// `Req`/`Resp` types registered. Use
    /// [`super::super::fory_envelope::register_envelope_types`] for the
    /// envelope types.
    pub async fn connect_zerocopy_tls<Req, Resp, A>(
        addr: A,
        fory: Arc<Fory>,
        tls_config: watch::Receiver<Arc<ClientConfig>>,
        server_name: ServerName<'static>,
    ) -> io::Result<ZeroCopyTlsTransport<Req, Resp>>
    where
        A: ToSocketAddrs,
        Req: fory::Serializer + fory::ForyDefault + Clone + Send + 'static,
        Resp: fory::Serializer + fory::ForyDefault + Send + 'static,
    {
        let stream = TcpStream::connect(addr).await?;
        let connector = TlsConnector::from(tls_config.borrow().clone());
        let tls_stream = connector.connect(server_name, stream).await?;
        let codec = ClientZeroCopyCodec::new(fory);
        let framed = Framed::new(tls_stream, codec);
        Ok(ZeroCopyTlsTransport { inner: framed })
    }

    /// Listen on `addr` and return a [`ZeroCopyTlsIncoming`] stream of
    /// server-side TLS zero-copy transports.
    ///
    /// The `watch::Receiver<Arc<ServerConfig>>` pattern supports cert rotation:
    /// update the sender and all subsequent TLS handshakes use the new cert
    /// without dropping existing connections.
    ///
    /// The `fory` instance must have all required envelope types and the
    /// `Req`/`Resp` types registered.
    pub async fn listen_zerocopy_tls<Req, Resp, A>(
        addr: A,
        fory: Arc<Fory>,
        tls_config: watch::Receiver<Arc<ServerConfig>>,
    ) -> io::Result<ZeroCopyTlsIncoming<Req, Resp>>
    where
        A: ToSocketAddrs,
        Req: fory::Serializer + fory::ForyDefault + Send + 'static,
        Resp: fory::Serializer + fory::ForyDefault + Clone + Send + 'static,
    {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;

        // Design note: `Stream::poll_next` is synchronous but TLS handshakes are
        // async. We spawn a background accept_loop (same pattern as the classic
        // fory TLS transport) that pushes fully-handshaked transports into an
        // mpsc channel. ZeroCopyTlsIncoming::poll_next simply drains the receiver.
        let (tx, rx) = mpsc::channel(64);

        tokio::spawn(accept_loop::<Req, Resp>(listener, fory, tls_config, tx));

        Ok(ZeroCopyTlsIncoming { rx, local_addr, _marker: PhantomData })
    }
}
