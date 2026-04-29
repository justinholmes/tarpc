//! TCP transport using Apache Fory codec.
//!
//! Mirrors [`crate::serde_transport::tcp`] but serializes tarpc envelope types
//! via [`fory`] instead of JSON/bincode. Requires the `serde-transport-fory`
//! and `tcp` features.
//!
//! # Quick start
//!
//! ```no_run
//! # #[cfg(all(feature = "serde-transport-fory", feature = "tcp"))]
//! # async fn example() -> std::io::Result<()> {
//! use fory::Fory;
//! use std::sync::Arc;
//! use tarpc::serde_transport::fory as fory_transport;
//!
//! let fory = Arc::new(Fory::default());
//!
//! // Server side
//! let incoming = fory_transport::listen_with_fory::<_, String, String>("127.0.0.1:0", fory.clone()).await?;
//! let addr = incoming.local_addr();
//!
//! // Client side
//! let transport = fory_transport::connect_with_fory::<_, String, String>(addr, fory).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Type registration
//!
//! The fory codec requires all types that cross the wire to be registered in the
//! same [`fory::Fory`] instance on both sides with identical numeric IDs. At a
//! minimum register the envelope wrapper types from
//! [`crate::serde_transport::fory_envelope`] **plus** your own `Req`/`Resp` types
//! (unless they are built-in primitive types that fory handles natively).

use super::fory_envelope::{ForyClientMessage, ForyResponse};
use crate::{ClientMessage, Response};
use fory::Fory;
use std::{io, marker::PhantomData, pin::Pin, sync::Arc};
use tokio_util::bytes::{Bytes, BytesMut};

// The parent module (serde_transport) exposes `new` and `Transport`.
use crate::serde_transport::{self, Transport};

/// Maximum frame size for fory-encoded messages (64 MiB).
/// Matches cloudverve fabric's existing cap (`fabric/transport.rs:143`).
const MAX_FRAME_LEN: usize = 64 * 1024 * 1024;

// ---------------------------------------------------------------------------
// ForyEnvelopeCodec
// ---------------------------------------------------------------------------

/// Codec that converts tarpc's native envelope types ↔ fory wrapper types at
/// the wire boundary.
///
/// `Req` is the user-defined request message type; `Resp` is the response
/// message type. The codec works symmetrically on both client and server sides:
///
/// - **Client side**: `Serializer<ClientMessage<Req>>` + `Deserializer<Response<Resp>>`
/// - **Server side**: `Serializer<Response<Resp>>` + `Deserializer<ClientMessage<Req>>`
pub struct ForyEnvelopeCodec<Req, Resp> {
    fory: Arc<Fory>,
    _marker: PhantomData<(Req, Resp)>,
}

impl<Req, Resp> ForyEnvelopeCodec<Req, Resp> {
    /// Create a new codec sharing the given [`Fory`] registry.
    pub fn new(fory: Arc<Fory>) -> Self {
        Self { fory, _marker: PhantomData }
    }
}

impl<Req, Resp> Clone for ForyEnvelopeCodec<Req, Resp> {
    fn clone(&self) -> Self {
        Self { fory: self.fory.clone(), _marker: PhantomData }
    }
}

// ---------------------------------------------------------------------------
// Client side: send ClientMessage<Req>
// ---------------------------------------------------------------------------

impl<Req, Resp> tokio_serde::Serializer<ClientMessage<Req>> for ForyEnvelopeCodec<Req, Resp>
where
    Req: fory::Serializer + fory::ForyDefault + Clone + Send + 'static,
{
    type Error = io::Error;

    fn serialize(
        self: Pin<&mut Self>,
        item: &ClientMessage<Req>,
    ) -> Result<Bytes, Self::Error> {
        let wrapper = ForyClientMessage::<Req>::from(item);
        self.fory
            .serialize(&wrapper)
            .map(Bytes::from)
            .map_err(|e: fory::Error| {
                io::Error::new(io::ErrorKind::InvalidData, e.to_string())
            })
    }
}

// ---------------------------------------------------------------------------
// Client side: receive Response<Resp>
// ---------------------------------------------------------------------------

impl<Req, Resp> tokio_serde::Deserializer<Response<Resp>> for ForyEnvelopeCodec<Req, Resp>
where
    Resp: fory::Serializer + fory::ForyDefault + Send + 'static,
{
    type Error = io::Error;

    fn deserialize(
        self: Pin<&mut Self>,
        src: &BytesMut,
    ) -> Result<Response<Resp>, Self::Error> {
        let wrapper: ForyResponse<Resp> = self
            .fory
            .deserialize(src.as_ref())
            .map_err(|e: fory::Error| {
                io::Error::new(io::ErrorKind::InvalidData, e.to_string())
            })?;
        Ok(Response::from(wrapper))
    }
}

// ---------------------------------------------------------------------------
// Server side: send Response<Resp>
// ---------------------------------------------------------------------------

impl<Req, Resp> tokio_serde::Serializer<Response<Resp>> for ForyEnvelopeCodec<Req, Resp>
where
    Resp: fory::Serializer + fory::ForyDefault + Clone + Send + 'static,
{
    type Error = io::Error;

    fn serialize(
        self: Pin<&mut Self>,
        item: &Response<Resp>,
    ) -> Result<Bytes, Self::Error> {
        let wrapper = ForyResponse::<Resp>::from(item);
        self.fory
            .serialize(&wrapper)
            .map(Bytes::from)
            .map_err(|e: fory::Error| {
                io::Error::new(io::ErrorKind::InvalidData, e.to_string())
            })
    }
}

// ---------------------------------------------------------------------------
// Server side: receive ClientMessage<Req>
// ---------------------------------------------------------------------------

impl<Req, Resp> tokio_serde::Deserializer<ClientMessage<Req>> for ForyEnvelopeCodec<Req, Resp>
where
    Req: fory::Serializer + fory::ForyDefault + Send + 'static,
{
    type Error = io::Error;

    fn deserialize(
        self: Pin<&mut Self>,
        src: &BytesMut,
    ) -> Result<ClientMessage<Req>, Self::Error> {
        let wrapper: ForyClientMessage<Req> = self
            .fory
            .deserialize(src.as_ref())
            .map_err(|e: fory::Error| {
                io::Error::new(io::ErrorKind::InvalidData, e.to_string())
            })?;
        Ok(ClientMessage::from(wrapper))
    }
}

// ---------------------------------------------------------------------------
// TCP support (requires `tcp` feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "tcp")]
pub use tcp::{Incoming, connect, connect_with_fory, listen, listen_with_fory};

#[cfg(all(feature = "tcp", feature = "serde-transport-fory-tls"))]
pub use tls::{TlsIncoming, connect_tls, connect_tls_with_fory, listen_tls, listen_tls_with_fory};

// ---------------------------------------------------------------------------
// TLS support (requires `serde-transport-fory-tls` + `tcp` features)
// ---------------------------------------------------------------------------

#[cfg(all(feature = "tcp", feature = "serde-transport-fory-tls"))]
mod tls {
    use super::*;
    use super::super::fory_envelope::ServiceWireSchema;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
    use tokio::sync::{mpsc, watch};
    use tokio_rustls::rustls::{ClientConfig, ServerConfig, pki_types::ServerName};
    use tokio_rustls::{TlsAcceptor, TlsConnector};
    use tokio_rustls::client::TlsStream as ClientTlsStream;
    use tokio_rustls::server::TlsStream as ServerTlsStream;
    use tokio_util::codec::length_delimited;

    use super::MAX_FRAME_LEN;

    // -----------------------------------------------------------------------
    // connect_tls (schema-driven)
    // -----------------------------------------------------------------------

    /// Schema-driven TLS connect. Mirrors [`super::tcp::connect`] but wraps the
    /// stream in a TLS layer using the supplied [`ClientConfig`] watch channel.
    ///
    /// The `watch::Receiver<Arc<ClientConfig>>` pattern lets callers rotate
    /// client certificates without reconnect storms: update the sender and all
    /// subsequent new connections will pick up the fresh config.
    pub async fn connect_tls<S, A>(
        addr: A,
        tls_config: watch::Receiver<Arc<ClientConfig>>,
        server_name: ServerName<'static>,
    ) -> io::Result<
        Transport<
            ClientTlsStream<TcpStream>,
            Response<S::Resp>,
            ClientMessage<S::Req>,
            ForyEnvelopeCodec<S::Req, S::Resp>,
        >,
    >
    where
        S: ServiceWireSchema,
        A: ToSocketAddrs,
        S::Req: Clone,
        ClientMessage<S::Req>: serde::Serialize,
        Response<S::Resp>: for<'de> serde::Deserialize<'de>,
    {
        let mut fory_inst = fory::Fory::default();
        S::register(&mut fory_inst)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        connect_tls_with_fory::<A, S::Req, S::Resp>(
            addr,
            Arc::new(fory_inst),
            tls_config,
            server_name,
        )
        .await
    }

    // -----------------------------------------------------------------------
    // connect_tls_with_fory (manual Fory)
    // -----------------------------------------------------------------------

    /// Manual-Fory TLS connect. The caller supplies a pre-configured [`Fory`]
    /// instance with all necessary types registered.
    pub async fn connect_tls_with_fory<A, Req, Resp>(
        addr: A,
        fory: Arc<fory::Fory>,
        tls_config: watch::Receiver<Arc<ClientConfig>>,
        server_name: ServerName<'static>,
    ) -> io::Result<
        Transport<
            ClientTlsStream<TcpStream>,
            Response<Resp>,
            ClientMessage<Req>,
            ForyEnvelopeCodec<Req, Resp>,
        >,
    >
    where
        A: ToSocketAddrs,
        Req: fory::Serializer + fory::ForyDefault + Clone + Send + 'static,
        Resp: fory::Serializer + fory::ForyDefault + Send + 'static,
        ClientMessage<Req>: serde::Serialize,
        Response<Resp>: for<'de> serde::Deserialize<'de>,
    {
        let stream = TcpStream::connect(addr).await?;
        // Snapshot the current config; new connections always use the latest.
        let connector = TlsConnector::from(tls_config.borrow().clone());
        let tls_stream = connector.connect(server_name, stream).await?;
        let framed = length_delimited::Builder::new()
            .max_frame_length(MAX_FRAME_LEN)
            .new_framed(tls_stream);
        Ok(serde_transport::new(framed, ForyEnvelopeCodec::new(fory)))
    }

    // -----------------------------------------------------------------------
    // listen_tls (schema-driven)
    // -----------------------------------------------------------------------

    /// Schema-driven TLS listen. Mirrors [`super::tcp::listen`] but wraps each
    /// accepted connection in a TLS layer.
    ///
    /// The `watch::Receiver<Arc<ServerConfig>>` pattern supports cert rotation:
    /// update the sender and all subsequent TLS handshakes use the new cert
    /// without dropping existing connections.
    pub async fn listen_tls<S, A>(
        addr: A,
        tls_config: watch::Receiver<Arc<ServerConfig>>,
    ) -> io::Result<TlsIncoming<S::Req, S::Resp>>
    where
        S: ServiceWireSchema,
        A: ToSocketAddrs,
        S::Req: Clone + Unpin,
        S::Resp: Clone + Unpin,
        Response<S::Resp>: serde::Serialize,
        ClientMessage<S::Req>: for<'de> serde::Deserialize<'de>,
    {
        let mut fory_inst = fory::Fory::default();
        S::register(&mut fory_inst)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        listen_tls_with_fory::<A, S::Req, S::Resp>(addr, Arc::new(fory_inst), tls_config).await
    }

    // -----------------------------------------------------------------------
    // listen_tls_with_fory / TlsIncoming
    // -----------------------------------------------------------------------

    /// Manual-Fory TLS listen.
    pub async fn listen_tls_with_fory<A, Req, Resp>(
        addr: A,
        fory: Arc<fory::Fory>,
        tls_config: watch::Receiver<Arc<ServerConfig>>,
    ) -> io::Result<TlsIncoming<Req, Resp>>
    where
        A: ToSocketAddrs,
        Req: fory::Serializer + fory::ForyDefault + Clone + Send + Unpin + 'static,
        Resp: fory::Serializer + fory::ForyDefault + Clone + Send + Unpin + 'static,
        Response<Resp>: serde::Serialize,
        ClientMessage<Req>: for<'de> serde::Deserialize<'de>,
    {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;

        // Design note: `Stream::poll_next` is synchronous but TLS handshakes are
        // async. Rather than pin-projecting an in-flight handshake future onto the
        // struct (which works but requires unsafe or pin_project and makes the type
        // non-Send without care), we spawn a single background task that loops on
        // `accept` + TLS handshake and pushes fully-built transports into an mpsc
        // channel. `TlsIncoming::poll_next` simply polls the receiver — one task
        // per listener, zero extra allocation per connection. The background task
        // runs until the listener errors or is dropped, which closes the sender
        // and causes the receiver-side stream to terminate naturally.
        let (tx, rx) = mpsc::channel(64);

        tokio::spawn(accept_loop::<Req, Resp>(listener, fory, tls_config, tx));

        Ok(TlsIncoming { rx, local_addr, _marker: std::marker::PhantomData })
    }

    type IncomingTransport<Req, Resp> = Transport<
        ServerTlsStream<TcpStream>,
        ClientMessage<Req>,
        Response<Resp>,
        ForyEnvelopeCodec<Req, Resp>,
    >;

    async fn accept_loop<Req, Resp>(
        listener: TcpListener,
        fory: Arc<fory::Fory>,
        tls_config: watch::Receiver<Arc<ServerConfig>>,
        tx: mpsc::Sender<io::Result<IncomingTransport<Req, Resp>>>,
    ) where
        Req: fory::Serializer + fory::ForyDefault + Clone + Send + Unpin + 'static,
        Resp: fory::Serializer + fory::ForyDefault + Clone + Send + Unpin + 'static,
        Response<Resp>: serde::Serialize,
        ClientMessage<Req>: for<'de> serde::Deserialize<'de>,
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

            // Snapshot the current server config at handshake time — this is
            // the cert-rotation hook: each new TLS handshake picks up whatever
            // ServerConfig is current in the watch channel.
            let acceptor = TlsAcceptor::from(tls_config.borrow().clone());
            let fory_clone = fory.clone();
            let tx_clone = tx.clone();

            // Spawn each handshake concurrently so a slow client can't stall
            // subsequent accepts.
            tokio::spawn(async move {
                let result = async {
                    let tls_stream = acceptor.accept(stream).await?;
                    let framed = length_delimited::Builder::new()
                        .max_frame_length(MAX_FRAME_LEN)
                        .new_framed(tls_stream);
                    io::Result::Ok(serde_transport::new(
                        framed,
                        ForyEnvelopeCodec::<Req, Resp>::new(fory_clone),
                    ))
                }
                .await;
                let _ = tx_clone.send(result).await;
            });
        }
    }

    /// Stream of TLS-wrapped server-side fory transports.
    ///
    /// Each item is a fully-handshaked [`Transport`] ready for use with
    /// [`tarpc::server::BaseChannel`].
    pub struct TlsIncoming<Req, Resp> {
        rx: mpsc::Receiver<io::Result<IncomingTransport<Req, Resp>>>,
        local_addr: SocketAddr,
        _marker: std::marker::PhantomData<(Req, Resp)>,
    }

    impl<Req, Resp> TlsIncoming<Req, Resp> {
        /// Returns the local address this listener is bound to.
        pub fn local_addr(&self) -> SocketAddr {
            self.local_addr
        }
    }

    impl<Req, Resp> futures::stream::Stream for TlsIncoming<Req, Resp>
    where
        Req: fory::Serializer + fory::ForyDefault + Clone + Send + Unpin + 'static,
        Resp: fory::Serializer + fory::ForyDefault + Clone + Send + Unpin + 'static,
        Response<Resp>: serde::Serialize,
        ClientMessage<Req>: for<'de> serde::Deserialize<'de>,
    {
        type Item = io::Result<IncomingTransport<Req, Resp>>;

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            self.rx.poll_recv(cx)
        }
    }
}

#[cfg(feature = "tcp")]
mod tcp {
    use super::*;
    use super::super::fory_envelope::ServiceWireSchema;
    use futures::ready;
    use pin_project::pin_project;
    use std::net::SocketAddr;
    use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
    use tokio_util::codec::length_delimited;

    // -----------------------------------------------------------------------
    // connect (schema-driven — zero user-side registration)
    // -----------------------------------------------------------------------

    /// Connects to `addr` and returns a client-side fory transport.
    ///
    /// `S` is the generated marker struct (e.g. `HelloService`) that implements
    /// [`ServiceWireSchema`]. All required types are auto-registered from
    /// `S::register`; no manual `Fory` setup is needed.
    pub async fn connect<S, A>(
        addr: A,
    ) -> io::Result<
        Transport<TcpStream, Response<S::Resp>, ClientMessage<S::Req>, ForyEnvelopeCodec<S::Req, S::Resp>>,
    >
    where
        S: ServiceWireSchema,
        A: ToSocketAddrs,
        S::Req: Clone,
        // serde bounds required by Transport's Stream/Sink impls
        ClientMessage<S::Req>: serde::Serialize,
        Response<S::Resp>: for<'de> serde::Deserialize<'de>,
    {
        let mut fory_inst = fory::Fory::default();
        S::register(&mut fory_inst).map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let fory_arc = Arc::new(fory_inst);
        connect_with_fory::<A, S::Req, S::Resp>(addr, fory_arc).await
    }

    /// Listens on `addr` and returns an [`Incoming`] stream of server-side transports.
    ///
    /// `S` is the generated marker struct (e.g. `HelloService`) that implements
    /// [`ServiceWireSchema`]. All required types are auto-registered from
    /// `S::register`; no manual `Fory` setup is needed.
    pub async fn listen<S, A>(addr: A) -> io::Result<Incoming<S::Req, S::Resp>>
    where
        S: ServiceWireSchema,
        A: ToSocketAddrs,
    {
        let mut fory_inst = fory::Fory::default();
        S::register(&mut fory_inst).map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let fory_arc = Arc::new(fory_inst);
        listen_with_fory::<A, S::Req, S::Resp>(addr, fory_arc).await
    }

    // -----------------------------------------------------------------------
    // connect_with_fory (manual Fory registry — power users)
    // -----------------------------------------------------------------------

    /// Connects to `addr` and returns a client-side fory transport.
    ///
    /// The caller is responsible for registering all required types in `fory`
    /// before calling this function. Prefer [`connect`] with a `ServiceWireSchema`
    /// type parameter for ergonomic zero-boilerplate usage.
    pub async fn connect_with_fory<A, Req, Resp>(
        addr: A,
        fory: Arc<Fory>,
    ) -> io::Result<
        Transport<TcpStream, Response<Resp>, ClientMessage<Req>, ForyEnvelopeCodec<Req, Resp>>,
    >
    where
        A: ToSocketAddrs,
        Req: fory::Serializer + fory::ForyDefault + Clone + Send + 'static,
        Resp: fory::Serializer + fory::ForyDefault + Send + 'static,
        // serde bounds required by Transport's Stream/Sink impls
        ClientMessage<Req>: serde::Serialize,
        Response<Resp>: for<'de> serde::Deserialize<'de>,
    {
        let stream = TcpStream::connect(addr).await?;
        let framed = length_delimited::Builder::new()
            .max_frame_length(MAX_FRAME_LEN)
            .new_framed(stream);
        Ok(serde_transport::new(framed, ForyEnvelopeCodec::new(fory)))
    }

    // -----------------------------------------------------------------------
    // listen_with_fory / Incoming
    // -----------------------------------------------------------------------

    /// A [`TcpListener`] that wraps accepted connections in fory-encoded
    /// server-side transports.
    #[pin_project]
    pub struct Incoming<Req, Resp> {
        #[pin]
        listener: TcpListener,
        fory: Arc<Fory>,
        local_addr: SocketAddr,
        config: length_delimited::Builder,
        _marker: PhantomData<(Req, Resp)>,
    }

    impl<Req, Resp> Incoming<Req, Resp> {
        /// Returns the address being listened on.
        pub fn local_addr(&self) -> SocketAddr {
            self.local_addr
        }

        /// Returns an immutable reference to the length-delimited codec's config.
        pub fn config(&self) -> &length_delimited::Builder {
            &self.config
        }

        /// Returns a mutable reference to the length-delimited codec's config.
        pub fn config_mut(&mut self) -> &mut length_delimited::Builder {
            &mut self.config
        }
    }

    /// Listens on `addr` and returns an [`Incoming`] stream of server-side
    /// transports.
    ///
    /// Each accepted transport sinks `Response<Resp>` and streams
    /// `ClientMessage<Req>`.
    ///
    /// The caller is responsible for registering all required types in `fory`
    /// before calling this function. Prefer [`listen`] with a `ServiceWireSchema`
    /// type parameter for ergonomic zero-boilerplate usage.
    pub async fn listen_with_fory<A, Req, Resp>(addr: A, fory: Arc<Fory>) -> io::Result<Incoming<Req, Resp>>
    where
        A: ToSocketAddrs,
    {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        Ok(Incoming {
            listener,
            fory,
            local_addr,
            config: *length_delimited::Builder::new().max_frame_length(MAX_FRAME_LEN),
            _marker: PhantomData,
        })
    }

    impl<Req, Resp> futures::stream::Stream for Incoming<Req, Resp>
    where
        Req: fory::Serializer + fory::ForyDefault + Send + 'static,
        Resp: fory::Serializer + fory::ForyDefault + Clone + Send + 'static,
        // serde bounds required by Transport's Stream/Sink impls
        Response<Resp>: serde::Serialize,
        ClientMessage<Req>: for<'de> serde::Deserialize<'de>,
    {
        type Item = io::Result<
            Transport<
                TcpStream,
                ClientMessage<Req>,
                Response<Resp>,
                ForyEnvelopeCodec<Req, Resp>,
            >,
        >;

        fn poll_next(
            mut self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            let accept_result = ready!(self.as_mut().project().listener.poll_accept(cx));
            let (stream, _peer) = match accept_result {
                Ok(pair) => pair,
                Err(e) => return std::task::Poll::Ready(Some(Err(e))),
            };
            let framed = self.config.new_framed(stream);
            let transport = serde_transport::new(
                framed,
                ForyEnvelopeCodec::<Req, Resp>::new(self.fory.clone()),
            );
            std::task::Poll::Ready(Some(Ok(transport)))
        }
    }
}
