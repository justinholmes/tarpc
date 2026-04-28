//! Integration tests for fory-over-TLS transport.
//!
//! Exercises [`tarpc::serde_transport::fory::connect_tls`] /
//! [`listen_tls`] with self-signed certificates and validates that cert
//! rotation via `watch::Sender` does not drop existing connections.

#![cfg(all(feature = "serde-transport-fory-tls", feature = "tcp"))]

use futures::StreamExt as _;
use std::sync::Arc;
use tarpc::{
    client, context,
    server::{self, Channel},
    serde_transport::fory as fory_transport,
};
use tokio::sync::watch;
use tokio_rustls::rustls::{
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName},
    ClientConfig, RootCertStore, ServerConfig,
};

#[tarpc::service(derive = [Clone, serde::Serialize, serde::Deserialize])]
trait Hello {
    async fn hello(name: String) -> String;
}

#[derive(Clone)]
struct HelloServer;

impl Hello for HelloServer {
    async fn hello(self, _: context::Context, name: String) -> String {
        format!("hello, {}", name)
    }
}

/// Generate a self-signed cert + key for "localhost" using rcgen 0.13.
fn make_self_signed() -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    // cert.der() returns &CertificateDer<'static>; clone to take ownership.
    let cert_der = certified.cert.der().clone();
    // key_pair.serialize_der() returns Vec<u8> in PKCS#8 format.
    let key_der =
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(certified.key_pair.serialize_der()));
    (cert_der, key_der)
}

fn make_server_config(cert: CertificateDer<'static>, key: PrivateKeyDer<'static>) -> Arc<ServerConfig> {
    Arc::new(
        ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)
            .unwrap(),
    )
}

fn make_client_config(trusted_cert: &CertificateDer<'static>) -> Arc<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(trusted_cert.clone()).unwrap();
    Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    )
}

// ---------------------------------------------------------------------------
// Round-trip test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hello_service_over_fory_tls() {
    let (cert, key) = make_self_signed();

    let (server_tx, server_rx) = watch::channel(make_server_config(cert.clone(), key));
    let _hold_server_tx = server_tx;

    let (client_tx, client_rx) = watch::channel(make_client_config(&cert));
    let _hold_client_tx = client_tx;

    let mut listener = fory_transport::listen_tls::<HelloService, _>("127.0.0.1:0", server_rx)
        .await
        .unwrap();
    let addr = listener.local_addr();

    tokio::spawn(async move {
        while let Some(Ok(transport)) = listener.next().await {
            let channel = server::BaseChannel::with_defaults(transport);
            tokio::spawn(channel.execute(HelloServer.serve()).for_each(|fut| async move {
                tokio::spawn(fut);
            }));
        }
    });

    let server_name = ServerName::try_from("localhost").unwrap();
    let transport =
        fory_transport::connect_tls::<HelloService, _>(addr, client_rx, server_name)
            .await
            .unwrap();
    let client = HelloClient::new(client::Config::default(), transport).spawn();

    let resp = client
        .hello(context::current(), "world".to_string())
        .await
        .unwrap();
    assert_eq!(resp, "hello, world");
}

// ---------------------------------------------------------------------------
// Cert rotation test
// ---------------------------------------------------------------------------

/// Cert rotation: while a long-lived connection is active, swap the server
/// cert via the watch channel.  The existing connection must NOT drop (TLS
/// sessions are independent of the ServerConfig snapshot used at handshake
/// time). New connections must use the fresh cert.
#[tokio::test]
async fn cert_rotation_no_drop() {
    let (cert1, key1) = make_self_signed();
    let (cert2, key2) = make_self_signed();

    let (server_tx, server_rx) = watch::channel(make_server_config(cert1.clone(), key1));

    let (client_tx, client_rx) = watch::channel(make_client_config(&cert1));

    let mut listener = fory_transport::listen_tls::<HelloService, _>("127.0.0.1:0", server_rx)
        .await
        .unwrap();
    let addr = listener.local_addr();

    tokio::spawn(async move {
        while let Some(Ok(transport)) = listener.next().await {
            let channel = server::BaseChannel::with_defaults(transport);
            tokio::spawn(channel.execute(HelloServer.serve()).for_each(|fut| async move {
                tokio::spawn(fut);
            }));
        }
    });

    // First connection established under cert1.
    let server_name = ServerName::try_from("localhost").unwrap();
    let transport =
        fory_transport::connect_tls::<HelloService, _>(addr, client_rx.clone(), server_name.clone())
            .await
            .unwrap();
    let client = HelloClient::new(client::Config::default(), transport).spawn();

    let r1 = client
        .hello(context::current(), "before".to_string())
        .await
        .unwrap();
    assert_eq!(r1, "hello, before");

    // Rotate server cert to cert2.  Existing TLS sessions are unaffected; only
    // new handshakes will use the new ServerConfig.
    server_tx.send(make_server_config(cert2.clone(), key2)).unwrap();

    // Existing connection still alive after rotation.
    let r2 = client
        .hello(context::current(), "during".to_string())
        .await
        .unwrap();
    assert_eq!(r2, "hello, during");

    // Update the client's trust store to include cert2 so a new connection can
    // complete the handshake.
    client_tx.send(make_client_config(&cert2)).unwrap();

    // New connection with rotated cert2 on both sides.
    let transport2 =
        fory_transport::connect_tls::<HelloService, _>(addr, client_rx, server_name)
            .await
            .unwrap();
    let client2 = HelloClient::new(client::Config::default(), transport2).spawn();

    let r3 = client2
        .hello(context::current(), "after".to_string())
        .await
        .unwrap();
    assert_eq!(r3, "hello, after");
}
