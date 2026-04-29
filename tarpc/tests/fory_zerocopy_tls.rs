//! Integration tests for the TLS variant of the zero-copy bulk-payload codec.
//!
//! Exercises [`tarpc::serde_transport::fory_zerocopy::connect_zerocopy_tls`] /
//! [`listen_zerocopy_tls`] with self-signed certificates and validates:
//!
//! 1. Round-trip of a 4 MiB body over TLS — functional correctness.
//! 2. Zero-copy aliasing behaviour: the test documents that body-aliasing does
//!    **not** hold through the TLS layer (rustls decrypts into its own buffer),
//!    and asserts the functionally correct values instead.
//! 3. Cert rotation: swap the server cert mid-flight via the watch channel.
//!    Existing connections must survive; new connections must use the fresh cert.

#![cfg(all(feature = "serde-transport-fory-tls", feature = "tcp"))]

use bytes::Bytes;
use fory::Fory;
use futures::{SinkExt as _, StreamExt as _};
use std::sync::Arc;
use tarpc::{
    ClientMessage, Request, Response,
    context,
    serde_transport::{
        fory_envelope::register_envelope_types,
        fory_zerocopy::{connect_zerocopy_tls, listen_zerocopy_tls},
    },
};
use tokio::sync::watch;
use tokio_rustls::rustls::{
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName},
    ClientConfig, RootCertStore, ServerConfig,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_fory() -> Arc<Fory> {
    let mut fory = Fory::default();
    register_envelope_types::<String>(&mut fory).unwrap();
    Arc::new(fory)
}

fn make_request(id: u64, msg: &str) -> ClientMessage<String> {
    ClientMessage::Request(Request {
        context: context::current(),
        id,
        message: msg.to_string(),
    })
}

/// Generate a self-signed cert + key for "localhost" using rcgen 0.13.
fn make_self_signed() -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_der = certified.cert.der().clone();
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
// Test 1: 4 MiB round-trip over TLS
//
// Sends a 4 MiB body from client to server, server echoes it back.
// Verifies functional correctness: length and sentinel bytes.
//
// Zero-copy aliasing note:
//   Unlike the plain-TCP variant (fory_zerocopy.rs::body_is_aliased), TLS
//   breaks the allocation-aliasing property. rustls decrypts each TLS record
//   into its own internal plaintext buffer and then copies the data into the
//   codec's read buffer. The Bytes returned for the body region is therefore
//   NOT a sub-slice of the original socket read buffer. This is a fundamental
//   constraint of the TLS decryption model, not a bug in the codec.
//
//   Consequence: callers using the TLS variant do not get zero-copy on the
//   receive side. If allocation-level zero-copy is critical, consider
//   terminating TLS at a sidecar and using the plain-TCP transport on the
//   internal leg.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zerocopy_4mib_round_trip_over_tls() {
    let (cert, key) = make_self_signed();
    let server_cfg = make_server_config(cert.clone(), key);
    let client_cfg = make_client_config(&cert);

    let (server_tx, server_rx) = watch::channel(server_cfg);
    let _hold_server_tx = server_tx;
    let (client_tx, client_rx) = watch::channel(client_cfg);
    let _hold_client_tx = client_tx;

    let fory_srv = make_fory();
    let fory_cli = make_fory();

    let mut incoming =
        listen_zerocopy_tls::<String, String, _>("127.0.0.1:0", fory_srv, server_rx)
            .await
            .unwrap();
    let addr = incoming.local_addr();

    let body_data = vec![0xABu8; 4 * 1024 * 1024];
    let body_send = Bytes::from(body_data.clone());

    // Server: receive one request, echo body back in response.
    tokio::spawn(async move {
        if let Some(Ok(mut srv)) = incoming.next().await {
            if let Some(Ok((ClientMessage::Request(req), body))) = srv.next().await {
                let body_len = body.as_ref().map(|b| b.len()).unwrap_or(0);
                let resp = Response {
                    request_id: req.id,
                    message: Ok(format!("body_len:{}", body_len)),
                };
                srv.send((resp, body)).await.unwrap();
            }
        }
    });

    let server_name = ServerName::try_from("localhost").unwrap();
    let mut client =
        connect_zerocopy_tls::<String, String, _>(addr, fory_cli, client_rx, server_name)
            .await
            .unwrap();

    let req = make_request(1, "tls-zerocopy-4mib");
    client.send((req, Some(body_send))).await.unwrap();

    let (resp, resp_body) = client.next().await.unwrap().unwrap();
    assert_eq!(resp.request_id, 1);
    assert_eq!(
        resp.message.unwrap(),
        format!("body_len:{}", 4 * 1024 * 1024),
    );

    let rb = resp_body.expect("expected body in response");
    assert_eq!(rb.len(), 4 * 1024 * 1024, "body length mismatch");
    assert_eq!(rb[0], 0xAB, "sentinel at [0] corrupted");
    assert_eq!(rb[rb.len() - 1], 0xAB, "sentinel at [last] corrupted");
    assert_eq!(&rb[..], &body_data[..], "full body content mismatch");

    // Confirm that body-aliasing does NOT hold through TLS decryption.
    // The body Bytes returned by the codec comes from rustls's plaintext
    // buffer, which is a separate allocation from the socket read buffer.
    // We cannot check the pointer directly (we have no access to the socket
    // buffer in this path), but we document the expectation here:
    //   rb.as_ptr() != <socket-read-buffer-ptr>  — always true after TLS
    //
    // Functional correctness (byte content) is verified above.
}

// ---------------------------------------------------------------------------
// Test 2: no-body round-trip over TLS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zerocopy_no_body_over_tls() {
    let (cert, key) = make_self_signed();
    let server_cfg = make_server_config(cert.clone(), key);
    let client_cfg = make_client_config(&cert);

    let (server_tx, server_rx) = watch::channel(server_cfg);
    let _hold_server_tx = server_tx;
    let (client_tx, client_rx) = watch::channel(client_cfg);
    let _hold_client_tx = client_tx;

    let fory_srv = make_fory();
    let fory_cli = make_fory();

    let mut incoming =
        listen_zerocopy_tls::<String, String, _>("127.0.0.1:0", fory_srv, server_rx)
            .await
            .unwrap();
    let addr = incoming.local_addr();

    tokio::spawn(async move {
        if let Some(Ok(mut srv)) = incoming.next().await {
            if let Some(Ok((ClientMessage::Request(req), body))) = srv.next().await {
                assert!(body.is_none(), "expected no body on server side");
                let resp = Response {
                    request_id: req.id,
                    message: Ok("no-body-ok".to_string()),
                };
                srv.send((resp, None)).await.unwrap();
            }
        }
    });

    let server_name = ServerName::try_from("localhost").unwrap();
    let mut client =
        connect_zerocopy_tls::<String, String, _>(addr, fory_cli, client_rx, server_name)
            .await
            .unwrap();

    let req = make_request(7, "tls-no-body");
    client.send((req, None)).await.unwrap();

    let (resp, body) = client.next().await.unwrap().unwrap();
    assert_eq!(resp.request_id, 7);
    assert_eq!(resp.message.unwrap(), "no-body-ok");
    assert!(body.is_none(), "expected no body in response");
}

// ---------------------------------------------------------------------------
// Test 3: cert rotation
//
// Mirrors fory_tls.rs::cert_rotation_no_drop, adapted for the zero-copy API.
//
// 1. Establish a connection under cert1.
// 2. Send a request — it succeeds.
// 3. Rotate the server to cert2 via the watch channel.
// 4. Existing connection still alive — send another request, it succeeds.
// 5. Update the client trust store to cert2.
// 6. Open a new connection — handshake succeeds with the new cert.
// 7. Send a request on the new connection — it succeeds.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zerocopy_tls_cert_rotation() {
    let (cert1, key1) = make_self_signed();
    let (cert2, key2) = make_self_signed();

    let (server_tx, server_rx) = watch::channel(make_server_config(cert1.clone(), key1));
    let (client_tx, client_rx) = watch::channel(make_client_config(&cert1));

    let fory_srv = make_fory();
    let fory_cli1 = make_fory();
    let fory_cli2 = make_fory();

    let mut incoming =
        listen_zerocopy_tls::<String, String, _>("127.0.0.1:0", fory_srv, server_rx)
            .await
            .unwrap();
    let addr = incoming.local_addr();

    // Server: handle multiple connections, each receiving one request.
    tokio::spawn(async move {
        while let Some(Ok(mut srv)) = incoming.next().await {
            tokio::spawn(async move {
                while let Some(Ok((ClientMessage::Request(req), body))) = srv.next().await {
                    let resp = Response {
                        request_id: req.id,
                        message: Ok(format!("echo:{}", req.message)),
                    };
                    srv.send((resp, body)).await.unwrap();
                }
            });
        }
    });

    let server_name = ServerName::try_from("localhost").unwrap();

    // --- Connection 1 under cert1 ---
    let mut client1 =
        connect_zerocopy_tls::<String, String, _>(addr, fory_cli1, client_rx.clone(), server_name.clone())
            .await
            .unwrap();

    let req = make_request(1, "before-rotation");
    client1.send((req, None)).await.unwrap();
    let (resp, _) = client1.next().await.unwrap().unwrap();
    assert_eq!(resp.request_id, 1);
    assert_eq!(resp.message.unwrap(), "echo:before-rotation");

    // --- Rotate server cert to cert2 ---
    // Existing TLS sessions are independent of the ServerConfig snapshot used
    // at handshake time; they MUST NOT be dropped by the rotation.
    server_tx.send(make_server_config(cert2.clone(), key2)).unwrap();

    // Existing connection still alive after rotation.
    let req2 = make_request(2, "during-rotation");
    client1.send((req2, None)).await.unwrap();
    let (resp2, _) = client1.next().await.unwrap().unwrap();
    assert_eq!(resp2.request_id, 2);
    assert_eq!(resp2.message.unwrap(), "echo:during-rotation");

    // --- Update client trust store to cert2 ---
    client_tx.send(make_client_config(&cert2)).unwrap();

    // --- Connection 2 under cert2 ---
    let mut client2 =
        connect_zerocopy_tls::<String, String, _>(addr, fory_cli2, client_rx, server_name)
            .await
            .unwrap();

    let req3 = make_request(3, "after-rotation");
    client2.send((req3, None)).await.unwrap();
    let (resp3, _) = client2.next().await.unwrap().unwrap();
    assert_eq!(resp3.request_id, 3);
    assert_eq!(resp3.message.unwrap(), "echo:after-rotation");
}
