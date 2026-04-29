//! Additional coverage tests for the fory transport layer.
//!
//! Covers:
//!   - Cancel message end-to-end over TCP
//!   - ServerError through the full transport
//!   - Two services on separate Fory instances (no ID collision)

#![cfg(all(feature = "serde-transport-fory", feature = "tcp"))]

use fory::Fory;
use futures::{SinkExt as _, StreamExt as _};
use std::sync::Arc;
use tarpc::{
    ClientMessage, Request, Response, ServerError,
    context,
    serde_transport::fory as fory_transport,
    serde_transport::fory_envelope::register_envelope_types,
    trace,
};

// ---------------------------------------------------------------------------
// Helper: build a Fory registry for String req/resp.
// ---------------------------------------------------------------------------

fn make_fory_string() -> Arc<Fory> {
    let mut fory = Fory::default();
    register_envelope_types::<String>(&mut fory).unwrap();
    Arc::new(fory)
}

// ---------------------------------------------------------------------------
// cancel_message_round_trip_tcp
//
// Sends a ClientMessage::Cancel over the fory TCP transport and verifies that
// the server receives a Cancel variant with the correct request_id.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cancel_message_round_trip_tcp() {
    let fory = make_fory_string();

    let mut incoming =
        fory_transport::listen_with_fory::<_, String, String>("127.0.0.1:0", fory.clone())
            .await
            .unwrap();
    let addr = incoming.local_addr();

    // Server: receive one message, assert it's a Cancel with request_id = 42.
    let server_task = tokio::spawn(async move {
        if let Some(Ok(mut server_transport)) = incoming.next().await {
            let msg = futures::StreamExt::next(&mut server_transport).await;
            match msg {
                Some(Ok(ClientMessage::Cancel { request_id, .. })) => {
                    assert_eq!(request_id, 42, "cancel request_id mismatch");
                }
                other => panic!("expected Cancel, got {:?}", other),
            }
        } else {
            panic!("no connection accepted");
        }
    });

    // Client: connect and send a Cancel.
    let mut client_transport =
        fory_transport::connect_with_fory::<_, String, String>(addr, fory)
            .await
            .unwrap();

    let cancel = ClientMessage::Cancel {
        trace_context: trace::Context::default(),
        request_id: 42,
    };
    client_transport.send(cancel).await.unwrap();

    server_task.await.unwrap();
}

// ---------------------------------------------------------------------------
// server_error_round_trip_tcp
//
// Server sends Response { message: Err(ServerError { kind: NotFound, detail:
// "gone" }) }. Client receives it and asserts kind + detail match.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn server_error_round_trip_tcp() {
    use std::io;

    let fory = make_fory_string();

    let mut incoming =
        fory_transport::listen_with_fory::<_, String, String>("127.0.0.1:0", fory.clone())
            .await
            .unwrap();
    let addr = incoming.local_addr();

    // Server: wait for one request, reply with a ServerError.
    tokio::spawn(async move {
        if let Some(Ok(mut server_transport)) = incoming.next().await {
            // Consume the incoming request so the client isn't blocked sending.
            let _ = futures::StreamExt::next(&mut server_transport).await;
            let err_resp = Response::<String> {
                request_id: 1,
                message: Err(ServerError::new(
                    io::ErrorKind::NotFound,
                    "gone".to_string(),
                )),
            };
            let _ = server_transport.send(err_resp).await;
        }
    });

    // Client: connect, send a request, receive the error response.
    let mut client_transport =
        fory_transport::connect_with_fory::<_, String, String>(addr, fory)
            .await
            .unwrap();

    let req = ClientMessage::Request(Request {
        context: context::current(),
        id: 1,
        message: "ping".to_string(),
    });
    client_transport.send(req).await.unwrap();

    let response = futures::StreamExt::next(&mut client_transport)
        .await
        .expect("expected a response")
        .expect("io error receiving response");

    assert_eq!(response.request_id, 1);
    let err = response.message.expect_err("expected Err response");
    assert_eq!(err.kind, std::io::ErrorKind::NotFound);
    assert_eq!(err.detail, "gone");
}

// ---------------------------------------------------------------------------
// two_services_separate_registries
//
// Two #[tarpc::service] definitions on separate ports with separate Fory
// instances. Verifies that no ID collision occurs and both round-trip cleanly.
// ---------------------------------------------------------------------------

use tarpc::{client, server::{self, Channel}};

#[tarpc::service(derive = [Clone, serde::Serialize, serde::Deserialize])]
trait Alpha {
    async fn greet(name: String) -> String;
}

#[tarpc::service(derive = [Clone, serde::Serialize, serde::Deserialize])]
trait Beta {
    async fn square(n: u32) -> u32;
}

#[derive(Clone)]
struct AlphaServer;

impl Alpha for AlphaServer {
    async fn greet(self, _: context::Context, name: String) -> String {
        format!("hello, {}", name)
    }
}

#[derive(Clone)]
struct BetaServer;

impl Beta for BetaServer {
    async fn square(self, _: context::Context, n: u32) -> u32 {
        n * n
    }
}

#[tokio::test]
async fn two_services_separate_registries() {
    // ---- Alpha service ----
    let mut alpha_listener = fory_transport::listen::<AlphaService, _>("127.0.0.1:0")
        .await
        .unwrap();
    let alpha_addr = alpha_listener.local_addr();

    tokio::spawn(async move {
        while let Some(Ok(transport)) = alpha_listener.next().await {
            let channel = server::BaseChannel::with_defaults(transport);
            tokio::spawn(
                channel
                    .execute(AlphaServer.serve())
                    .for_each(|fut| async move { tokio::spawn(fut); }),
            );
        }
    });

    // ---- Beta service ----
    let mut beta_listener = fory_transport::listen::<BetaService, _>("127.0.0.1:0")
        .await
        .unwrap();
    let beta_addr = beta_listener.local_addr();

    tokio::spawn(async move {
        while let Some(Ok(transport)) = beta_listener.next().await {
            let channel = server::BaseChannel::with_defaults(transport);
            tokio::spawn(
                channel
                    .execute(BetaServer.serve())
                    .for_each(|fut| async move { tokio::spawn(fut); }),
            );
        }
    });

    // ---- Alpha client ----
    let alpha_transport = fory_transport::connect::<AlphaService, _>(alpha_addr)
        .await
        .unwrap();
    let alpha_client = AlphaClient::new(client::Config::default(), alpha_transport).spawn();

    let greet_resp = alpha_client
        .greet(context::current(), "world".to_string())
        .await
        .unwrap();
    assert_eq!(greet_resp, "hello, world");

    // ---- Beta client ----
    let beta_transport = fory_transport::connect::<BetaService, _>(beta_addr)
        .await
        .unwrap();
    let beta_client = BetaClient::new(client::Config::default(), beta_transport).spawn();

    let square_resp = beta_client.square(context::current(), 7).await.unwrap();
    assert_eq!(square_resp, 49);
}
