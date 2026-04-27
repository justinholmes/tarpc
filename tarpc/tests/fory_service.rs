//! Canonical proof: `#[tarpc::service]` over real fory TCP — schema-driven, zero boilerplate.
//!
//! User code does NOT register any types. The proc-macro emits a `HelloService` marker
//! type implementing `ServiceWireSchema`; `tarpc_fory::listen::<HelloService>(addr)`
//! auto-registers all envelope wrappers, generated `HelloRequest` / `HelloResponse`,
//! and any user types referenced in method signatures.

#![cfg(all(feature = "serde-transport-fory", feature = "tcp"))]

use futures::StreamExt as _;
use tarpc::{
    client, context,
    server::{self, Channel},
    serde_transport::fory as fory_transport,
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

#[tokio::test]
async fn hello_service_over_fory_tcp_real() {
    // Server: listen on an ephemeral port. ZERO registration.
    let mut listener = fory_transport::listen::<HelloService, _>("127.0.0.1:0")
        .await
        .unwrap();
    let addr = listener.local_addr();

    tokio::spawn(async move {
        while let Some(Ok(transport)) = listener.next().await {
            let channel = server::BaseChannel::with_defaults(transport);
            tokio::spawn(
                channel
                    .execute(HelloServer.serve())
                    .for_each(|fut| async move {
                        tokio::spawn(fut);
                    }),
            );
        }
    });

    // Client: connect, call, assert. ZERO registration.
    let transport = fory_transport::connect::<HelloService, _>(addr).await.unwrap();
    let client = HelloClient::new(client::Config::default(), transport).spawn();

    let resp = client
        .hello(context::current(), "world".to_string())
        .await
        .unwrap();

    assert_eq!(resp, "hello, world");
}
