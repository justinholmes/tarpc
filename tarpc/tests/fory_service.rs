// Copyright 2018 Google LLC
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! Canonical proof: `#[tarpc::service]` over real fory TCP.
//!
//! This test is the deliverable for the TYPE_ID_COUNTER bypass. It proves that:
//!
//! 1. `#[tarpc::service]`-generated types (`HelloRequest`, `HelloResponse`) derive
//!    `ForyObject` via the proc-macro patch and can be registered in a `Fory` instance.
//! 2. Tarpc envelope types (`ForyTraceContext`, etc.) use manual `Serializer` impls
//!    and are registered via `register_serializer` (EXT type path).
//! 3. Both can coexist in the same `Fory` instance without "Type index N already
//!    registered" collision — because `register_serializer` never touches the
//!    `type_id_index` Vec that `register` writes via `fory_type_index()`.
//! 4. A full `#[tarpc::service]` call round-trip works over a real fory TCP transport.
//!
//! ## Why the collision is gone
//!
//! - `register::<HelloRequest>(100)` writes to `type_id_index[HelloRequest::fory_type_index()]`
//!   (index 0 in this test binary's crate).
//! - `register_serializer::<ForyTraceContext>(2)` writes to `user_type_info_by_id[2]`
//!   and never touches `type_id_index`.
//! - Two different data structures → no collision.

#![cfg(all(feature = "serde-transport-fory", feature = "tcp"))]

use fory::Fory;
use futures::StreamExt as _;
use std::sync::Arc;
use tarpc::{
    client, context,
    server::{self, Channel},
    server::incoming::Incoming as _,
    serde_transport::fory as fory_transport,
    serde_transport::fory_envelope::{
        ForyClientMessage, ForyRequest, ForyResponse, ForyResult, ForyServerError, ForyTraceContext,
        register_envelope_types,
    },
};

// ---------------------------------------------------------------------------
// Service definition
//
// The proc-macro emits `#[cfg_attr(feature = "fory", derive(::fory::ForyObject))]`
// on both HelloRequest and HelloResponse, so they implement Serializer + ForyDefault.
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Helper: build Fory registry with both envelope types and user types.
//
// This is the registration that was previously IMPOSSIBLE due to the
// TYPE_ID_COUNTER collision. Now it works.
//
// We use register_envelope_types::<HelloRequest> for the request-side types,
// and manually register the response-side types with different IDs to avoid
// conflicts when Req != Resp. IDs 2-7 are for the Req-parameterized types;
// IDs 8-9 are for the Resp-only types (ForyResult<Resp>, ForyResponse<Resp>).
// ---------------------------------------------------------------------------

fn make_fory() -> Arc<Fory> {
    let mut fory = Fory::default();

    // Non-generic envelope types (shared by Req and Resp sides).
    fory.register_serializer::<ForyTraceContext>(2).unwrap();
    fory.register_serializer::<ForyServerError>(3).unwrap();

    // Request-side generic types (parameterized by HelloRequest).
    fory.register_serializer::<ForyResult<HelloRequest>>(4).unwrap();
    fory.register_serializer::<ForyRequest<HelloRequest>>(5).unwrap();
    fory.register_serializer::<ForyClientMessage<HelloRequest>>(7).unwrap();

    // Response-side generic types (parameterized by HelloResponse).
    // Use IDs 8-9 to avoid collision with the request-side IDs 4 and 6.
    fory.register_serializer::<ForyResult<HelloResponse>>(8).unwrap();
    fory.register_serializer::<ForyResponse<HelloResponse>>(6).unwrap();

    // Register user types via register (STRUCT path — uses fory_type_index).
    // HelloRequest is the first ForyObject in this test binary → index 0.
    // HelloResponse is the second → index 1.
    // Envelope types registered above do NOT occupy index 0 or 1 — no collision.
    fory.register::<HelloRequest>(100).unwrap();
    fory.register::<HelloResponse>(101).unwrap();

    Arc::new(fory)
}

// ---------------------------------------------------------------------------
// THE CANONICAL PROOF: #[tarpc::service] over real fory TCP
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hello_service_over_fory_tcp_real() {
    let fory = make_fory();

    // Server: listen on an ephemeral port.
    let mut listener =
        fory_transport::listen::<_, HelloRequest, HelloResponse>("127.0.0.1:0", fory.clone())
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

    // Client: connect, call, assert.
    let transport =
        fory_transport::connect::<_, HelloRequest, HelloResponse>(addr, fory)
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
// Test 2: In-memory serialization of generated types (regression guard)
//
// Verifies that HelloRequest and HelloResponse implement ForyObject correctly
// in isolation.
// ---------------------------------------------------------------------------

#[test]
fn generated_types_satisfy_fory_bounds() {
    let mut fory = Fory::default();
    fory.register::<HelloRequest>(10).unwrap();
    fory.register::<HelloResponse>(11).unwrap();

    let req = HelloRequest::Hello { name: "world".to_string() };
    let bytes = fory.serialize(&req).unwrap();
    let decoded: HelloRequest = fory.deserialize(&bytes).unwrap();
    assert!(matches!(decoded, HelloRequest::Hello { name } if name == "world"));

    let resp = HelloResponse::Hello("hello, world".to_string());
    let bytes = fory.serialize(&resp).unwrap();
    let decoded: HelloResponse = fory.deserialize(&bytes).unwrap();
    assert!(matches!(decoded, HelloResponse::Hello(ref s) if s == "hello, world"));
}

// ---------------------------------------------------------------------------
// Test 3: Envelope + user types in same registry (the collision test)
//
// Proves register_envelope_types and register::<HelloXxx> can coexist.
// ---------------------------------------------------------------------------

#[test]
fn envelope_and_user_types_coexist_in_same_registry() {
    let mut fory = Fory::default();
    // This used to fail with "Type index 0 already registered".
    // Now it succeeds because register_serializer and register use different
    // internal data structures.
    register_envelope_types::<HelloRequest>(&mut fory)
        .expect("register_envelope_types should not fail");
    fory.register::<HelloRequest>(100)
        .expect("register HelloRequest should not fail");
    // Verify both are usable.
    let req = HelloRequest::Hello { name: "test".to_string() };
    let bytes = fory.serialize(&req).unwrap();
    let decoded: HelloRequest = fory.deserialize(&bytes).unwrap();
    assert!(matches!(decoded, HelloRequest::Hello { name } if name == "test"));
}
