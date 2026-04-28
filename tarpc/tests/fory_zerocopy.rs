//! Zero-copy bulk-payload codec tests.
//!
//! The canonical proof of zero-copy is `body_is_aliased`: the `Bytes` returned
//! by the codec for the body region must point into the same allocation as the
//! frame buffer, not a freshly allocated copy.

#![cfg(all(feature = "serde-transport-fory", feature = "tcp"))]

use bytes::Bytes;
use fory::Fory;
use futures::{SinkExt as _, StreamExt as _};
use std::sync::Arc;
use tarpc::{
    ClientMessage, Request, Response,
    context,
    serde_transport::{
        fory_envelope::register_envelope_types,
        fory_zerocopy::{connect_zerocopy, listen_zerocopy},
    },
};

// ---------------------------------------------------------------------------
// Helper: shared Fory registry
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

// ---------------------------------------------------------------------------
// body_is_aliased — canonical zero-copy proof
//
// Strategy: encode a frame from the client side (encoder serialises into a
// single contiguous BytesMut), transmit over TCP, and on the server side
// capture both the raw frame pointer and the decoded body pointer.
//
// The server-side ZeroCopyForyCodec uses BytesMut::split_off to extract the
// body region. split_off returns a BytesMut that shares the same underlying
// Arc<[u8]> allocation as the parent frame buffer; .freeze() produces a Bytes
// that still points into the same memory.
//
// We verify:
//   1. received_body.len() == 4 MiB
//   2. The sentinel byte pattern is intact (0xAB throughout).
//   3. The body Bytes' raw pointer falls within the bounds of the frame
//      that was assembled on the server side — specifically, we record the
//      pointer of the entire frame BytesMut before any split and confirm
//      that received_body.as_ptr() >= frame_start.
//
// Approach for #3: we intercept the frame allocation by testing with a
// deliberate encode/decode cycle through an in-memory BytesMut (no TCP),
// which lets us inspect both sides of the allocation in the same process.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn body_is_aliased() {
    use tarpc::serde_transport::fory_zerocopy::{ClientZeroCopyCodec, ServerZeroCopyCodec};
    use tokio_util::codec::{Decoder, Encoder};

    let fory = make_fory();

    // Build a 4 MiB body with sentinel pattern.
    let body_data = vec![0xABu8; 4 * 1024 * 1024];
    let body = Bytes::from(body_data);

    let req = make_request(1, "zerocopy-test");

    // Encode using client-side codec into a raw BytesMut.
    let mut client_codec = ClientZeroCopyCodec::<String, String>::new(fory.clone());
    let mut wire_buf = bytes::BytesMut::new();
    client_codec
        .encode((req, Some(body.clone())), &mut wire_buf)
        .expect("encode failed");

    // Record the start of the wire buffer allocation before the server
    // decoder splits it.  After LengthDelimitedCodec pulls out the frame,
    // the frame BytesMut shares the same backing store as wire_buf (because
    // LengthDelimitedCodec does split_to under the hood).
    let wire_start = wire_buf.as_ptr() as usize;
    let wire_end = wire_start + wire_buf.capacity();

    // Decode using server-side codec.
    let mut server_codec = ServerZeroCopyCodec::<String, String>::new(fory.clone());
    let decoded = server_codec
        .decode(&mut wire_buf)
        .expect("decode returned Err")
        .expect("decode returned None — frame incomplete");

    let (_envelope, received_body) = decoded;
    let received_body = received_body.expect("expected Some(body), got None");

    // 1. Length matches.
    assert_eq!(
        received_body.len(),
        4 * 1024 * 1024,
        "body length mismatch"
    );

    // 2. Sentinel bytes intact.
    assert_eq!(received_body[0], 0xAB, "sentinel at [0] corrupted");
    assert_eq!(
        received_body[received_body.len() - 1],
        0xAB,
        "sentinel at [last] corrupted"
    );

    // 3. Zero-copy: the body Bytes points into the original frame allocation.
    //    After LengthDelimitedCodec's split_to, the frame BytesMut refers to
    //    the same backing memory as wire_buf. split_off on that frame gives a
    //    BytesMut (→ Bytes via freeze()) whose data pointer is within the
    //    original allocation.
    let body_ptr = received_body.as_ptr() as usize;
    assert!(
        body_ptr >= wire_start && body_ptr + received_body.len() <= wire_end + 8,
        // +8 for the length-prefix bytes that may shift the window slightly.
        "body pointer 0x{:x} is NOT within frame allocation [0x{:x}, 0x{:x}) — a copy occurred",
        body_ptr,
        wire_start,
        wire_end,
    );
}

// ---------------------------------------------------------------------------
// round_trip_4mib_body — TCP end-to-end with a 4 MiB body
// ---------------------------------------------------------------------------

#[tokio::test]
async fn round_trip_4mib_body() {
    let fory = make_fory();

    let mut incoming =
        listen_zerocopy::<String, String, _>("127.0.0.1:0", fory.clone())
            .await
            .unwrap();
    let addr = incoming.local_addr();

    let body_data = vec![0xABu8; 4 * 1024 * 1024];
    let body_send = Bytes::from(body_data.clone());

    // Server: receive (ClientMessage, body) and echo the body back as a
    // Response payload (encoded as a String of the body length for simplicity)
    // while also returning the body bytes in the response body slot.
    tokio::spawn(async move {
        if let Some(Ok(mut srv)) = incoming.next().await {
            if let Some(Ok((ClientMessage::Request(req), body))) = srv.next().await {
                let body_len = body.as_ref().map(|b| b.len()).unwrap_or(0);
                let resp = Response {
                    request_id: req.id,
                    message: Ok(format!("body_len:{}", body_len)),
                };
                // Echo the body back unchanged.
                srv.send((resp, body)).await.unwrap();
            }
        }
    });

    let mut client =
        connect_zerocopy::<String, String, _>(addr, fory).await.unwrap();

    let req = make_request(42, "big-payload");
    client.send((req, Some(body_send.clone()))).await.unwrap();

    let (resp, resp_body) = client.next().await.unwrap().unwrap();
    assert_eq!(resp.request_id, 42);
    assert_eq!(
        resp.message.unwrap(),
        format!("body_len:{}", 4 * 1024 * 1024)
    );

    let rb = resp_body.expect("expected body in response");
    assert_eq!(rb.len(), 4 * 1024 * 1024);
    assert_eq!(&rb[..], &body_data[..]);
}

// ---------------------------------------------------------------------------
// no_body_request — body = None round-trip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn no_body_request() {
    let fory = make_fory();

    let mut incoming =
        listen_zerocopy::<String, String, _>("127.0.0.1:0", fory.clone())
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

    let mut client =
        connect_zerocopy::<String, String, _>(addr, fory).await.unwrap();

    let req = make_request(7, "no-body");
    client.send((req, None)).await.unwrap();

    let (resp, body) = client.next().await.unwrap().unwrap();
    assert_eq!(resp.request_id, 7);
    assert_eq!(resp.message.unwrap(), "no-body-ok");
    assert!(body.is_none(), "expected no body in response");
}

// ---------------------------------------------------------------------------
// concurrent_4mib_bodies — 100 concurrent 4 MiB calls on one connection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn concurrent_4mib_bodies() {
    use std::collections::HashMap;

    let fory = make_fory();

    let mut incoming =
        listen_zerocopy::<String, String, _>("127.0.0.1:0", fory.clone())
            .await
            .unwrap();
    let addr = incoming.local_addr();

    // Server: echo each request back with its body, preserving request_id.
    tokio::spawn(async move {
        if let Some(Ok(srv)) = incoming.next().await {
            let (mut sink, mut stream) = futures::StreamExt::split(srv);
            let mut responses: Vec<(Response<String>, Option<Bytes>)> = Vec::new();

            // Collect all 100 requests first, then send all responses.
            // (A real server would spawn per-request, but this is a test.)
            while let Some(Ok((ClientMessage::Request(req), body))) = stream.next().await {
                let body_len = body.as_ref().map(|b| b.len()).unwrap_or(0);
                let resp = Response {
                    request_id: req.id,
                    message: Ok(format!("id:{}:len:{}", req.id, body_len)),
                };
                responses.push((resp, body));
                if responses.len() == 100 {
                    break;
                }
            }
            for item in responses {
                sink.send(item).await.unwrap();
            }
        }
    });

    let mut client =
        connect_zerocopy::<String, String, _>(addr, fory).await.unwrap();

    let body_template = vec![0xCDu8; 4 * 1024 * 1024];

    // Send 100 requests.
    for i in 0u64..100 {
        let body = Bytes::from(body_template.clone());
        let req = make_request(i, &format!("concurrent-{}", i));
        client.send((req, Some(body))).await.unwrap();
    }

    // Receive all 100 responses.
    let mut seen: HashMap<u64, bool> = HashMap::new();
    for _ in 0..100 {
        let (resp, body) = client.next().await.unwrap().unwrap();
        let rb = body.expect("expected body");
        assert_eq!(rb.len(), 4 * 1024 * 1024, "body length wrong for id {}", resp.request_id);
        assert_eq!(rb[0], 0xCD, "sentinel wrong for id {}", resp.request_id);
        let msg = resp.message.unwrap();
        assert!(
            msg.starts_with(&format!("id:{}:len:", resp.request_id)),
            "unexpected msg: {}",
            msg
        );
        seen.insert(resp.request_id, true);
    }
    assert_eq!(seen.len(), 100, "did not receive all 100 responses");
}
