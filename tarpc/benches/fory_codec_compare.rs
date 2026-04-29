//! Codec throughput comparison: classic (tokio-serde ForyEnvelopeCodec) vs
//! zero-copy (tokio-util ZeroCopyForyCodec) on 4 MiB body workloads.
//!
//! # Workloads
//!
//! - `send_4mib`        — encode side only. Client encodes a 4 MiB frame;
//!                        server discards it. Measures serialisation + framing.
//! - `recv_4mib`        — decode side only. Server sends a 4 MiB response back;
//!                        client receives it. recv path is where zero-copy wins.
//! - `roundtrip_small`  — ping-shaped: empty body, full send+recv round-trip.
//!                        Regression guard — expected within ±5%.
//!
//! # Connection reuse
//!
//! Each group sets up one loopback TCP connection outside the iter loop.
//! iter_custom is used so that the client/server futures can own the transport
//! across multiple samples without moves.

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use fory::Fory;
use futures::{SinkExt as _, StreamExt as _};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tarpc::{
    ClientMessage, Request, Response,
    context,
    serde_transport::{
        fory_envelope::register_envelope_types,
        fory_zerocopy::{connect_zerocopy, listen_zerocopy},
    },
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Mutex,
};
use tokio_util::codec::length_delimited;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

const BODY_4MIB: usize = 4 * 1024 * 1024;

/// Global request-id counter.
static REQ_ID: AtomicU64 = AtomicU64::new(1);

fn next_id() -> u64 {
    REQ_ID.fetch_add(1, Ordering::Relaxed)
}

fn make_fory() -> Arc<Fory> {
    let mut fory = Fory::default();
    register_envelope_types::<String>(&mut fory).unwrap();
    Arc::new(fory)
}

fn make_request(id: u64) -> ClientMessage<String> {
    ClientMessage::Request(Request {
        context: context::current(),
        id,
        message: "bench".to_string(),
    })
}

fn make_response(request_id: u64) -> Response<String> {
    Response { request_id, message: Ok("ok".to_string()) }
}

// ---------------------------------------------------------------------------
// Type aliases
// ---------------------------------------------------------------------------

use tarpc::serde_transport::fory as fory_transport;

type ClassicClientTransport = tarpc::serde_transport::Transport<
    TcpStream,
    Response<String>,
    ClientMessage<String>,
    tarpc::serde_transport::fory::ForyEnvelopeCodec<String, String>,
>;

type ClassicServerTransport = tarpc::serde_transport::Transport<
    TcpStream,
    ClientMessage<String>,
    Response<String>,
    tarpc::serde_transport::fory::ForyEnvelopeCodec<String, String>,
>;

type ZcClientTransport =
    tarpc::serde_transport::fory_zerocopy::ZeroCopyTransport<String, String>;

type ZcServerTransport =
    tarpc::serde_transport::fory_zerocopy::ZeroCopyServerTransport<String, String>;

// ---------------------------------------------------------------------------
// Connection builders
// ---------------------------------------------------------------------------

async fn classic_pair() -> (ClassicClientTransport, ClassicServerTransport) {
    let fory = make_fory();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let fory_srv = fory.clone();
    let server_fut = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let framed = length_delimited::Builder::new()
            .max_frame_length(usize::MAX / 2)
            .new_framed(stream);
        tarpc::serde_transport::new(
            framed,
            tarpc::serde_transport::fory::ForyEnvelopeCodec::<String, String>::new(fory_srv),
        )
    });

    let client =
        fory_transport::connect_with_fory::<_, String, String>(addr, fory).await.unwrap();
    let server = server_fut.await.unwrap();
    (client, server)
}

async fn zerocopy_pair() -> (ZcClientTransport, ZcServerTransport) {
    let fory = make_fory();
    let mut incoming =
        listen_zerocopy::<String, String, _>("127.0.0.1:0", fory.clone()).await.unwrap();
    let addr = incoming.local_addr();

    let server_fut =
        tokio::spawn(async move { incoming.next().await.unwrap().unwrap() });

    let client =
        connect_zerocopy::<String, String, _>(addr, fory).await.unwrap();
    let server = server_fut.await.unwrap();
    (client, server)
}

// ---------------------------------------------------------------------------
// send_4mib: client sends a 4 MiB frame; server discards it.
// ---------------------------------------------------------------------------

fn bench_send_4mib(c: &mut Criterion) {
    let mut group = c.benchmark_group("send_4mib");
    group.throughput(Throughput::Bytes(BODY_4MIB as u64));
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(15));

    // ---------- classic ----------
    group.bench_function(BenchmarkId::new("classic", "4mib"), |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (client, server) = rt.block_on(classic_pair());

        // Server: discard everything.
        rt.spawn(async move {
            let mut s = server;
            while (s.next().await).is_some() {}
        });

        // Wrap client in Arc<Mutex<>> so iter_custom closure is FnMut.
        let client = Arc::new(Mutex::new(client));
        let body_str = Arc::new("A".repeat(BODY_4MIB));

        b.iter_custom(|iters| {
            let client = client.clone();
            let body_str = body_str.clone();
            rt.block_on(async move {
                let mut c = client.lock().await;
                let start = Instant::now();
                for _ in 0..iters {
                    let req = ClientMessage::Request(Request {
                        context: context::current(),
                        id: next_id(),
                        message: (*body_str).clone(),
                    });
                    c.send(req).await.unwrap();
                }
                c.flush().await.unwrap();
                start.elapsed()
            })
        });
    });

    // ---------- zerocopy ----------
    group.bench_function(BenchmarkId::new("zerocopy", "4mib"), |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (client, server) = rt.block_on(zerocopy_pair());

        rt.spawn(async move {
            let mut s = server;
            while (s.next().await).is_some() {}
        });

        let client = Arc::new(Mutex::new(client));
        let body = Bytes::from(vec![0x42u8; BODY_4MIB]);

        b.iter_custom(|iters| {
            let client = client.clone();
            let body = body.clone();
            rt.block_on(async move {
                let mut c = client.lock().await;
                let start = Instant::now();
                for _ in 0..iters {
                    let req = make_request(next_id());
                    c.send((req, Some(body.clone()))).await.unwrap();
                }
                c.flush().await.unwrap();
                start.elapsed()
            })
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// recv_4mib: server sends a 4 MiB response; client receives it.
// ---------------------------------------------------------------------------

fn bench_recv_4mib(c: &mut Criterion) {
    let mut group = c.benchmark_group("recv_4mib");
    group.throughput(Throughput::Bytes(BODY_4MIB as u64));
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(15));

    // ---------- classic ----------
    group.bench_function(BenchmarkId::new("classic", "4mib"), |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (client, server) = rt.block_on(classic_pair());

        rt.spawn(async move {
            let mut s = server;
            let large_payload = "X".repeat(BODY_4MIB);
            while let Some(Ok(ClientMessage::Request(req))) = s.next().await {
                let resp =
                    Response { request_id: req.id, message: Ok(large_payload.clone()) };
                s.send(resp).await.unwrap();
            }
        });

        let client = Arc::new(Mutex::new(client));

        b.iter_custom(|iters| {
            let client = client.clone();
            rt.block_on(async move {
                let mut c = client.lock().await;
                let start = Instant::now();
                for _ in 0..iters {
                    c.send(make_request(next_id())).await.unwrap();
                    let _ = c.next().await.unwrap().unwrap();
                }
                start.elapsed()
            })
        });
    });

    // ---------- zerocopy ----------
    group.bench_function(BenchmarkId::new("zerocopy", "4mib"), |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (client, server) = rt.block_on(zerocopy_pair());

        let response_body = Bytes::from(vec![0x5Au8; BODY_4MIB]);

        rt.spawn(async move {
            let mut s = server;
            while let Some(Ok((ClientMessage::Request(req), _))) = s.next().await {
                let resp = make_response(req.id);
                s.send((resp, Some(response_body.clone()))).await.unwrap();
            }
        });

        let client = Arc::new(Mutex::new(client));

        b.iter_custom(|iters| {
            let client = client.clone();
            rt.block_on(async move {
                let mut c = client.lock().await;
                let start = Instant::now();
                for _ in 0..iters {
                    c.send((make_request(next_id()), None)).await.unwrap();
                    let _ = c.next().await.unwrap().unwrap();
                }
                start.elapsed()
            })
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// roundtrip_small: ping-shaped, no body, full send+recv cycle.
// ---------------------------------------------------------------------------

fn bench_roundtrip_small(c: &mut Criterion) {
    let mut group = c.benchmark_group("roundtrip_small");
    group.throughput(Throughput::Elements(1));
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));

    // ---------- classic ----------
    group.bench_function("classic", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (client, server) = rt.block_on(classic_pair());

        rt.spawn(async move {
            let mut s = server;
            while let Some(Ok(ClientMessage::Request(req))) = s.next().await {
                let resp = make_response(req.id);
                s.send(resp).await.unwrap();
            }
        });

        let client = Arc::new(Mutex::new(client));

        b.iter_custom(|iters| {
            let client = client.clone();
            rt.block_on(async move {
                let mut c = client.lock().await;
                let start = Instant::now();
                for _ in 0..iters {
                    c.send(make_request(next_id())).await.unwrap();
                    let _ = c.next().await.unwrap().unwrap();
                }
                start.elapsed()
            })
        });
    });

    // ---------- zerocopy ----------
    group.bench_function("zerocopy", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (client, server) = rt.block_on(zerocopy_pair());

        rt.spawn(async move {
            let mut s = server;
            while let Some(Ok((ClientMessage::Request(req), _))) = s.next().await {
                let resp = make_response(req.id);
                s.send((resp, None)).await.unwrap();
            }
        });

        let client = Arc::new(Mutex::new(client));

        b.iter_custom(|iters| {
            let client = client.clone();
            rt.block_on(async move {
                let mut c = client.lock().await;
                let start = Instant::now();
                for _ in 0..iters {
                    c.send((make_request(next_id()), None)).await.unwrap();
                    let _ = c.next().await.unwrap().unwrap();
                }
                start.elapsed()
            })
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Criterion harness
// ---------------------------------------------------------------------------

criterion_group!(benches, bench_send_4mib, bench_recv_4mib, bench_roundtrip_small);
criterion_main!(benches);
