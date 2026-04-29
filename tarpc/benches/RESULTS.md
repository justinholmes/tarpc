# Codec Comparison: classic vs zero-copy

Date: 2026-04-25
Hardware: AMD Ryzen 9 5900X 12-Core Processor, WSL2 (Linux 6.6.87.2-microsoft-standard-WSL2), x86_64

## Raw criterion output

```
send_4mib/classic/4mib   time: [2.89 ms  2.96 ms  3.07 ms]   thrpt: [1.27 GiB/s  1.32 GiB/s  1.35 GiB/s]
send_4mib/zerocopy/4mib  time: [1.39 ms  1.44 ms  1.48 ms]   thrpt: [2.64 GiB/s  2.72 GiB/s  2.81 GiB/s]

recv_4mib/classic/4mib   time: [3.79 ms  3.91 ms  4.05 ms]   thrpt: [986 MiB/s  1023 MiB/s  1.03 GiB/s]
recv_4mib/zerocopy/4mib  time: [2.17 ms  2.33 ms  2.47 ms]   thrpt: [1.58 GiB/s  1.68 GiB/s  1.80 GiB/s]

roundtrip_small/classic  time: [101 µs  103 µs  105 µs]
roundtrip_small/zerocopy time: [100 µs  102 µs  103 µs]
```

## Summary table

| Workload         | Classic         | Zero-copy       | Delta           |
|------------------|-----------------|-----------------|-----------------|
| send_4mib        | 1.32 GiB/s      | 2.72 GiB/s      | +106% (2.1×)    |
| recv_4mib        | 1.02 GiB/s      | 1.68 GiB/s      | +65%  (1.6×)    |
| roundtrip_small  | 103 µs/op       | 102 µs/op       | -1% (no change) |

## Interpretation

### send_4mib (+106%)

Larger-than-expected win for zerocopy on the send side. Both codecs perform at
least one memcpy of the body (the encode path is documented to do two in
fory_zerocopy.rs). The gap comes from the classic codec serialising the 4 MiB
payload as an inline fory field — fory allocates a new Vec<u8> for the entire
serialised frame (envelope + body), whereas the zerocopy codec copies only the
body into a pre-sized BytesMut. The body-as-String serialisation in the classic
bench reflects its type system constraint (fory-registered Vec<u8> requires an
explicit type ID); real payloads of the same byte count would behave similarly.

### recv_4mib (+65%) — the load-bearing number

This is where zero-copy matters most. The classic codec's fory::deserialize
allocates a fresh Vec<u8> for the body as part of the String's heap storage,
then the tarpc layer may clone it again. The zerocopy codec calls
BytesMut::split_off + freeze(), producing a Bytes that aliases the socket
receive buffer with zero allocation. On 4 MiB bodies this saves one 4 MiB
allocation and copy per message.

A 65% throughput improvement translates directly to reduced CPU time and
reduced memory bandwidth consumption in bulk-transfer workloads (e.g.
cloudverve S3 GET responses, NVMe-oF read paths).

### roundtrip_small (±1%)

No regression on small/empty-body workloads. The framing overhead of the
zerocopy codec (4-byte body_len suffix) is negligible relative to loopback
TCP latency. Both codecs converge to the same ~102 µs RTT, well within ±5%.
