//! Fory-friendly envelope types for the `serde_transport::fory` codec path.
//!
//! These mirror tarpc's native `ClientMessage<T>`, `Response<T>`, `Request<T>`,
//! and `ServerError` types. The native types are unsuitable for fory because
//! they reference `std::result::Result`, `std::time::Instant`, and
//! `std::io::ErrorKind` — none of which fory-core 0.17 provides
//! `fory::Serializer` impls for.
//!
//! Conversions between native and wrapper types are field-by-field and lossless
//! for the round-trip path the transport actually uses. Schema drift is
//! caught by the codec parity test (Task 3.5).
//!
//! ## Registration
//!
//! These types use manual `Serializer` implementations and must be registered
//! via `fory.register_serializer::<T>(id)` (NOT `fory.register::<T>(id)`).
//! Use [`register_envelope_types`] to register all envelope types at once.
//!
//! Using `register_serializer` instead of `register` sidesteps the fory-derive
//! compile-time `TYPE_ID_COUNTER` collision: `register` requires `StructSerializer`
//! which calls `fory_type_index()` — a per-crate counter that resets to 0 for
//! every compilation unit. `register_serializer` uses the `EXT` type path which
//! never consults `fory_type_index()`, so user-defined types (also starting at
//! index 0 in their crate) can coexist in the same `Fory` instance.
//!
//! ## `io::ErrorKind` wire encoding
//!
//! The u32 discriminants match tarpc's own serde encoding in `util/serde.rs`:
//!
//! | u32 | io::ErrorKind        |
//! |-----|----------------------|
//! |   0 | NotFound             |
//! |   1 | PermissionDenied     |
//! |   2 | ConnectionRefused    |
//! |   3 | ConnectionReset      |
//! |   4 | ConnectionAborted    |
//! |   5 | NotConnected         |
//! |   6 | AddrInUse            |
//! |   7 | AddrNotAvailable     |
//! |   8 | BrokenPipe           |
//! |   9 | AlreadyExists        |
//! |  10 | WouldBlock           |
//! |  11 | InvalidInput         |
//! |  12 | InvalidData          |
//! |  13 | TimedOut             |
//! |  14 | WriteZero            |
//! |  15 | Interrupted          |
//! |  16 | Other                |
//! |  17 | UnexpectedEof        |
//! |  _  | Other (fallback)     |
//!
//! ## Deadline encoding
//!
//! `context::Context.deadline` is a `std::time::Instant` (monotonic, local-only).
//! For the wire we encode it as the number of nanoseconds remaining from `Instant::now()`
//! at serialize time (i.e. `deadline.saturating_duration_since(now).as_nanos() as u64`).
//! A value of 0 means "already expired or no deadline". The receiver reconstructs
//! `Instant::now() + Duration::from_nanos(remaining_ns)`.

use std::any::Any;
use std::io;
use std::time::{Duration, Instant};

use fory::{Error, ForyDefault, ReadContext, Serializer, TypeResolver, WriteContext};
use fory_core::types::RefMode;

use crate::{ClientMessage, Request, Response, ServerError, context, trace};

// ---------------------------------------------------------------------------
// Wrapper types
// ---------------------------------------------------------------------------

/// Fory-serializable mirror of `trace::Context`.
#[derive(Debug, Clone)]
pub struct ForyTraceContext {
    /// Raw u128 bits of the `TraceId`.
    pub trace_id: u128,
    /// Raw u64 bits of the `SpanId`.
    pub span_id: u64,
    /// Sampling decision: 0 = Unsampled, 1 = Sampled.
    pub sampling: u8,
}

impl ForyDefault for ForyTraceContext {
    fn fory_default() -> Self {
        ForyTraceContext { trace_id: 0, span_id: 0, sampling: 0 }
    }
}

impl Serializer for ForyTraceContext {
    fn fory_write_data(&self, context: &mut WriteContext) -> Result<(), Error> {
        self.trace_id.fory_write(context, RefMode::None, false, false)?;
        self.span_id.fory_write(context, RefMode::None, false, false)?;
        self.sampling.fory_write(context, RefMode::None, false, false)?;
        Ok(())
    }

    fn fory_read_data(context: &mut ReadContext) -> Result<Self, Error>
    where
        Self: Sized + ForyDefault,
    {
        let trace_id = u128::fory_read(context, RefMode::None, false)?;
        let span_id = u64::fory_read(context, RefMode::None, false)?;
        let sampling = u8::fory_read(context, RefMode::None, false)?;
        Ok(ForyTraceContext { trace_id, span_id, sampling })
    }

    fn fory_type_id_dyn(&self, type_resolver: &TypeResolver) -> Result<fory::TypeId, Error> {
        Self::fory_get_type_id(type_resolver)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Fory-serializable mirror of `ServerError`.
///
/// `kind` is encoded as a u32 per the table in the module-level docs.
#[derive(Debug, Clone)]
pub struct ForyServerError {
    /// io::ErrorKind discriminant value (see module docs for mapping).
    pub kind: u32,
    /// Human-readable error detail.
    pub detail: String,
}

impl ForyDefault for ForyServerError {
    fn fory_default() -> Self {
        ForyServerError { kind: 0, detail: String::new() }
    }
}

impl Serializer for ForyServerError {
    fn fory_write_data(&self, context: &mut WriteContext) -> Result<(), Error> {
        self.kind.fory_write(context, RefMode::None, false, false)?;
        self.detail.fory_write(context, RefMode::None, false, false)?;
        Ok(())
    }

    fn fory_read_data(context: &mut ReadContext) -> Result<Self, Error>
    where
        Self: Sized + ForyDefault,
    {
        let kind = u32::fory_read(context, RefMode::None, false)?;
        let detail = String::fory_read(context, RefMode::None, false)?;
        Ok(ForyServerError { kind, detail })
    }

    fn fory_type_id_dyn(&self, type_resolver: &TypeResolver) -> Result<fory::TypeId, Error> {
        Self::fory_get_type_id(type_resolver)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Fory-serializable analog of `Result<T, ServerError>`.
///
/// fory-core 0.17 has no `Serializer` impl for `std::result::Result`, so we
/// define a dedicated two-variant enum.
///
/// Wire encoding: u8 discriminant (0 = Ok, 1 = Err) followed by the payload.
#[derive(Debug, Clone)]
pub enum ForyResult<T: Serializer + ForyDefault + 'static> {
    /// Successful response payload.
    Ok(T),
    /// Server-side error.
    Err(ForyServerError),
}

impl<T: Serializer + ForyDefault + 'static> ForyDefault for ForyResult<T> {
    fn fory_default() -> Self {
        ForyResult::Ok(T::fory_default())
    }
}

impl<T: Serializer + ForyDefault + 'static> Serializer for ForyResult<T> {
    fn fory_write_data(&self, context: &mut WriteContext) -> Result<(), Error> {
        match self {
            ForyResult::Ok(v) => {
                context.writer.write_u8(0);
                v.fory_write(context, RefMode::None, false, false)?;
            }
            ForyResult::Err(e) => {
                context.writer.write_u8(1);
                e.fory_write(context, RefMode::None, false, false)?;
            }
        }
        Ok(())
    }

    fn fory_read_data(context: &mut ReadContext) -> Result<Self, Error>
    where
        Self: Sized + ForyDefault,
    {
        let discriminant = context.reader.read_u8()?;
        match discriminant {
            0 => {
                let v = T::fory_read(context, RefMode::None, false)?;
                Ok(ForyResult::Ok(v))
            }
            1 => {
                let e = ForyServerError::fory_read(context, RefMode::None, false)?;
                Ok(ForyResult::Err(e))
            }
            _ => Err(Error::invalid_data(format!(
                "ForyResult: unknown discriminant {}",
                discriminant
            ))),
        }
    }

    fn fory_type_id_dyn(&self, type_resolver: &TypeResolver) -> Result<fory::TypeId, Error> {
        Self::fory_get_type_id(type_resolver)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Fory-serializable mirror of `Request<T>`.
///
/// `context::Context.deadline` (an `Instant`) is replaced by `deadline_ns`:
/// nanoseconds remaining from the point of serialization. See module docs.
#[derive(Debug, Clone)]
pub struct ForyRequest<T: Serializer + ForyDefault + 'static> {
    /// Request ID — unique within a single channel.
    pub id: u64,
    /// Trace context (mirrors `context.trace_context`).
    pub trace: ForyTraceContext,
    /// Deadline as nanoseconds remaining (from serialize time).  0 = expired / no deadline.
    pub deadline_ns: u64,
    /// Request body.
    pub message: T,
}

impl<T: Serializer + ForyDefault + 'static> ForyDefault for ForyRequest<T> {
    fn fory_default() -> Self {
        ForyRequest {
            id: 0,
            trace: ForyTraceContext::fory_default(),
            deadline_ns: 0,
            message: T::fory_default(),
        }
    }
}

impl<T: Serializer + ForyDefault + 'static> Serializer for ForyRequest<T> {
    fn fory_write_data(&self, context: &mut WriteContext) -> Result<(), Error> {
        self.id.fory_write(context, RefMode::None, false, false)?;
        self.trace.fory_write(context, RefMode::None, false, false)?;
        self.deadline_ns.fory_write(context, RefMode::None, false, false)?;
        self.message.fory_write(context, RefMode::None, false, false)?;
        Ok(())
    }

    fn fory_read_data(context: &mut ReadContext) -> Result<Self, Error>
    where
        Self: Sized + ForyDefault,
    {
        let id = u64::fory_read(context, RefMode::None, false)?;
        let trace = ForyTraceContext::fory_read(context, RefMode::None, false)?;
        let deadline_ns = u64::fory_read(context, RefMode::None, false)?;
        let message = T::fory_read(context, RefMode::None, false)?;
        Ok(ForyRequest { id, trace, deadline_ns, message })
    }

    fn fory_type_id_dyn(&self, type_resolver: &TypeResolver) -> Result<fory::TypeId, Error> {
        Self::fory_get_type_id(type_resolver)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Fory-serializable mirror of `Response<T>`.
#[derive(Debug, Clone)]
pub struct ForyResponse<T: Serializer + ForyDefault + 'static> {
    /// ID of the request this is responding to.
    pub request_id: u64,
    /// Response body or server error.
    pub message: ForyResult<T>,
}

impl<T: Serializer + ForyDefault + 'static> ForyDefault for ForyResponse<T> {
    fn fory_default() -> Self {
        ForyResponse {
            request_id: 0,
            message: ForyResult::fory_default(),
        }
    }
}

impl<T: Serializer + ForyDefault + 'static> Serializer for ForyResponse<T> {
    fn fory_write_data(&self, context: &mut WriteContext) -> Result<(), Error> {
        self.request_id.fory_write(context, RefMode::None, false, false)?;
        self.message.fory_write(context, RefMode::None, false, false)?;
        Ok(())
    }

    fn fory_read_data(context: &mut ReadContext) -> Result<Self, Error>
    where
        Self: Sized + ForyDefault,
    {
        let request_id = u64::fory_read(context, RefMode::None, false)?;
        let message = ForyResult::<T>::fory_read(context, RefMode::None, false)?;
        Ok(ForyResponse { request_id, message })
    }

    fn fory_type_id_dyn(&self, type_resolver: &TypeResolver) -> Result<fory::TypeId, Error> {
        Self::fory_get_type_id(type_resolver)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Fory-serializable mirror of `ClientMessage<T>`.
///
/// Wire encoding: u8 discriminant (0 = Request, 1 = Cancel) followed by payload.
#[derive(Debug, Clone)]
pub enum ForyClientMessage<T: Serializer + ForyDefault + 'static> {
    /// A new request from the client.
    Request(ForyRequest<T>),
    /// Cancellation of an in-flight request.
    Cancel {
        /// Trace context associated with the original request.
        trace: ForyTraceContext,
        /// ID of the request to cancel.
        request_id: u64,
    },
}

impl<T: Serializer + ForyDefault + 'static> ForyDefault for ForyClientMessage<T> {
    fn fory_default() -> Self {
        ForyClientMessage::Request(ForyRequest::fory_default())
    }
}

impl<T: Serializer + ForyDefault + 'static> Serializer for ForyClientMessage<T> {
    fn fory_write_data(&self, context: &mut WriteContext) -> Result<(), Error> {
        match self {
            ForyClientMessage::Request(req) => {
                context.writer.write_u8(0);
                req.fory_write(context, RefMode::None, false, false)?;
            }
            ForyClientMessage::Cancel { trace, request_id } => {
                context.writer.write_u8(1);
                trace.fory_write(context, RefMode::None, false, false)?;
                request_id.fory_write(context, RefMode::None, false, false)?;
            }
        }
        Ok(())
    }

    fn fory_read_data(context: &mut ReadContext) -> Result<Self, Error>
    where
        Self: Sized + ForyDefault,
    {
        let discriminant = context.reader.read_u8()?;
        match discriminant {
            0 => {
                let req = ForyRequest::<T>::fory_read(context, RefMode::None, false)?;
                Ok(ForyClientMessage::Request(req))
            }
            1 => {
                let trace = ForyTraceContext::fory_read(context, RefMode::None, false)?;
                let request_id = u64::fory_read(context, RefMode::None, false)?;
                Ok(ForyClientMessage::Cancel { trace, request_id })
            }
            _ => Err(Error::invalid_data(format!(
                "ForyClientMessage: unknown discriminant {}",
                discriminant
            ))),
        }
    }

    fn fory_type_id_dyn(&self, type_resolver: &TypeResolver) -> Result<fory::TypeId, Error> {
        Self::fory_get_type_id(type_resolver)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// ---------------------------------------------------------------------------
// Registration helper
// ---------------------------------------------------------------------------

/// Register all envelope wrapper types in a `Fory` instance.
///
/// This uses `register_serializer` (EXT type path) rather than `register`
/// (STRUCT type path), which avoids the fory-derive `TYPE_ID_COUNTER` collision
/// between independently compiled crates. See the module-level documentation.
///
/// `T` is the user's request/response type. Call this helper once per `T`:
///
/// ```rust,ignore
/// let mut fory = Fory::default();
/// register_envelope_types::<HelloRequest>(&mut fory).unwrap();
/// register_envelope_types::<HelloResponse>(&mut fory).unwrap();
/// fory.register_serializer::<HelloRequest>(100).unwrap();
/// fory.register_serializer::<HelloResponse>(101).unwrap();
/// ```
///
/// The IDs 2–7 are reserved for the tarpc envelope types. User types must
/// use IDs >= 100 (or any value that does not conflict).
///
/// Note: if the server sends `ForyClientMessage<Req>` and `ForyResponse<Resp>`,
/// you need to call this for both `Req` and `Resp` when both are user-defined
/// types. If using built-in types like `String` or `u32`, only call it for the
/// user-defined type (built-ins are pre-registered).
pub fn register_envelope_types<T>(fory: &mut fory::Fory) -> Result<(), fory::Error>
where
    T: Serializer + ForyDefault + Send + 'static,
{
    fory.register_serializer::<ForyTraceContext>(2)?;
    fory.register_serializer::<ForyServerError>(3)?;
    fory.register_serializer::<ForyResult<T>>(4)?;
    fory.register_serializer::<ForyRequest<T>>(5)?;
    fory.register_serializer::<ForyResponse<T>>(6)?;
    fory.register_serializer::<ForyClientMessage<T>>(7)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// FNV-1a hash helper for stable wire IDs
// ---------------------------------------------------------------------------

/// Compute a stable 32-bit wire ID from a fully-qualified type name using FNV-1a.
///
/// The high bit is always set to avoid collision with small user-supplied IDs (< 2^31).
/// IDs 2–9 are reserved for tarpc envelope types, so setting the high bit is safe.
///
/// This is a pure `const fn` — zero runtime cost.
pub const fn fory_wire_id(name: &str) -> u32 {
    const FNV_PRIME: u32 = 16777619;
    const FNV_OFFSET: u32 = 2166136261;
    let bytes = name.as_bytes();
    let mut hash = FNV_OFFSET;
    let mut i = 0;
    while i < bytes.len() {
        hash ^= bytes[i] as u32;
        hash = hash.wrapping_mul(FNV_PRIME);
        i += 1;
    }
    // Set high bit to avoid collisions with small manually-assigned IDs (envelope IDs 2-9).
    hash | 0x8000_0000
}

// ---------------------------------------------------------------------------
// ServiceWireSchema trait
// ---------------------------------------------------------------------------

/// Per-service wire schema — proc-macro emits one impl per `#[tarpc::service]` trait.
///
/// Implementors are the generated `XxxService` marker structs (emitted alongside the
/// `Xxx` trait by the proc-macro). The `register` function auto-registers all types
/// this service transports: tarpc envelope wrappers, the generated `XxxRequest` /
/// `XxxResponse` enums, and any user types referenced in method signatures.
///
/// Use [`tarpc::serde_transport::fory::connect`] and
/// [`tarpc::serde_transport::fory::listen`] which call `S::register` automatically
/// before creating the transport.
pub trait ServiceWireSchema: Sized + 'static {
    /// The generated request enum.
    type Req: Serializer + ForyDefault + Send + 'static;
    /// The generated response enum.
    type Resp: Serializer + ForyDefault + Send + 'static;

    /// Register all types this service transports.
    ///
    /// This is called automatically by [`connect`] and [`listen`]. You only need
    /// to call this directly if building a [`Fory`] instance manually.
    ///
    /// [`connect`]: crate::serde_transport::fory::connect
    /// [`listen`]: crate::serde_transport::fory::listen
    /// [`Fory`]: fory::Fory
    fn register(fory: &mut fory::Fory) -> Result<(), fory::Error>;
}

// ---------------------------------------------------------------------------
// Conversions: native → wrapper
// ---------------------------------------------------------------------------

impl From<trace::SamplingDecision> for u8 {
    fn from(d: trace::SamplingDecision) -> u8 {
        match d {
            trace::SamplingDecision::Sampled => 1,
            trace::SamplingDecision::Unsampled => 0,
        }
    }
}

impl From<u8> for trace::SamplingDecision {
    fn from(v: u8) -> trace::SamplingDecision {
        if v == 1 {
            trace::SamplingDecision::Sampled
        } else {
            trace::SamplingDecision::Unsampled
        }
    }
}

impl From<&trace::Context> for ForyTraceContext {
    fn from(tc: &trace::Context) -> Self {
        ForyTraceContext {
            trace_id: u128::from(tc.trace_id),
            span_id: u64::from(tc.span_id),
            sampling: u8::from(tc.sampling_decision),
        }
    }
}

impl From<ForyTraceContext> for trace::Context {
    fn from(ftc: ForyTraceContext) -> Self {
        trace::Context {
            trace_id: trace::TraceId::from(ftc.trace_id),
            span_id: trace::SpanId::from(ftc.span_id),
            sampling_decision: trace::SamplingDecision::from(ftc.sampling),
        }
    }
}

/// Encode `io::ErrorKind` as u32 using the same table as `util::serde`.
pub fn error_kind_to_u32(kind: io::ErrorKind) -> u32 {
    use io::ErrorKind::*;
    match kind {
        NotFound => 0,
        PermissionDenied => 1,
        ConnectionRefused => 2,
        ConnectionReset => 3,
        ConnectionAborted => 4,
        NotConnected => 5,
        AddrInUse => 6,
        AddrNotAvailable => 7,
        BrokenPipe => 8,
        AlreadyExists => 9,
        WouldBlock => 10,
        InvalidInput => 11,
        InvalidData => 12,
        TimedOut => 13,
        WriteZero => 14,
        Interrupted => 15,
        Other => 16,
        UnexpectedEof => 17,
        _ => 16, // map unknown variants to Other
    }
}

/// Decode u32 back to `io::ErrorKind`.
pub fn u32_to_error_kind(v: u32) -> io::ErrorKind {
    use io::ErrorKind::*;
    match v {
        0 => NotFound,
        1 => PermissionDenied,
        2 => ConnectionRefused,
        3 => ConnectionReset,
        4 => ConnectionAborted,
        5 => NotConnected,
        6 => AddrInUse,
        7 => AddrNotAvailable,
        8 => BrokenPipe,
        9 => AlreadyExists,
        10 => WouldBlock,
        11 => InvalidInput,
        12 => InvalidData,
        13 => TimedOut,
        14 => WriteZero,
        15 => Interrupted,
        16 => Other,
        17 => UnexpectedEof,
        _ => Other,
    }
}

impl From<&ServerError> for ForyServerError {
    fn from(se: &ServerError) -> Self {
        ForyServerError {
            kind: error_kind_to_u32(se.kind),
            detail: se.detail.clone(),
        }
    }
}

impl From<ForyServerError> for ServerError {
    fn from(fse: ForyServerError) -> Self {
        ServerError {
            kind: u32_to_error_kind(fse.kind),
            detail: fse.detail,
        }
    }
}

impl<T: Serializer + ForyDefault + Clone + 'static> From<&Request<T>> for ForyRequest<T> {
    fn from(req: &Request<T>) -> Self {
        let now = Instant::now();
        let deadline_ns = req
            .context
            .deadline
            .checked_duration_since(now)
            .map(|d| d.as_nanos().min(u64::MAX as u128) as u64)
            .unwrap_or(0);
        ForyRequest {
            id: req.id,
            trace: ForyTraceContext::from(&req.context.trace_context),
            deadline_ns,
            message: req.message.clone(),
        }
    }
}

impl<T: Serializer + ForyDefault + 'static> From<ForyRequest<T>> for Request<T> {
    fn from(fr: ForyRequest<T>) -> Self {
        let deadline = Instant::now() + Duration::from_nanos(fr.deadline_ns);
        Request {
            context: context::Context {
                deadline,
                trace_context: trace::Context::from(fr.trace),
            },
            id: fr.id,
            message: fr.message,
        }
    }
}

impl<T: Serializer + ForyDefault + 'static> From<Result<T, ServerError>> for ForyResult<T> {
    fn from(r: Result<T, ServerError>) -> Self {
        match r {
            Ok(v) => ForyResult::Ok(v),
            Err(e) => ForyResult::Err(ForyServerError::from(&e)),
        }
    }
}

impl<T: Serializer + ForyDefault + 'static> From<ForyResult<T>> for Result<T, ServerError> {
    fn from(fr: ForyResult<T>) -> Self {
        match fr {
            ForyResult::Ok(v) => Ok(v),
            ForyResult::Err(e) => Err(ServerError::from(e)),
        }
    }
}

impl<T: Serializer + ForyDefault + 'static> From<&Response<T>> for ForyResponse<T>
where
    T: Clone,
{
    fn from(resp: &Response<T>) -> Self {
        ForyResponse {
            request_id: resp.request_id,
            message: ForyResult::from(resp.message.clone()),
        }
    }
}

impl<T: Serializer + ForyDefault + 'static> From<ForyResponse<T>> for Response<T> {
    fn from(fr: ForyResponse<T>) -> Self {
        Response {
            request_id: fr.request_id,
            message: Result::from(fr.message),
        }
    }
}

impl<T: Serializer + ForyDefault + Clone + 'static> From<&ClientMessage<T>>
    for ForyClientMessage<T>
{
    fn from(msg: &ClientMessage<T>) -> Self {
        match msg {
            ClientMessage::Request(req) => {
                ForyClientMessage::Request(ForyRequest::from(req))
            }
            ClientMessage::Cancel {
                trace_context,
                request_id,
            } => ForyClientMessage::Cancel {
                trace: ForyTraceContext::from(trace_context),
                request_id: *request_id,
            },
        }
    }
}

impl<T: Serializer + ForyDefault + 'static> From<ForyClientMessage<T>> for ClientMessage<T> {
    fn from(fm: ForyClientMessage<T>) -> Self {
        match fm {
            ForyClientMessage::Request(fr) => ClientMessage::Request(Request::from(fr)),
            ForyClientMessage::Cancel { trace, request_id } => ClientMessage::Cancel {
                trace_context: trace::Context::from(trace),
                request_id,
            },
        }
    }
}
