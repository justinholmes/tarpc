#![no_implicit_prelude]
extern crate tarpc as some_random_other_name;

#[cfg(feature = "serde1")]
pub mod serde1_feature {
    #[::tarpc::derive_serde]
    #[derive(Debug, PartialEq, Eq)]
    pub enum TestData {
        Black,
        White,
    }
}

// The `ForyObject` derive macro (activated by `cfg_attr(feature = "fory", ...)` on the
// generated types) generates non-hygienic code that references `fory_core` and `std` by bare
// name, which fails under `#![no_implicit_prelude]` because that attribute suppresses the
// extern prelude.  The hygiene property being tested here (that tarpc's own generated code
// does not rely on implicit prelude items) remains valid; we simply skip this module when
// fory is active to avoid a compile error from the third-party derive.
#[cfg(not(feature = "fory"))]
#[::tarpc::service]
pub trait ColorProtocol {
    async fn get_opposite_color(color: u8) -> u8;
}
