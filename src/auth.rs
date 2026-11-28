//! OAuth 2.1 PKCE client flow, RFC 9728/8414 discovery, `WWW-Authenticate` diagnostics,
//! private credential persistence, and singleflight refresh rotation.

mod callback;
mod challenge;
mod discovery;
mod manager;
mod store;

#[cfg(test)]
mod tests;

pub use challenge::{
    AuthChallengeInfo, BearerChallenge, canonical_resource_url, credential_storage_key,
    parse_www_authenticate,
};
pub use manager::{HttpAuthManager, LoginOptions, login, logout, status};
pub use store::StoredCredential;
