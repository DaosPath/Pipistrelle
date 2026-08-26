use rustls::crypto::CryptoProvider;
use rustls::crypto::aws_lc_rs::{default_provider, kx_group};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsProfile {
    /// Prefer X25519+ML-KEM-768 while retaining modern classical fallbacks.
    Hybrid,
    /// Require X25519+ML-KEM-768; clients without PQC support cannot connect.
    PqcStrict,
    /// Classical TLS 1.3 key exchange only, useful for compatibility and benchmarks.
    Classical,
}

impl TlsProfile {
    pub fn from_env(var_name: &str, default: Self) -> Self {
        let Ok(value) = std::env::var(var_name) else {
            return default;
        };

        match value.trim().to_ascii_lowercase().as_str() {
            "hybrid" | "pqc-hybrid" | "post-quantum" => Self::Hybrid,
            "pqc-strict" | "strict" | "pq-strict" => Self::PqcStrict,
            "classical" | "classic" | "pre-quantum" | "prequantum" => Self::Classical,
            _ => default,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Hybrid => "hybrid",
            Self::PqcStrict => "pqc-strict",
            Self::Classical => "classical",
        }
    }

    pub const fn description(self) -> &'static str {
        match self {
            Self::Hybrid => "X25519MLKEM768 preferred with X25519/P-256/P-384 fallback",
            Self::PqcStrict => "X25519MLKEM768 required",
            Self::Classical => "X25519/P-256/P-384 only",
        }
    }
}

impl fmt::Display for TlsProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn provider(profile: TlsProfile) -> CryptoProvider {
    let mut provider = default_provider();
    provider.kx_groups = match profile {
        TlsProfile::Hybrid => vec![
            kx_group::X25519MLKEM768,
            kx_group::X25519,
            kx_group::SECP256R1,
            kx_group::SECP384R1,
        ],
        TlsProfile::PqcStrict => vec![kx_group::X25519MLKEM768],
        TlsProfile::Classical => vec![kx_group::X25519, kx_group::SECP256R1, kx_group::SECP384R1],
    };
    provider
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profiles_have_expected_group_counts() {
        assert_eq!(provider(TlsProfile::Hybrid).kx_groups.len(), 4);
        assert_eq!(provider(TlsProfile::PqcStrict).kx_groups.len(), 1);
        assert_eq!(provider(TlsProfile::Classical).kx_groups.len(), 3);
    }

    #[test]
    fn profile_names_are_stable() {
        assert_eq!(TlsProfile::Hybrid.as_str(), "hybrid");
        assert_eq!(TlsProfile::PqcStrict.as_str(), "pqc-strict");
        assert_eq!(TlsProfile::Classical.as_str(), "classical");
    }
}
