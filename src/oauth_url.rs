//! Pure OAuth URL, IP/SSRF, and callback-path validation helpers.
use anyhow::{Result, bail};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

const MAX_CALLBACK_PATH_BYTES: usize = 256;

pub fn parse_url_host_ip(host: &str) -> Option<IpAddr> {
    if let Some(v6_str) = host.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
        v6_str.parse::<Ipv6Addr>().ok().map(IpAddr::V6)
    } else {
        host.parse::<Ipv4Addr>().ok().map(IpAddr::V4)
    }
}

fn is_loopback_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_loopback(),
        IpAddr::V6(v6) => {
            v6.is_loopback() || v6.to_ipv4_mapped().is_some_and(|v4| v4.is_loopback())
        }
    }
}

fn is_non_public_ipv4(v4: Ipv4Addr) -> bool {
    let octets = v4.octets();
    v4.is_loopback()
        || v4.is_private()
        || v4.is_link_local()
        || v4.is_unspecified()
        || v4.is_broadcast()
        || v4.is_multicast()
        || octets[0] == 0
        || (octets[0] == 100 && (octets[1] & 0b1100_0000) == 64)
        || (octets[0] == 192 && octets[1] == 0 && octets[2] == 0)
        || (octets[0] == 198 && (octets[1] & 0xfe) == 18)
        || octets[0] >= 240
}

pub fn is_disallowed_remote_oauth_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_non_public_ipv4(v4),
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_non_public_ipv4(v4);
            }
            let seg0 = v6.segments()[0];
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || (seg0 & 0xfe00) == 0xfc00
                || (seg0 & 0xffc0) == 0xfe80
        }
    }
}

pub fn is_loopback_literal_host(host: Option<&str>) -> bool {
    let Some(h) = host else {
        return false;
    };
    if let Some(ip) = parse_url_host_ip(h) {
        return is_loopback_ip(ip);
    }
    let trimmed = h.trim_end_matches('.');
    trimmed.eq_ignore_ascii_case("localhost")
        || trimmed.to_ascii_lowercase().ends_with(".localhost")
}

fn is_disallowed_remote_oauth_host(host: &str) -> bool {
    if let Some(ip) = parse_url_host_ip(host) {
        return is_disallowed_remote_oauth_ip(ip);
    }
    let lower = host.trim_end_matches('.').to_ascii_lowercase();
    lower == "localhost"
        || lower.ends_with(".localhost")
        || lower == "localhost.localdomain"
        || lower == "ip6-localhost"
        || lower == "ip6-loopback"
        || lower.ends_with(".local")
        || lower == "metadata"
        || lower == "metadata.google.internal"
        || lower == "instance-data"
}

/// Validate OAuth discovery, registration, authorization, token, or revocation URLs.
pub fn validate_oauth_endpoint_url(
    raw: &str,
    label: &str,
    allow_loopback: bool,
) -> Result<reqwest::Url> {
    let url = reqwest::Url::parse(raw).map_err(|_| anyhow::anyhow!("invalid {label} URL"))?;
    if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
        bail!("{label} URL must not include credentials or a fragment");
    }
    let Some(host) = url.host_str() else {
        bail!("invalid {label} URL");
    };
    if allow_loopback {
        match url.scheme() {
            "https" => Ok(url),
            "http" if is_loopback_literal_host(Some(host)) => Ok(url),
            _ => bail!(
                "{label} must use https:// or loopback http:// (127.0.0.0/8, [::1], or localhost)"
            ),
        }
    } else {
        if url.scheme() != "https" {
            bail!("{label} must use https:// for non-loopback MCP servers");
        }
        if is_disallowed_remote_oauth_host(host) {
            bail!(
                "{label} must not target loopback, private, or metadata addresses for non-loopback MCP servers"
            );
        }
        Ok(url)
    }
}

/// Validate a local OAuth callback path: leading `/`, printable ASCII without `?`, `#`,
/// whitespace, or control characters, and at most 256 bytes.
pub fn validate_callback_path(path: &str) -> Result<()> {
    if !path.starts_with('/') {
        bail!("callback_path must start with '/'");
    }
    if path.len() > MAX_CALLBACK_PATH_BYTES {
        bail!("callback_path must be at most {MAX_CALLBACK_PATH_BYTES} bytes");
    }
    for byte in path.bytes() {
        if matches!(byte, b'?' | b'#') {
            bail!("callback_path must not contain '?' or '#'");
        }
        if !(0x21..=0x7e).contains(&byte) {
            bail!(
                "callback_path must contain only printable ASCII characters without whitespace or control characters"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_literal_host_recognizes_ipv4_ipv6_and_localhost() {
        for host in [
            "127.0.0.1",
            "127.0.0.99",
            "[::1]",
            "[::ffff:127.0.0.1]",
            "localhost",
            "LOCALHOST.",
            "sub.localhost",
        ] {
            assert!(is_loopback_literal_host(Some(host)), "{host}");
        }
        for host in [
            None,
            Some(""),
            Some("example.com"),
            Some("192.168.1.1"),
            Some("[fe80::1]"),
            Some("localhost.example.com"),
        ] {
            assert!(!is_loopback_literal_host(host), "{host:?}");
        }
    }

    #[test]
    fn disallowed_remote_oauth_ip_blocks_non_public_and_allows_public() {
        for blocked in [
            "127.0.0.1",
            "10.0.0.1",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.169.254",
            "100.64.0.1",
            "::1",
            "::ffff:127.0.0.1",
            "fd00::1",
            "fe80::1",
        ] {
            let ip: IpAddr = blocked.parse().unwrap();
            assert!(is_disallowed_remote_oauth_ip(ip), "{blocked}");
        }
        for allowed in ["1.1.1.1", "8.8.8.8", "2606:4700:4700::1111"] {
            let ip: IpAddr = allowed.parse().unwrap();
            assert!(!is_disallowed_remote_oauth_ip(ip), "{allowed}");
        }
    }

    #[test]
    fn validate_callback_path_accepts_valid_paths() {
        for valid in [
            "/",
            "/callback",
            "/oauth/callback",
            "/v1/auth_cb-01.html",
            &format!("/{}", "a".repeat(MAX_CALLBACK_PATH_BYTES - 1)),
        ] {
            assert!(validate_callback_path(valid).is_ok(), "{valid}");
        }
    }

    #[test]
    fn validate_callback_path_rejects_invalid_paths() {
        let too_long = format!("/{}", "a".repeat(MAX_CALLBACK_PATH_BYTES));
        for invalid in [
            "",
            "callback",
            "https://127.0.0.1/callback",
            "/callback?code=1",
            "/callback#frag",
            "/call back",
            "/call\tback",
            "/call\nback",
            "/call\rback",
            "/call\x00back",
            "/call\x7fback",
            "/cb/日本語",
            &too_long,
        ] {
            assert!(validate_callback_path(invalid).is_err(), "{invalid:?}");
        }
    }
}
