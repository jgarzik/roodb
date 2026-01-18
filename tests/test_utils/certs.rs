//! TLS certificate generation for tests

use std::path::PathBuf;
use std::sync::Once;

use rcgen::{generate_simple_self_signed, CertifiedKey};
use roodb::tls::TlsConfig;
use tempfile::TempDir;

static CRYPTO_INIT: Once = Once::new();

fn init_crypto() {
    CRYPTO_INIT.call_once(|| {
        rustls::crypto::ring::default_provider()
            .install_default()
            .expect("Failed to install rustls crypto provider");
    });
}

/// Generate self-signed certificates for testing
pub fn generate_test_certs() -> (Vec<u8>, Vec<u8>) {
    let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
    let CertifiedKey { cert, key_pair } = generate_simple_self_signed(subject_alt_names).unwrap();

    let cert_pem = cert.pem();
    let key_pem = key_pair.serialize_pem();

    (cert_pem.into_bytes(), key_pem.into_bytes())
}

/// Create TLS config for testing
pub fn test_tls_config() -> TlsConfig {
    init_crypto();
    let (cert_pem, key_pem) = generate_test_certs();
    TlsConfig::from_pem(&cert_pem, &key_pem).unwrap()
}

/// TLS certificate files written to disk
pub struct TlsCertFiles {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub _cert_dir: TempDir,
}

/// Write test certificates to temporary files
pub fn write_test_cert_files() -> TlsCertFiles {
    init_crypto();
    let (cert_pem, key_pem) = generate_test_certs();

    let cert_dir = TempDir::new().expect("Failed to create cert temp dir");
    let cert_path = cert_dir.path().join("server.crt");
    let key_path = cert_dir.path().join("server.key");

    std::fs::write(&cert_path, &cert_pem).expect("Failed to write cert file");
    std::fs::write(&key_path, &key_pem).expect("Failed to write key file");

    TlsCertFiles {
        cert_path,
        key_path,
        _cert_dir: cert_dir,
    }
}
