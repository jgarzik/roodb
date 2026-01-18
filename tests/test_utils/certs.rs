//! TLS certificate generation for tests using openssl

use std::path::PathBuf;
use std::process::Command;
use std::sync::Once;

use roodb::tls::{RaftTlsConfig, TlsConfig};
use tempfile::TempDir;

static CRYPTO_INIT: Once = Once::new();

fn init_crypto() {
    CRYPTO_INIT.call_once(|| {
        rustls::crypto::ring::default_provider()
            .install_default()
            .expect("Failed to install rustls crypto provider");
    });
}

/// Run openssl command, panic on failure
fn openssl(args: &[&str]) {
    let status = Command::new("openssl")
        .args(args)
        .status()
        .expect("Failed to run openssl");
    assert!(status.success(), "openssl {:?} failed", args);
}

/// Generate self-signed certificate using openssl
fn generate_self_signed_cert(cert_path: &std::path::Path, key_path: &std::path::Path) {
    openssl(&[
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-keyout",
        key_path.to_str().unwrap(),
        "-out",
        cert_path.to_str().unwrap(),
        "-days",
        "1",
        "-nodes",
        "-subj",
        "/CN=localhost",
        "-addext",
        "subjectAltName=DNS:localhost,IP:127.0.0.1",
    ]);
}

/// Generate CA certificate using openssl
fn generate_ca_cert_files(ca_key_path: &std::path::Path, ca_cert_path: &std::path::Path) {
    openssl(&[
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-keyout",
        ca_key_path.to_str().unwrap(),
        "-out",
        ca_cert_path.to_str().unwrap(),
        "-days",
        "1",
        "-nodes",
        "-subj",
        "/CN=RooDB Test CA",
    ]);
}

/// Generate node certificate signed by CA using openssl
fn generate_signed_node_cert(
    node_id: u64,
    ca_cert_path: &std::path::Path,
    ca_key_path: &std::path::Path,
    node_cert_path: &std::path::Path,
    node_key_path: &std::path::Path,
    ext_file_path: &std::path::Path,
) {
    let csr_path = node_cert_path.with_extension("csr");

    // Generate key and CSR
    openssl(&[
        "req",
        "-newkey",
        "rsa:2048",
        "-keyout",
        node_key_path.to_str().unwrap(),
        "-out",
        csr_path.to_str().unwrap(),
        "-nodes",
        "-subj",
        &format!("/CN=RooDB Node {}", node_id),
    ]);

    // Write extension file for SAN
    std::fs::write(ext_file_path, "subjectAltName=DNS:localhost,IP:127.0.0.1")
        .expect("Failed to write ext file");

    // Sign with CA
    openssl(&[
        "x509",
        "-req",
        "-in",
        csr_path.to_str().unwrap(),
        "-CA",
        ca_cert_path.to_str().unwrap(),
        "-CAkey",
        ca_key_path.to_str().unwrap(),
        "-CAcreateserial",
        "-out",
        node_cert_path.to_str().unwrap(),
        "-days",
        "1",
        "-extfile",
        ext_file_path.to_str().unwrap(),
    ]);

    // Cleanup CSR
    let _ = std::fs::remove_file(&csr_path);
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
    let cert_dir = TempDir::new().expect("Failed to create cert temp dir");
    let cert_path = cert_dir.path().join("server.crt");
    let key_path = cert_dir.path().join("server.key");

    generate_self_signed_cert(&cert_path, &key_path);

    TlsCertFiles {
        cert_path,
        key_path,
        _cert_dir: cert_dir,
    }
}

/// Create TLS config for testing (client connections, no mTLS)
pub fn test_tls_config() -> TlsConfig {
    init_crypto();
    let files = write_test_cert_files();
    let cert_pem = std::fs::read(&files.cert_path).unwrap();
    let key_pem = std::fs::read(&files.key_path).unwrap();
    TlsConfig::from_pem(&cert_pem, &key_pem).unwrap()
}

// =============================================================================
// Raft mTLS Certificate Generation
// =============================================================================

/// Raft cluster certificate files with CA and per-node certs
pub struct RaftCertFiles {
    pub ca_cert_path: PathBuf,
    pub node1_cert_path: PathBuf,
    pub node1_key_path: PathBuf,
    pub node2_cert_path: PathBuf,
    pub node2_key_path: PathBuf,
    pub node3_cert_path: PathBuf,
    pub node3_key_path: PathBuf,
    pub _cert_dir: TempDir,
}

/// Write Raft cluster certificates (CA + 3 node certs) to temporary files
pub fn write_raft_cluster_certs() -> RaftCertFiles {
    init_crypto();
    let cert_dir = TempDir::new().expect("Failed to create cert temp dir");

    let ca_cert_path = cert_dir.path().join("ca.crt");
    let ca_key_path = cert_dir.path().join("ca.key");
    let ext_file_path = cert_dir.path().join("ext.cnf");

    // Generate CA
    generate_ca_cert_files(&ca_key_path, &ca_cert_path);

    // Generate node certs signed by CA
    let node1_cert_path = cert_dir.path().join("node1.crt");
    let node1_key_path = cert_dir.path().join("node1.key");
    generate_signed_node_cert(
        1,
        &ca_cert_path,
        &ca_key_path,
        &node1_cert_path,
        &node1_key_path,
        &ext_file_path,
    );

    let node2_cert_path = cert_dir.path().join("node2.crt");
    let node2_key_path = cert_dir.path().join("node2.key");
    generate_signed_node_cert(
        2,
        &ca_cert_path,
        &ca_key_path,
        &node2_cert_path,
        &node2_key_path,
        &ext_file_path,
    );

    let node3_cert_path = cert_dir.path().join("node3.crt");
    let node3_key_path = cert_dir.path().join("node3.key");
    generate_signed_node_cert(
        3,
        &ca_cert_path,
        &ca_key_path,
        &node3_cert_path,
        &node3_key_path,
        &ext_file_path,
    );

    RaftCertFiles {
        ca_cert_path,
        node1_cert_path,
        node1_key_path,
        node2_cert_path,
        node2_key_path,
        node3_cert_path,
        node3_key_path,
        _cert_dir: cert_dir,
    }
}

/// Create RaftTlsConfig for single-node testing
pub fn test_raft_tls_config() -> RaftTlsConfig {
    init_crypto();
    let cert_dir = TempDir::new().expect("Failed to create cert temp dir");

    let ca_cert_path = cert_dir.path().join("ca.crt");
    let ca_key_path = cert_dir.path().join("ca.key");
    let node_cert_path = cert_dir.path().join("node.crt");
    let node_key_path = cert_dir.path().join("node.key");
    let ext_file_path = cert_dir.path().join("ext.cnf");

    generate_ca_cert_files(&ca_key_path, &ca_cert_path);
    generate_signed_node_cert(
        1,
        &ca_cert_path,
        &ca_key_path,
        &node_cert_path,
        &node_key_path,
        &ext_file_path,
    );

    let ca_pem = std::fs::read(&ca_cert_path).unwrap();
    let cert_pem = std::fs::read(&node_cert_path).unwrap();
    let key_pem = std::fs::read(&node_key_path).unwrap();

    RaftTlsConfig::from_pem_with_ca(&cert_pem, &key_pem, &ca_pem).unwrap()
}
