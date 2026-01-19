//! TLS configuration for RooDB
//!
//! All network communication requires TLS - no plaintext allowed.

use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::CertificateDer;
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use thiserror::Error;
use tracing::warn;

#[derive(Error, Debug)]
pub enum TlsError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("TLS error: {0}")]
    Tls(#[from] rustls::Error),
    #[error("No certificates found in file")]
    NoCertificates,
    #[error("No private key found in file")]
    NoPrivateKey,
}

/// TLS configuration holder
#[derive(Clone)]
pub struct TlsConfig {
    server_config: Arc<ServerConfig>,
    client_config: Arc<ClientConfig>,
}

impl TlsConfig {
    /// Create TLS config from certificate and key files
    pub async fn from_files(cert_path: &Path, key_path: &Path) -> Result<Self, TlsError> {
        let cert_data = tokio::fs::read(cert_path).await?;
        let key_data = tokio::fs::read(key_path).await?;

        Self::from_pem(&cert_data, &key_data)
    }

    /// Create TLS config from PEM-encoded data
    pub fn from_pem(cert_pem: &[u8], key_pem: &[u8]) -> Result<Self, TlsError> {
        // Parse certificates
        let certs: Vec<CertificateDer<'static>> =
            rustls_pemfile::certs(&mut BufReader::new(cert_pem))
                .filter_map(|r| r.ok())
                .collect();

        if certs.is_empty() {
            return Err(TlsError::NoCertificates);
        }

        // Parse private key
        let key = rustls_pemfile::private_key(&mut BufReader::new(key_pem))?
            .ok_or(TlsError::NoPrivateKey)?;

        // Build server config
        let server_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs.clone(), key.clone_key())?;

        // Build client config with the same cert as root CA (self-signed scenario)
        let mut root_store = RootCertStore::empty();
        for cert in &certs {
            if let Err(e) = root_store.add(cert.clone()) {
                warn!("Failed to add cert to root store: {}", e);
            }
        }

        let client_config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_client_auth_cert(certs, key)?;

        Ok(Self {
            server_config: Arc::new(server_config),
            client_config: Arc::new(client_config),
        })
    }

    /// Get the server TLS config
    pub fn server_config(&self) -> Arc<ServerConfig> {
        self.server_config.clone()
    }

    /// Get the client TLS config
    pub fn client_config(&self) -> Arc<ClientConfig> {
        self.client_config.clone()
    }
}

/// TLS configuration for Raft inter-node communication with mTLS
///
/// Unlike `TlsConfig` which uses no client authentication (for MySQL clients),
/// this config enforces mutual TLS: both server and client present certificates
/// signed by a trusted CA.
#[derive(Clone)]
pub struct RaftTlsConfig {
    server_config: Arc<ServerConfig>,
    client_config: Arc<ClientConfig>,
}

impl RaftTlsConfig {
    /// Create Raft TLS config from certificate, key, and CA files
    ///
    /// The CA certificate is used to verify peer certificates.
    /// The node's own certificate must be signed by the same CA.
    pub async fn from_files_with_ca(
        cert_path: &Path,
        key_path: &Path,
        ca_path: &Path,
    ) -> Result<Self, TlsError> {
        let cert_data = tokio::fs::read(cert_path).await?;
        let key_data = tokio::fs::read(key_path).await?;
        let ca_data = tokio::fs::read(ca_path).await?;

        Self::from_pem_with_ca(&cert_data, &key_data, &ca_data)
    }

    /// Create Raft TLS config from PEM-encoded data
    pub fn from_pem_with_ca(
        cert_pem: &[u8],
        key_pem: &[u8],
        ca_pem: &[u8],
    ) -> Result<Self, TlsError> {
        // Parse node certificate
        let certs: Vec<CertificateDer<'static>> =
            rustls_pemfile::certs(&mut BufReader::new(cert_pem))
                .filter_map(|r| r.ok())
                .collect();

        if certs.is_empty() {
            return Err(TlsError::NoCertificates);
        }

        // Parse private key
        let key = rustls_pemfile::private_key(&mut BufReader::new(key_pem))?
            .ok_or(TlsError::NoPrivateKey)?;

        // Parse CA certificate(s) for peer verification
        let ca_certs: Vec<CertificateDer<'static>> =
            rustls_pemfile::certs(&mut BufReader::new(ca_pem))
                .filter_map(|r| r.ok())
                .collect();

        if ca_certs.is_empty() {
            return Err(TlsError::NoCertificates);
        }

        // Build root store with CA cert(s)
        let mut root_store = RootCertStore::empty();
        for ca_cert in &ca_certs {
            if let Err(e) = root_store.add(ca_cert.clone()) {
                warn!("Failed to add CA cert to root store: {}", e);
            }
        }

        // Build client verifier that requires client cert signed by CA
        let client_verifier = WebPkiClientVerifier::builder(Arc::new(root_store.clone()))
            .build()
            .map_err(|e| TlsError::Tls(rustls::Error::General(e.to_string())))?;

        // Build server config with client cert verification
        let server_config = ServerConfig::builder()
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(certs.clone(), key.clone_key())?;

        // Build client config that presents our cert and verifies server cert
        let client_config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_client_auth_cert(certs, key)?;

        Ok(Self {
            server_config: Arc::new(server_config),
            client_config: Arc::new(client_config),
        })
    }

    /// Get the server TLS config (for accepting Raft RPC connections)
    pub fn server_config(&self) -> Arc<ServerConfig> {
        self.server_config.clone()
    }

    /// Get the client TLS config (for connecting to Raft peers)
    pub fn client_config(&self) -> Arc<ClientConfig> {
        self.client_config.clone()
    }
}
