use std::fs::File;
use std::io::{self, Write};
use std::path::Path;
use tracing::info;
use rcgen::{generate_simple_self_signed, CertifiedKey};

/// Ensures that TLS certificates exist at the specified paths.
/// If they do not, it generates a new self-signed certificate valid for localhost,
/// 127.0.0.1, 10.0.1.2, and host.wokwi.internal, and saves it to disk.
pub fn ensure_certificates(cert_path: &Path, key_path: &Path) -> io::Result<()> {
    if cert_path.exists() && key_path.exists() {
        return Ok(());
    }

    info!("TLS certificates not found. Generating programmatic self-signed certificates...");

    let subject_alt_names = vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
        "10.0.1.2".to_string(),
        "host.wokwi.internal".to_string(),
    ];

    let CertifiedKey { cert, key_pair } = generate_simple_self_signed(subject_alt_names)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("rcgen error: {:?}", e)))?;

    let cert_pem = cert.pem();
    let key_pem = key_pair.serialize_pem();

    // Create directories if they don't exist
    if let Some(parent) = cert_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if let Some(parent) = key_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Write certificate file
    let mut cert_file = File::create(cert_path)?;
    cert_file.write_all(cert_pem.as_bytes())?;

    // Write private key file
    let mut key_file = File::create(key_path)?;
    key_file.write_all(key_pem.as_bytes())?;

    info!("Successfully generated self-signed certificate at: {:?}", cert_path);
    info!("Successfully generated private key at: {:?}", key_path);

    Ok(())
}
