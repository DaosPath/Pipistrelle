use bytes::{Buf, BytesMut};
use pipistrelle::crypto::{self, TlsProfile};
use rustls::ClientConfig;
use rustls_pki_types::ServerName;
use serde::Serialize;
use std::env;
use std::fs::File;
use std::io::{self, BufReader};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Notify, watch};
use tokio::task::JoinSet;
use tokio::time::timeout;
use tokio_rustls::TlsConnector;

trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T> AsyncStream for T where T: AsyncRead + AsyncWrite + Unpin + Send {}
type BoxStream = Box<dyn AsyncStream>;
type BenchError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Loopback,
    Ingest,
}

impl Mode {
    fn parse(value: &str) -> Result<Self, BenchError> {
        match value {
            "loopback" => Ok(Self::Loopback),
            "ingest" => Ok(Self::Ingest),
            other => Err(format!("unknown mode '{other}', expected loopback|ingest").into()),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Loopback => "loopback",
            Self::Ingest => "ingest",
        }
    }
}

#[derive(Clone, Debug)]
struct Args {
    host: String,
    port: u16,
    clients: usize,
    messages: u64,
    payload_bytes: usize,
    qos: u8,
    window: usize,
    mode: Mode,
    username: String,
    password: String,
    timeout: Duration,
    tls: bool,
    ca: Option<PathBuf>,
    server_name: String,
    tls_profile: TlsProfile,
    json_out: Option<PathBuf>,
}

#[derive(Debug)]
struct SetupReport {
    setup_ms: f64,
    negotiated_group: Option<String>,
    error: Option<String>,
}

#[derive(Debug)]
struct WorkerResult {
    completed: u64,
    error: Option<String>,
}

#[derive(Serialize)]
struct Report {
    status: &'static str,
    engine: &'static str,
    transport: &'static str,
    mode: &'static str,
    tls_profile: Option<&'static str>,
    clients: usize,
    messages_per_client: u64,
    total_messages: u64,
    qos: u8,
    payload_bytes: usize,
    window: usize,
    elapsed_seconds: f64,
    messages_per_second: f64,
    payload_mib_per_second: f64,
    setup_p50_ms: f64,
    setup_p95_ms: f64,
    negotiated_groups: std::collections::BTreeMap<String, usize>,
    failures: usize,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), BenchError> {
    let args = parse_args()?;
    let (start_tx, start_rx) = watch::channel(false);
    let (setup_tx, mut setup_rx) = tokio::sync::mpsc::channel::<SetupReport>(args.clients);
    let mut workers = JoinSet::new();

    for index in 0..args.clients {
        let worker_args = args.clone();
        let setup_tx = setup_tx.clone();
        let start_rx = start_rx.clone();
        workers.spawn(async move { run_worker(index, worker_args, setup_tx, start_rx).await });
    }
    drop(setup_tx);

    let mut setup_times = Vec::with_capacity(args.clients);
    let mut groups = std::collections::BTreeMap::<String, usize>::new();
    let mut setup_failures = 0usize;
    for _ in 0..args.clients {
        let Some(report) = setup_rx.recv().await else {
            setup_failures += 1;
            break;
        };
        if let Some(error) = report.error {
            eprintln!("setup failure: {error}");
            setup_failures += 1;
        } else {
            setup_times.push(report.setup_ms);
            if let Some(group) = report.negotiated_group {
                *groups.entry(group).or_default() += 1;
            }
        }
    }

    if setup_failures > 0 {
        let _ = start_tx.send(true);
        while workers.join_next().await.is_some() {}
        return Err(format!("{setup_failures} client(s) failed during setup").into());
    }

    let start = Instant::now();
    start_tx.send(true)?;

    let mut completed = 0u64;
    let mut failures = 0usize;
    while let Some(joined) = workers.join_next().await {
        match joined {
            Ok(result) => {
                completed += result.completed;
                if let Some(error) = result.error {
                    eprintln!("worker failure: {error}");
                    failures += 1;
                }
            }
            Err(error) => {
                eprintln!("worker task failed: {error}");
                failures += 1;
            }
        }
    }
    let elapsed = start.elapsed().as_secs_f64();

    let report = Report {
        status: if failures == 0 { "ok" } else { "partial" },
        engine: "rust-native-tokio",
        transport: if args.tls { "tls" } else { "tcp" },
        mode: args.mode.as_str(),
        tls_profile: args.tls.then_some(args.tls_profile.as_str()),
        clients: args.clients,
        messages_per_client: args.messages,
        total_messages: completed,
        qos: args.qos,
        payload_bytes: args.payload_bytes,
        window: args.window,
        elapsed_seconds: elapsed,
        messages_per_second: if elapsed > 0.0 {
            completed as f64 / elapsed
        } else {
            0.0
        },
        payload_mib_per_second: if elapsed > 0.0 {
            completed as f64 * args.payload_bytes as f64 / 1_048_576.0 / elapsed
        } else {
            0.0
        },
        setup_p50_ms: percentile(&setup_times, 0.50),
        setup_p95_ms: percentile(&setup_times, 0.95),
        negotiated_groups: groups,
        failures,
    };

    let output = serde_json::to_string_pretty(&report)?;
    println!("{output}");
    if let Some(path) = args.json_out {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, format!("{output}\n"))?;
    }

    if failures == 0 {
        Ok(())
    } else {
        Err(format!("{failures} worker(s) failed").into())
    }
}

async fn run_worker(
    index: usize,
    args: Args,
    setup_tx: tokio::sync::mpsc::Sender<SetupReport>,
    mut start_rx: watch::Receiver<bool>,
) -> WorkerResult {
    match run_worker_inner(index, &args, &setup_tx, &mut start_rx).await {
        Ok(completed) => WorkerResult {
            completed,
            error: None,
        },
        Err(error) => WorkerResult {
            completed: 0,
            error: Some(format!("client {index}: {error}")),
        },
    }
}

async fn run_worker_inner(
    index: usize,
    args: &Args,
    setup_tx: &tokio::sync::mpsc::Sender<SetupReport>,
    start_rx: &mut watch::Receiver<bool>,
) -> Result<u64, BenchError> {
    let setup_start = Instant::now();
    let setup_result = timeout(args.timeout, setup_client(index, args)).await;
    let (stream, group) = match setup_result {
        Ok(Ok(value)) => value,
        Ok(Err(error)) => {
            let _ = setup_tx
                .send(SetupReport {
                    setup_ms: setup_start.elapsed().as_secs_f64() * 1000.0,
                    negotiated_group: None,
                    error: Some(error.to_string()),
                })
                .await;
            return Err(error);
        }
        Err(_) => {
            let error: BenchError = "client setup timed out".into();
            let _ = setup_tx
                .send(SetupReport {
                    setup_ms: setup_start.elapsed().as_secs_f64() * 1000.0,
                    negotiated_group: None,
                    error: Some(error.to_string()),
                })
                .await;
            return Err(error);
        }
    };

    setup_tx
        .send(SetupReport {
            setup_ms: setup_start.elapsed().as_secs_f64() * 1000.0,
            negotiated_group: group,
            error: None,
        })
        .await?;

    while !*start_rx.borrow() {
        start_rx.changed().await?;
    }

    let (read_half, write_half) = tokio::io::split(stream);
    let writer = Arc::new(Mutex::new(write_half));
    let received = Arc::new(AtomicU64::new(0));
    let pubacks = Arc::new(AtomicU64::new(0));
    let received_notify = Arc::new(Notify::new());
    let puback_notify = Arc::new(Notify::new());

    let reader_writer = writer.clone();
    let reader_received = received.clone();
    let reader_pubacks = pubacks.clone();
    let reader_received_notify = received_notify.clone();
    let reader_puback_notify = puback_notify.clone();
    let reader = tokio::spawn(async move {
        reader_loop(
            read_half,
            reader_writer,
            reader_received,
            reader_pubacks,
            reader_received_notify,
            reader_puback_notify,
        )
        .await
    });

    let topic = match args.mode {
        Mode::Loopback => format!("bench/native/{index}"),
        Mode::Ingest => format!("bench/native/ingest/{index}"),
    };
    let payload = vec![b'x'; args.payload_bytes];
    let batch_size = args.window.max(1);

    let mut sent = 0u64;
    while sent < args.messages {
        let count = std::cmp::min(batch_size as u64, args.messages - sent) as usize;
        let mut batch = Vec::new();
        if args.qos == 0 {
            let packet = encode_publish(&topic, &payload, 0, None);
            batch.reserve(packet.len() * count);
            for _ in 0..count {
                batch.extend_from_slice(&packet);
            }
        } else {
            batch.reserve((payload.len() + topic.len() + 16) * count);
            for offset in 0..count {
                let sequence = sent + offset as u64;
                let packet_id = ((sequence % 65_535) + 1) as u16;
                batch.extend_from_slice(&encode_publish(&topic, &payload, 1, Some(packet_id)));
            }
        }

        {
            let mut guard = writer.lock().await;
            guard.write_all(&batch).await?;
        }
        sent += count as u64;

        if args.qos == 1 {
            wait_counter(&pubacks, &puback_notify, sent, args.timeout).await?;
        }
    }

    match (args.mode, args.qos) {
        (Mode::Loopback, _) => {
            wait_counter(&received, &received_notify, args.messages, args.timeout).await?;
            if args.qos == 1 {
                wait_counter(&pubacks, &puback_notify, args.messages, args.timeout).await?;
            }
        }
        (Mode::Ingest, 1) => {
            wait_counter(&pubacks, &puback_notify, args.messages, args.timeout).await?;
        }
        (Mode::Ingest, 0) => {
            // QoS 0 has no acknowledgement. A trailing QoS 1 marker proves that all
            // preceding TCP-ordered publishes have been processed by the broker.
            let marker = encode_publish(
                &format!("bench/native/ingest/{index}/marker"),
                b"done",
                1,
                Some(65_535),
            );
            {
                let mut guard = writer.lock().await;
                guard.write_all(&marker).await?;
            }
            wait_counter(&pubacks, &puback_notify, 1, args.timeout).await?;
        }
        _ => unreachable!(),
    }

    reader.abort();
    Ok(args.messages)
}

async fn setup_client(
    index: usize,
    args: &Args,
) -> Result<(BoxStream, Option<String>), BenchError> {
    let (mut stream, group) = open_stream(args).await?;
    let client_id = format!("pipistrelle_native_bench_{index}");
    stream
        .write_all(&encode_connect(&client_id, &args.username, &args.password))
        .await?;
    let (header, body) = read_packet(&mut stream).await?;
    if header >> 4 != 2 || body.len() < 2 || body[1] != 0 {
        return Err(format!("CONNECT rejected: header=0x{header:02x}, body={body:?}").into());
    }

    if args.mode == Mode::Loopback {
        let topic = format!("bench/native/{index}");
        stream
            .write_all(&encode_subscribe(1, &topic, args.qos))
            .await?;
        let (header, body) = read_packet(&mut stream).await?;
        if header >> 4 != 9 {
            return Err(format!("expected SUBACK, got packet type {}", header >> 4).into());
        }
        let reason = parse_suback_reason(&body)?;
        if reason >= 0x80 {
            return Err(format!("subscription rejected with reason 0x{reason:02x}").into());
        }
    }

    Ok((stream, group))
}

async fn open_stream(args: &Args) -> Result<(BoxStream, Option<String>), BenchError> {
    let tcp = TcpStream::connect((args.host.as_str(), args.port)).await?;
    tcp.set_nodelay(true)?;
    if !args.tls {
        return Ok((Box::new(tcp), None));
    }

    let ca = args
        .ca
        .as_ref()
        .ok_or("--ca is required for TLS benchmarking")?;
    let mut roots = rustls::RootCertStore::empty();
    let mut reader = BufReader::new(File::open(ca)?);
    for cert in rustls_pemfile::certs(&mut reader) {
        roots.add(cert?)?;
    }

    let provider = crypto::provider(args.tls_profile);
    let config = ClientConfig::builder_with_provider(Arc::new(provider))
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let server_name = ServerName::try_from(args.server_name.clone())?;
    let tls = connector.connect(server_name, tcp).await?;
    let group = tls
        .get_ref()
        .1
        .negotiated_key_exchange_group()
        .map(|group| format!("{:?}", group.name()));
    Ok((Box::new(tls), group))
}

async fn reader_loop<R, W>(
    mut reader: R,
    writer: Arc<Mutex<W>>,
    received: Arc<AtomicU64>,
    pubacks: Arc<AtomicU64>,
    received_notify: Arc<Notify>,
    puback_notify: Arc<Notify>,
) -> Result<(), BenchError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut read_buf = BytesMut::with_capacity(256 * 1024);
    let mut ack_batch = Vec::with_capacity(16 * 1024);

    loop {
        let n = reader.read_buf(&mut read_buf).await?;
        if n == 0 {
            return Err(
                io::Error::new(io::ErrorKind::UnexpectedEof, "benchmark stream closed").into(),
            );
        }

        let mut received_batch = 0u64;
        let mut puback_batch = 0u64;
        ack_batch.clear();

        while let Some((header, body_start, packet_len)) = try_packet_bounds(&read_buf)? {
            let body = &read_buf[body_start..packet_len];
            match header >> 4 {
                3 => {
                    let qos = (header >> 1) & 0x03;
                    received_batch += 1;
                    if qos == 1 {
                        if let Some(packet_id) = parse_publish_packet_id(body, qos)? {
                            ack_batch.extend_from_slice(&encode_puback(packet_id));
                        }
                    }
                }
                4 => puback_batch += 1,
                13 => {}
                _ => {}
            }
            read_buf.advance(packet_len);
        }

        if received_batch != 0 {
            received.fetch_add(received_batch, Ordering::Relaxed);
            received_notify.notify_waiters();
        }
        if puback_batch != 0 {
            pubacks.fetch_add(puback_batch, Ordering::Relaxed);
            puback_notify.notify_waiters();
        }
        if !ack_batch.is_empty() {
            let mut guard = writer.lock().await;
            guard.write_all(&ack_batch).await?;
        }
    }
}

/// Returns fixed header, body offset and total packet length when a complete MQTT
/// packet is already buffered. Parsing never allocates and handles partial varints.
fn try_packet_bounds(buf: &[u8]) -> io::Result<Option<(u8, usize, usize)>> {
    if buf.len() < 2 {
        return Ok(None);
    }
    let header = buf[0];
    let mut multiplier = 1usize;
    let mut remaining = 0usize;
    let mut index = 1usize;
    for _ in 0..4 {
        if index >= buf.len() {
            return Ok(None);
        }
        let byte = buf[index];
        index += 1;
        remaining = remaining
            .checked_add(((byte & 0x7f) as usize).saturating_mul(multiplier))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "MQTT length overflow"))?;
        if byte & 0x80 == 0 {
            let total = index.checked_add(remaining).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "MQTT packet overflow")
            })?;
            if buf.len() < total {
                return Ok(None);
            }
            return Ok(Some((header, index, total)));
        }
        multiplier = multiplier.saturating_mul(128);
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "malformed MQTT remaining length",
    ))
}

async fn wait_counter(
    counter: &AtomicU64,
    notify: &Notify,
    target: u64,
    max_wait: Duration,
) -> Result<(), BenchError> {
    let deadline = Instant::now() + max_wait;
    loop {
        let notified = notify.notified();
        if counter.load(Ordering::Acquire) >= target {
            return Ok(());
        }
        let now = Instant::now();
        if now >= deadline {
            return Err(format!(
                "timeout waiting for counter: {}/{}",
                counter.load(Ordering::Relaxed),
                target
            )
            .into());
        }
        timeout(deadline - now, notified).await?;
    }
}

async fn read_packet<R>(reader: &mut R) -> io::Result<(u8, Vec<u8>)>
where
    R: AsyncRead + Unpin,
{
    let mut first = [0u8; 1];
    reader.read_exact(&mut first).await?;
    let mut multiplier = 1usize;
    let mut remaining = 0usize;
    for _ in 0..4 {
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte).await?;
        remaining += ((byte[0] & 0x7f) as usize) * multiplier;
        if byte[0] & 0x80 == 0 {
            let mut body = vec![0u8; remaining];
            reader.read_exact(&mut body).await?;
            return Ok((first[0], body));
        }
        multiplier *= 128;
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "malformed MQTT remaining length",
    ))
}

fn encode_connect(client_id: &str, username: &str, password: &str) -> Vec<u8> {
    let mut body = Vec::new();
    put_utf8(&mut body, "MQTT");
    body.push(5);
    body.push(0x02 | 0x80 | 0x40); // clean start + username + password
    body.extend_from_slice(&60u16.to_be_bytes());
    body.push(0); // CONNECT properties length
    put_utf8(&mut body, client_id);
    put_utf8(&mut body, username);
    put_binary(&mut body, password.as_bytes());
    make_packet(0x10, body)
}

fn encode_subscribe(packet_id: u16, topic: &str, qos: u8) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    body.push(0); // properties
    put_utf8(&mut body, topic);
    body.push(qos & 0x03);
    make_packet(0x82, body)
}

fn encode_publish(topic: &str, payload: &[u8], qos: u8, packet_id: Option<u16>) -> Vec<u8> {
    let mut body = Vec::with_capacity(topic.len() + payload.len() + 8);
    put_utf8(&mut body, topic);
    if qos > 0 {
        body.extend_from_slice(&packet_id.unwrap_or(1).to_be_bytes());
    }
    body.push(0); // PUBLISH properties length
    body.extend_from_slice(payload);
    make_packet(0x30 | ((qos & 0x03) << 1), body)
}

fn encode_puback(packet_id: u16) -> Vec<u8> {
    vec![0x40, 0x02, (packet_id >> 8) as u8, packet_id as u8]
}

fn make_packet(header: u8, body: Vec<u8>) -> Vec<u8> {
    let mut packet = Vec::with_capacity(body.len() + 5);
    packet.push(header);
    encode_varint(body.len(), &mut packet);
    packet.extend_from_slice(&body);
    packet
}

fn encode_varint(mut value: usize, output: &mut Vec<u8>) {
    loop {
        let mut encoded = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            encoded |= 0x80;
        }
        output.push(encoded);
        if value == 0 {
            break;
        }
    }
}

fn decode_varint(bytes: &[u8], pos: &mut usize) -> io::Result<usize> {
    let mut multiplier = 1usize;
    let mut value = 0usize;
    for _ in 0..4 {
        let byte = *bytes
            .get(*pos)
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "truncated MQTT varint"))?;
        *pos += 1;
        value += ((byte & 0x7f) as usize) * multiplier;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        multiplier *= 128;
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "invalid MQTT varint",
    ))
}

fn parse_suback_reason(body: &[u8]) -> io::Result<u8> {
    if body.len() < 3 {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short SUBACK"));
    }
    let mut pos = 2usize;
    let properties_len = decode_varint(body, &mut pos)?;
    pos = pos
        .checked_add(properties_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "SUBACK overflow"))?;
    body.get(pos)
        .copied()
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "SUBACK missing reason"))
}

fn parse_publish_packet_id(body: &[u8], qos: u8) -> io::Result<Option<u16>> {
    if body.len() < 2 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "short PUBLISH",
        ));
    }
    let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
    let mut pos = 2usize
        .checked_add(topic_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "PUBLISH topic overflow"))?;
    if pos > body.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "short PUBLISH topic",
        ));
    }
    if qos == 0 {
        return Ok(None);
    }
    if pos + 2 > body.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "PUBLISH missing packet id",
        ));
    }
    let packet_id = u16::from_be_bytes([body[pos], body[pos + 1]]);
    pos += 2;
    let _properties_len = decode_varint(body, &mut pos)?;
    Ok(Some(packet_id))
}

fn put_utf8(output: &mut Vec<u8>, value: &str) {
    let len = value.len() as u16;
    output.extend_from_slice(&len.to_be_bytes());
    output.extend_from_slice(value.as_bytes());
}

fn put_binary(output: &mut Vec<u8>, value: &[u8]) {
    let len = value.len() as u16;
    output.extend_from_slice(&len.to_be_bytes());
    output.extend_from_slice(value);
}

fn percentile(values: &[f64], percentile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let position = (sorted.len() - 1) as f64 * percentile;
    let low = position.floor() as usize;
    let high = position.ceil() as usize;
    if low == high {
        sorted[low]
    } else {
        let fraction = position - low as f64;
        sorted[low] * (1.0 - fraction) + sorted[high] * fraction
    }
}

fn parse_args() -> Result<Args, BenchError> {
    let mut args = Args {
        host: "127.0.0.1".to_string(),
        port: 1883,
        clients: 1,
        messages: 100_000,
        payload_bytes: 128,
        qos: 0,
        window: 1024,
        mode: Mode::Loopback,
        username: env::var("PIPISTRELLE_BENCH_USER").unwrap_or_else(|_| "admin".to_string()),
        password: env::var("PIPISTRELLE_BENCH_PASSWORD").unwrap_or_else(|_| "admin123".to_string()),
        timeout: Duration::from_secs(60),
        tls: false,
        ca: None,
        server_name: "localhost".to_string(),
        tls_profile: TlsProfile::Hybrid,
        json_out: None,
    };
    let mut port_explicit = false;
    let raw: Vec<String> = env::args().skip(1).collect();
    let mut i = 0usize;
    while i < raw.len() {
        let flag = raw[i].as_str();
        let value = |i: &mut usize| -> Result<&str, BenchError> {
            *i += 1;
            raw.get(*i)
                .map(String::as_str)
                .ok_or_else(|| format!("missing value for {flag}").into())
        };
        match flag {
            "--host" => args.host = value(&mut i)?.to_string(),
            "--port" => {
                args.port = value(&mut i)?.parse()?;
                port_explicit = true;
            }
            "--clients" => args.clients = value(&mut i)?.parse()?,
            "--messages" => args.messages = value(&mut i)?.parse()?,
            "--payload" => args.payload_bytes = value(&mut i)?.parse()?,
            "--qos" => args.qos = value(&mut i)?.parse()?,
            "--window" => args.window = value(&mut i)?.parse()?,
            "--mode" => args.mode = Mode::parse(value(&mut i)?)?,
            "--username" => args.username = value(&mut i)?.to_string(),
            "--password" => args.password = value(&mut i)?.to_string(),
            "--timeout" => args.timeout = Duration::from_secs(value(&mut i)?.parse()?),
            "--tls" => args.tls = true,
            "--ca" => args.ca = Some(PathBuf::from(value(&mut i)?)),
            "--server-name" => args.server_name = value(&mut i)?.to_string(),
            "--tls-profile" => {
                args.tls_profile = match value(&mut i)? {
                    "hybrid" => TlsProfile::Hybrid,
                    "pqc-strict" => TlsProfile::PqcStrict,
                    "classical" => TlsProfile::Classical,
                    other => return Err(format!("invalid TLS profile '{other}'").into()),
                }
            }
            "--json-out" => args.json_out = Some(PathBuf::from(value(&mut i)?)),
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument '{other}'").into()),
        }
        i += 1;
    }

    if args.clients == 0 || args.messages == 0 || args.window == 0 {
        return Err("clients, messages and window must be > 0".into());
    }
    if args.qos > 1 {
        return Err("native benchmark currently supports QoS 0 and 1".into());
    }
    if args.tls && !port_explicit {
        args.port = 8883;
    }
    if args.tls && args.ca.is_none() {
        args.ca = Some(PathBuf::from("config/cert.pem"));
    }
    Ok(args)
}

fn print_help() {
    println!(
        "Pipistrelle native benchmark\n\
         \n\
         cargo run --release --bin pipistrelle-bench -- [options]\n\
         \n\
         --mode loopback|ingest   end-to-end routing or raw broker ingest\n\
         --clients N              concurrent MQTT clients (default 1)\n\
         --messages N             messages per client (default 100000)\n\
         --payload N              payload bytes (default 128)\n\
         --qos 0|1                MQTT QoS (default 0)\n\
         --window N               batch/in-flight window (default 1024)\n\
         --host HOST              broker host (default 127.0.0.1)\n\
         --port PORT              broker port (1883 TCP, 8883 TLS)\n\
         --tls                    enable native rustls TLS 1.3\n\
         --ca PATH                CA/self-signed certificate PEM\n\
         --server-name NAME       TLS SNI/certificate name (default localhost)\n\
         --tls-profile PROFILE    hybrid|pqc-strict|classical\n\
         --username USER          MQTT username\n\
         --password PASS          MQTT password (or PIPISTRELLE_BENCH_PASSWORD)\n\
         --timeout SECONDS        per-stage timeout (default 60)\n\
         --json-out PATH          save machine-readable result"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buffered_packet_bounds_handle_complete_and_partial_packets() {
        let packet = encode_publish("bench/native/7", b"payload", 0, None);
        assert_eq!(
            try_packet_bounds(&packet).unwrap(),
            Some((0x30, 2, packet.len()))
        );
        for cut in 0..packet.len() {
            assert_eq!(try_packet_bounds(&packet[..cut]).unwrap(), None);
        }
    }

    #[test]
    fn buffered_packet_bounds_support_multiple_packets() {
        let first = encode_publish("a", b"1", 0, None);
        let second = encode_puback(42);
        let mut combined = first.clone();
        combined.extend_from_slice(&second);
        let (_, _, first_len) = try_packet_bounds(&combined).unwrap().unwrap();
        assert_eq!(first_len, first.len());
        let (header, _, second_len) = try_packet_bounds(&combined[first_len..]).unwrap().unwrap();
        assert_eq!(header >> 4, 4);
        assert_eq!(second_len, second.len());
    }
}
