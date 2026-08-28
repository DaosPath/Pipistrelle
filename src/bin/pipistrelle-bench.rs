use bytes::{Buf, BytesMut};
use pipistrelle::crypto::{self, TlsProfile};
use rustls::ClientConfig;
use rustls_pki_types::{CertificateDer, ServerName, pem::PemObject};
use serde::Serialize;
use std::env;
use std::fs::File;
use std::io;
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

enum BenchWriter<W> {
    Direct(W),
    Shared(Arc<Mutex<W>>),
}

impl<W> BenchWriter<W>
where
    W: AsyncWrite + Unpin,
{
    #[inline]
    async fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        match self {
            Self::Direct(writer) => writer.write_all(bytes).await,
            Self::Shared(writer) => {
                let mut guard = writer.lock().await;
                guard.write_all(bytes).await
            }
        }
    }
}

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
    topic_alias: bool,
    accept_topic_alias: u16,
    sendfile: bool,
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
    topic_alias: bool,
    accept_topic_alias: u16,
    sendfile: bool,
    elapsed_seconds: f64,
    messages_per_second: f64,
    payload_mib_per_second: f64,
    setup_p50_ms: f64,
    setup_p95_ms: f64,
    negotiated_groups: std::collections::BTreeMap<String, usize>,
    failures: usize,
}

#[cfg(target_os = "linux")]
struct SendfilePreparedClient {
    stream: std::net::TcpStream,
    memfd: File,
    mapping: Option<Vec<u8>>,
    marker: Vec<u8>,
    packet_len: usize,
    messages: u64,
    window: usize,
}

#[cfg(target_os = "linux")]
fn read_packet_sync(stream: &mut std::net::TcpStream) -> io::Result<(u8, Vec<u8>)> {
    use std::io::Read;
    let mut first = [0u8; 1];
    stream.read_exact(&mut first)?;
    let mut multiplier = 1usize;
    let mut remaining = 0usize;
    for _ in 0..4 {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte)?;
        remaining += ((byte[0] & 0x7f) as usize) * multiplier;
        if byte[0] & 0x80 == 0 {
            let mut body = vec![0u8; remaining];
            stream.read_exact(&mut body)?;
            return Ok((first[0], body));
        }
        multiplier *= 128;
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "malformed MQTT remaining length",
    ))
}

#[cfg(target_os = "linux")]
fn sendfile_all(socket: &std::net::TcpStream, file: &File, bytes: usize) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let mut offset: libc::off_t = 0;
    let mut remaining = bytes;
    while remaining != 0 {
        let sent =
            unsafe { libc::sendfile(socket.as_raw_fd(), file.as_raw_fd(), &mut offset, remaining) };
        if sent < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        if sent == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "sendfile made no progress",
            ));
        }
        remaining -= sent as usize;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn prepare_sendfile_client(
    index: usize,
    args: &Args,
) -> Result<(SendfilePreparedClient, f64), BenchError> {
    use std::ffi::CString;
    use std::io::Write;
    use std::os::fd::FromRawFd;

    let setup_start = Instant::now();
    let mut stream = std::net::TcpStream::connect((args.host.as_str(), args.port))?;
    stream.set_nodelay(true)?;
    let client_id = format!("pipistrelle_native_bench_{index}");
    stream.write_all(&encode_connect(
        &client_id,
        &args.username,
        &args.password,
        args.accept_topic_alias,
    ))?;
    let (header, body) = read_packet_sync(&mut stream)?;
    if header >> 4 != 2 || body.len() < 2 || body[1] != 0 {
        return Err(format!("CONNECT rejected: header=0x{header:02x}, body={body:?}").into());
    }
    if args.topic_alias && parse_connack_topic_alias_maximum(&body)? < 1 {
        return Err("broker did not advertise Topic Alias Maximum >= 1".into());
    }

    let topic = format!("bench/native/ingest/{index}");
    let payload = vec![b'x'; args.payload_bytes];
    let mapping = args
        .topic_alias
        .then(|| encode_publish_topic_alias(&topic, &payload, 1));
    let packet = if args.topic_alias {
        encode_publish_topic_alias("", &payload, 1)
    } else {
        encode_publish(&topic, &payload, 0, None)
    };
    let packet_len = packet.len();
    let mut batch = Vec::with_capacity(packet_len * args.window);
    for _ in 0..args.window {
        batch.extend_from_slice(&packet);
    }

    let name = CString::new(format!("pipistrelle-bench-{index}"))?;
    let fd = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error().into());
    }
    let mut memfd = unsafe { File::from_raw_fd(fd) };
    memfd.write_all(&batch)?;

    let marker = encode_publish(
        &format!("bench/native/ingest/{index}/marker"),
        b"done",
        1,
        Some(65_535),
    );
    Ok((
        SendfilePreparedClient {
            stream,
            memfd,
            mapping,
            marker,
            packet_len,
            messages: args.messages,
            window: args.window,
        },
        setup_start.elapsed().as_secs_f64() * 1000.0,
    ))
}

#[cfg(target_os = "linux")]
fn run_sendfile_worker(mut client: SendfilePreparedClient) -> Result<u64, BenchError> {
    use std::io::Write;

    let mut sent = 0u64;
    if let Some(mapping) = client.mapping.as_ref() {
        if client.messages != 0 {
            client.stream.write_all(mapping)?;
            sent = 1;
        }
    }
    while sent < client.messages {
        let count = std::cmp::min(client.window as u64, client.messages - sent) as usize;
        sendfile_all(&client.stream, &client.memfd, client.packet_len * count)?;
        sent += count as u64;
    }

    client.stream.write_all(&client.marker)?;
    let (header, body) = read_packet_sync(&mut client.stream)?;
    if header >> 4 != 4 || body.len() < 2 || body[0] != 0xff || body[1] != 0xff {
        return Err(
            format!("expected marker PUBACK, got header=0x{header:02x}, body={body:?}").into(),
        );
    }
    Ok(client.messages)
}

#[cfg(target_os = "linux")]
fn run_sendfile_benchmark(args: &Args) -> Result<(), BenchError> {
    use std::sync::Barrier;

    let mut prepared = Vec::with_capacity(args.clients);
    let mut setup_times = Vec::with_capacity(args.clients);
    for index in 0..args.clients {
        let (client, setup_ms) = prepare_sendfile_client(index, args)?;
        prepared.push(client);
        setup_times.push(setup_ms);
    }

    let barrier = Arc::new(Barrier::new(args.clients + 1));
    let mut handles = Vec::with_capacity(args.clients);
    for client in prepared {
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            run_sendfile_worker(client)
        }));
    }

    let start = Instant::now();
    barrier.wait();
    let mut completed = 0u64;
    let mut failures = 0usize;
    for handle in handles {
        match handle.join() {
            Ok(Ok(messages)) => completed += messages,
            Ok(Err(error)) => {
                eprintln!("worker failure: {error}");
                failures += 1;
            }
            Err(_) => {
                eprintln!("worker thread panicked");
                failures += 1;
            }
        }
    }
    let elapsed = start.elapsed().as_secs_f64();
    let report = Report {
        status: if failures == 0 { "ok" } else { "partial" },
        engine: "rust-native-sendfile",
        transport: "tcp",
        mode: args.mode.as_str(),
        tls_profile: None,
        clients: args.clients,
        messages_per_client: args.messages,
        total_messages: completed,
        qos: args.qos,
        payload_bytes: args.payload_bytes,
        window: args.window,
        topic_alias: args.topic_alias,
        accept_topic_alias: args.accept_topic_alias,
        sendfile: true,
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
        negotiated_groups: std::collections::BTreeMap::new(),
        failures,
    };
    let output = serde_json::to_string_pretty(&report)?;
    println!("{output}");
    if let Some(path) = args.json_out.as_ref() {
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

#[cfg(not(target_os = "linux"))]
fn run_sendfile_benchmark(_args: &Args) -> Result<(), BenchError> {
    Err("--sendfile is supported on Linux only".into())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), BenchError> {
    let args = parse_args()?;
    if args.sendfile {
        return run_sendfile_benchmark(&args);
    }
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
        topic_alias: args.topic_alias,
        accept_topic_alias: args.accept_topic_alias,
        sendfile: false,
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
    // QoS0 has only one socket writer: the publisher task. Keep the WriteHalf owned
    // directly and avoid a Tokio mutex on every benchmark send window. QoS1 still
    // shares it because the reader must emit PUBACKs for routed QoS1 PUBLISH packets.
    let (reader_writer, mut writer) = if args.qos == 0 {
        (None, BenchWriter::Direct(write_half))
    } else {
        let shared = Arc::new(Mutex::new(write_half));
        (Some(shared.clone()), BenchWriter::Shared(shared))
    };
    let received = Arc::new(AtomicU64::new(0));
    let pubacks = Arc::new(AtomicU64::new(0));
    let received_notify = Arc::new(Notify::new());
    let puback_notify = Arc::new(Notify::new());

    let reader_received = received.clone();
    let reader_pubacks = pubacks.clone();
    let reader_received_notify = received_notify.clone();
    let reader_puback_notify = puback_notify.clone();
    let fast_qos0_loopback = args.mode == Mode::Loopback && args.qos == 0;
    let accept_topic_alias = args.accept_topic_alias;
    let reader = tokio::spawn(async move {
        reader_loop(
            read_half,
            reader_writer,
            reader_received,
            reader_pubacks,
            reader_received_notify,
            reader_puback_notify,
            fast_qos0_loopback,
            accept_topic_alias,
        )
        .await
    });

    let topic = match args.mode {
        Mode::Loopback => format!("bench/native/{index}"),
        Mode::Ingest => format!("bench/native/ingest/{index}"),
    };
    let payload = vec![b'x'; args.payload_bytes];
    let batch_size = args.window.max(1);

    // QoS0 benchmark packets are byte-identical for the lifetime of one client.
    // Build the full send window once and reuse it instead of copying the same packet
    // into a new multi-megabyte Vec for every window. With --topic-alias, the first
    // counted PUBLISH establishes alias 1; all remaining packets use zero-length Topic
    // Name + Topic Alias=1, exactly as MQTT 5 permits for a Network Connection.
    let qos0_mapping_packet = (args.qos == 0 && args.topic_alias)
        .then(|| encode_publish_topic_alias(&topic, &payload, 1));
    let qos0_packet = (args.qos == 0).then(|| {
        if args.topic_alias {
            encode_publish_topic_alias("", &payload, 1)
        } else {
            encode_publish(&topic, &payload, 0, None)
        }
    });
    let qos0_full_batch = qos0_packet.as_ref().map(|packet| {
        let mut batch = Vec::with_capacity(packet.len() * batch_size);
        for _ in 0..batch_size {
            batch.extend_from_slice(packet);
        }
        batch
    });

    let mut sent = 0u64;
    if let Some(mapping) = qos0_mapping_packet.as_ref() {
        if args.messages > 0 {
            writer.write_all(mapping).await?;
            sent = 1;
        }
    }
    while sent < args.messages {
        let count = std::cmp::min(batch_size as u64, args.messages - sent) as usize;
        if args.qos == 0 {
            let packet_len = qos0_packet.as_ref().expect("qos0 packet").len();
            let batch = qos0_full_batch.as_ref().expect("qos0 batch");
            writer.write_all(&batch[..packet_len * count]).await?;
        } else {
            let mut batch = Vec::with_capacity((payload.len() + topic.len() + 16) * count);
            for offset in 0..count {
                let sequence = sent + offset as u64;
                let packet_id = ((sequence % 65_535) + 1) as u16;
                batch.extend_from_slice(&encode_publish(&topic, &payload, 1, Some(packet_id)));
            }
            writer.write_all(&batch).await?;
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
            writer.write_all(&marker).await?;
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
        .write_all(&encode_connect(
            &client_id,
            &args.username,
            &args.password,
            args.accept_topic_alias,
        ))
        .await?;
    let (header, body) = read_packet(&mut stream).await?;
    if header >> 4 != 2 || body.len() < 2 || body[1] != 0 {
        return Err(format!("CONNECT rejected: header=0x{header:02x}, body={body:?}").into());
    }

    if args.topic_alias {
        let maximum = parse_connack_topic_alias_maximum(&body)?;
        if maximum < 1 {
            return Err("broker did not advertise Topic Alias Maximum >= 1".into());
        }
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
    let certs = CertificateDer::pem_file_iter(ca)?.collect::<Result<Vec<_>, _>>()?;
    roots.add_parsable_certificates(certs);

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
    writer: Option<Arc<Mutex<W>>>,
    received: Arc<AtomicU64>,
    pubacks: Arc<AtomicU64>,
    received_notify: Arc<Notify>,
    puback_notify: Arc<Notify>,
    fast_qos0_loopback: bool,
    accept_topic_alias: u16,
) -> Result<(), BenchError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut read_buf = BytesMut::with_capacity(256 * 1024);
    let mut ack_batch = Vec::with_capacity(16 * 1024);
    let mut qos0_layout = FastQos0ReadLayout::default();
    let mut server_topic_aliases = std::collections::HashMap::<u16, Vec<u8>>::new();

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

        loop {
            if fast_qos0_loopback && qos0_layout.packet_len != 0 {
                let batch = scan_qos0_read_batch(&read_buf, &qos0_layout);
                if batch.messages != 0 {
                    received_batch += batch.messages as u64;
                    read_buf.advance(batch.bytes);
                    continue;
                }
            }

            let Some((header, body_start, packet_len)) = try_packet_bounds(&read_buf)? else {
                break;
            };
            let body = &read_buf[body_start..packet_len];
            match header >> 4 {
                3 => {
                    let qos = (header >> 1) & 0x03;
                    received_batch += 1;
                    if fast_qos0_loopback && qos == 0 {
                        let prefix_end = if accept_topic_alias != 0 {
                            match validate_server_topic_alias(
                                body,
                                accept_topic_alias,
                                &mut server_topic_aliases,
                            )? {
                                Some(payload_offset) => body_start + payload_offset,
                                None => body_start,
                            }
                        } else {
                            body_start
                        };
                        qos0_layout.cache(&read_buf[..prefix_end], packet_len);
                    }
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
            let shared = writer.as_ref().ok_or_else(|| {
                io::Error::other("QoS0 benchmark reader unexpectedly needed a socket writer")
            })?;
            let mut guard = shared.lock().await;
            guard.write_all(&ack_batch).await?;
        }
    }
}

#[derive(Debug, Default)]
struct FastQos0ReadLayout {
    packet_len: usize,
    prefix_len: usize,
    prefix: [u8; 32],
    header_word: u32,
    header_mask: u32,
    scalar4: bool,
    scalar9_word: u64,
    scalar9_tail: u8,
    scalar9: bool,
}

impl FastQos0ReadLayout {
    fn cache(&mut self, prefix: &[u8], packet_len: usize) {
        if prefix.is_empty() || prefix.len() > self.prefix.len() || packet_len < prefix.len() {
            self.packet_len = 0;
            return;
        }
        self.packet_len = packet_len;
        self.prefix_len = prefix.len();
        self.prefix = [0; 32];
        self.prefix[..prefix.len()].copy_from_slice(prefix);
        self.scalar4 = prefix.len() <= 4 && packet_len >= 4;
        self.header_word = 0;
        self.header_mask = 0;
        if self.scalar4 {
            let mut word = [0u8; 4];
            let mut mask = [0u8; 4];
            word[..prefix.len()].copy_from_slice(prefix);
            mask[..prefix.len()].fill(0xff);
            self.header_word = u32::from_ne_bytes(word);
            self.header_mask = u32::from_ne_bytes(mask);
        }
        self.scalar9 = prefix.len() == 9 && packet_len >= 9;
        self.scalar9_word = 0;
        self.scalar9_tail = 0;
        if self.scalar9 {
            self.scalar9_word = u64::from_ne_bytes(prefix[..8].try_into().unwrap());
            self.scalar9_tail = prefix[8];
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct FastReadBatch {
    bytes: usize,
    messages: usize,
}

#[inline]
fn scan_qos0_read_batch(buf: &[u8], layout: &FastQos0ReadLayout) -> FastReadBatch {
    if layout.packet_len == 0 || layout.prefix_len == 0 {
        return FastReadBatch::default();
    }
    let mut offset = 0usize;
    let mut messages = 0usize;
    if layout.scalar9 {
        let sixteen = layout.packet_len.saturating_mul(16);
        while offset + sixteen <= buf.len() {
            let mut any = 0u64;
            let mut tails = 0u8;
            unsafe {
                for lane in 0..16 {
                    let ptr = buf.as_ptr().add(offset + lane * layout.packet_len);
                    any |= std::ptr::read_unaligned(ptr.cast::<u64>()) ^ layout.scalar9_word;
                    tails |= *ptr.add(8) ^ layout.scalar9_tail;
                }
            }
            if any != 0 || tails != 0 {
                break;
            }
            offset += sixteen;
            messages += 16;
        }
    } else if layout.scalar4 {
        let sixteen = layout.packet_len.saturating_mul(16);
        while offset + sixteen <= buf.len() {
            let mut any = 0u32;
            unsafe {
                for lane in 0..16 {
                    let ptr = buf.as_ptr().add(offset + lane * layout.packet_len);
                    let actual = std::ptr::read_unaligned(ptr.cast::<u32>());
                    any |= (actual ^ layout.header_word) & layout.header_mask;
                }
            }
            if any != 0 {
                break;
            }
            offset += sixteen;
            messages += 16;
        }
    }
    while offset + layout.packet_len <= buf.len() {
        let matches = if layout.scalar9 {
            let ptr = unsafe { buf.as_ptr().add(offset) };
            let word = unsafe { std::ptr::read_unaligned(ptr.cast::<u64>()) };
            word == layout.scalar9_word && unsafe { *ptr.add(8) } == layout.scalar9_tail
        } else if layout.scalar4 {
            let actual =
                unsafe { std::ptr::read_unaligned(buf.as_ptr().add(offset).cast::<u32>()) };
            ((actual ^ layout.header_word) & layout.header_mask) == 0
        } else {
            buf[offset..offset + layout.prefix_len] == layout.prefix[..layout.prefix_len]
        };
        if !matches {
            break;
        }
        offset += layout.packet_len;
        messages += 1;
    }
    FastReadBatch {
        bytes: offset,
        messages,
    }
}

fn validate_server_topic_alias(
    body: &[u8],
    maximum: u16,
    mappings: &mut std::collections::HashMap<u16, Vec<u8>>,
) -> io::Result<Option<usize>> {
    if body.len() < 3 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "short PUBLISH body",
        ));
    }
    let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
    let topic_end = 2usize
        .checked_add(topic_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "topic length overflow"))?;
    if topic_end >= body.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "short PUBLISH topic",
        ));
    }
    let mut pos = topic_end;
    let (properties_len, properties_len_bytes) = decode_bench_varint(&body[pos..])?;
    pos += properties_len_bytes;
    let properties_end = pos
        .checked_add(properties_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "property length overflow"))?;
    if properties_end > body.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "short PUBLISH properties",
        ));
    }
    if properties_len == 0 {
        return Ok(None);
    }
    if properties_len != 3 || body[pos] != 0x23 {
        return Ok(None);
    }
    let alias = u16::from_be_bytes([body[pos + 1], body[pos + 2]]);
    if alias == 0 || alias > maximum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid server Topic Alias",
        ));
    }
    let topic = &body[2..topic_end];
    if topic.is_empty() {
        if !mappings.contains_key(&alias) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "server used Topic Alias before mapping",
            ));
        }
    } else {
        mappings.insert(alias, topic.to_vec());
    }
    Ok(Some(properties_end))
}

fn decode_bench_varint(input: &[u8]) -> io::Result<(usize, usize)> {
    let mut value = 0usize;
    let mut multiplier = 1usize;
    for (index, &byte) in input.iter().take(4).enumerate() {
        value = value
            .checked_add(((byte & 0x7f) as usize).saturating_mul(multiplier))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "varint overflow"))?;
        if byte & 0x80 == 0 {
            return Ok((value, index + 1));
        }
        multiplier = multiplier.saturating_mul(128);
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "malformed varint",
    ))
}

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

fn encode_connect(
    client_id: &str,
    username: &str,
    password: &str,
    accept_topic_alias: u16,
) -> Vec<u8> {
    let mut body = Vec::new();
    put_utf8(&mut body, "MQTT");
    body.push(5);
    body.push(0x02 | 0x80 | 0x40); // clean start + username + password
    body.extend_from_slice(&60u16.to_be_bytes());
    let mut properties = Vec::new();
    if accept_topic_alias != 0 {
        properties.push(0x22); // Topic Alias Maximum
        properties.extend_from_slice(&accept_topic_alias.to_be_bytes());
    }
    encode_varint(properties.len(), &mut body);
    body.extend_from_slice(&properties);
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

fn encode_publish_topic_alias(topic: &str, payload: &[u8], alias: u16) -> Vec<u8> {
    let mut body = Vec::with_capacity(topic.len() + payload.len() + 8);
    put_utf8(&mut body, topic);
    body.push(3); // property length
    body.push(0x23); // Topic Alias
    body.extend_from_slice(&alias.to_be_bytes());
    body.extend_from_slice(payload);
    make_packet(0x30, body)
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

fn parse_connack_topic_alias_maximum(body: &[u8]) -> io::Result<u16> {
    if body.len() < 3 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "short CONNACK",
        ));
    }
    let mut pos = 2usize;
    let properties_len = decode_varint(body, &mut pos)?;
    let end = pos
        .checked_add(properties_len)
        .filter(|end| *end <= body.len())
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "short CONNACK properties"))?;
    let mut maximum = 0u16;
    while pos < end {
        let id = body[pos];
        pos += 1;
        match id {
            0x21 => pos += 2, // Receive Maximum
            0x27 => pos += 4, // Maximum Packet Size
            0x22 => {
                if pos + 2 > end {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "short Topic Alias Maximum",
                    ));
                }
                maximum = u16::from_be_bytes([body[pos], body[pos + 1]]);
                pos += 2;
            }
            0x12 | 0x1A | 0x1C | 0x15 => {
                // UTF-8 properties
                if pos + 2 > end {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "short CONNACK UTF-8 property",
                    ));
                }
                let len = u16::from_be_bytes([body[pos], body[pos + 1]]) as usize;
                pos += 2 + len;
            }
            0x16 => {
                // binary auth data
                if pos + 2 > end {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "short CONNACK binary property",
                    ));
                }
                let len = u16::from_be_bytes([body[pos], body[pos + 1]]) as usize;
                pos += 2 + len;
            }
            0x24 | 0x25 | 0x28 | 0x29 | 0x2A => pos += 1,
            0x13 => pos += 2,
            0x26 => {
                // User Property: two UTF-8 strings
                for _ in 0..2 {
                    if pos + 2 > end {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "short CONNACK user property",
                        ));
                    }
                    let len = u16::from_be_bytes([body[pos], body[pos + 1]]) as usize;
                    pos += 2 + len;
                }
            }
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown CONNACK property 0x{other:02x}"),
                ));
            }
        }
        if pos > end {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated CONNACK property",
            ));
        }
    }
    Ok(maximum)
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
        topic_alias: false,
        accept_topic_alias: 0,
        sendfile: false,
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
            "--topic-alias" => args.topic_alias = true,
            "--accept-topic-alias" => args.accept_topic_alias = value(&mut i)?.parse()?,
            "--sendfile" => args.sendfile = true,
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
    if args.topic_alias && args.qos != 0 {
        return Err("--topic-alias currently benchmarks QoS 0 only".into());
    }
    if args.sendfile {
        if args.mode != Mode::Ingest || args.qos != 0 || args.tls {
            return Err("--sendfile requires plain-TCP --mode ingest --qos 0".into());
        }
        #[cfg(not(target_os = "linux"))]
        return Err("--sendfile is supported on Linux only".into());
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
         --topic-alias            use MQTT 5 Topic Alias after first counted PUBLISH\n\
         --accept-topic-alias N   advertise Server->Client Topic Alias Maximum (0 disables)\n\
         --sendfile               Linux ingest backend: memfd -> TCP sendfile for QoS0\n\
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
    fn connect_can_advertise_server_to_client_topic_alias_maximum() {
        let packet = encode_connect("client", "user", "pass", 7);
        let (_, body_start, packet_len) = try_packet_bounds(&packet).unwrap().unwrap();
        let body = &packet[body_start..packet_len];
        // MQTT(6 bytes) + level + flags + keepalive = 10 bytes before properties.
        let properties_len = body[10] as usize;
        assert_eq!(properties_len, 3);
        assert_eq!(&body[11..14], &[0x22, 0x00, 0x07]);
    }

    #[test]
    fn server_topic_alias_requires_mapping_before_reuse() {
        let mut mappings = std::collections::HashMap::new();
        let unmapped = encode_publish_topic_alias("", b"x", 1);
        let (_, body_start, packet_len) = try_packet_bounds(&unmapped).unwrap().unwrap();
        assert!(
            validate_server_topic_alias(&unmapped[body_start..packet_len], 8, &mut mappings)
                .is_err()
        );

        let mapping = encode_publish_topic_alias("bench/native/1", b"x", 1);
        let (_, body_start, packet_len) = try_packet_bounds(&mapping).unwrap().unwrap();
        assert!(
            validate_server_topic_alias(&mapping[body_start..packet_len], 8, &mut mappings)
                .unwrap()
                .is_some()
        );
        let (_, body_start, packet_len) = try_packet_bounds(&unmapped).unwrap().unwrap();
        assert!(
            validate_server_topic_alias(&unmapped[body_start..packet_len], 8, &mut mappings)
                .unwrap()
                .is_some()
        );
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
    #[test]
    fn fast_qos0_reader_batches_stable_frames_and_stops_on_change() {
        let packet = encode_publish("bench/native/1", b"payload", 0, None);
        let (_, body_start, packet_len) = try_packet_bounds(&packet)
            .unwrap()
            .expect("complete packet");
        let mut layout = FastQos0ReadLayout::default();
        layout.cache(&packet[..body_start], packet_len);

        let mut block = Vec::new();
        block.extend_from_slice(&packet);
        block.extend_from_slice(&packet);
        block.extend_from_slice(&packet);
        let batch = scan_qos0_read_batch(&block, &layout);
        assert_eq!(batch.messages, 3);
        assert_eq!(batch.bytes, packet_len * 3);

        let mut changed = packet.clone();
        changed[0] = 0x31; // RETAIN=1 changes the fixed header and must end the fast run.
        let mut mixed = Vec::new();
        mixed.extend_from_slice(&packet);
        mixed.extend_from_slice(&changed);
        mixed.extend_from_slice(&packet);
        let batch = scan_qos0_read_batch(&mixed, &layout);
        assert_eq!(batch.messages, 1);
        assert_eq!(batch.bytes, packet_len);
    }
}
