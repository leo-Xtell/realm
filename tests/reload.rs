#![cfg(unix)]

use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream, UdpSocket};
use std::path::Path;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

fn spawn_backend(tag: u8) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind backend");
    let port = listener.local_addr().expect("backend local_addr").port();
    thread::spawn(move || loop {
        match listener.accept() {
            Ok((stream, _)) => {
                thread::spawn(move || {
                    let mut stream = stream;
                    if stream.write_all(&[tag]).is_err() {
                        return;
                    }
                    let _ = stream.flush();
                    let mut scratch = [0u8; 1024];
                    loop {
                        match stream.read(&mut scratch) {
                            Ok(0) => break,
                            Ok(_) => continue,
                            Err(_) => break,
                        }
                    }
                });
            }
            Err(_) => break,
        }
    });
    port
}

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind free_port");
    listener.local_addr().expect("free_port local_addr").port()
}

fn write_config(path: &Path, listen: u16, remote: u16) {
    let contents = format!(
        "[[endpoints]]\nlisten = \"127.0.0.1:{}\"\nremote = \"127.0.0.1:{}\"\n",
        listen, remote
    );
    fs::write(path, contents).expect("write config");
}

fn send_sighup(child: &Child) {
    unsafe {
        libc::kill(child.id() as libc::pid_t, libc::SIGHUP);
    }
}

fn probe(listen: u16, timeout: Duration) -> std::io::Result<u8> {
    let mut stream = TcpStream::connect(("127.0.0.1", listen))?;
    stream.set_read_timeout(Some(timeout))?;
    let mut buf = [0u8; 1];
    stream.read_exact(&mut buf)?;
    Ok(buf[0])
}

fn wait_for_tag(listen: u16, expected: u8, overall_timeout: Duration) -> bool {
    let deadline = Instant::now() + overall_timeout;
    loop {
        if let Ok(tag) = probe(listen, Duration::from_millis(500)) {
            if tag == expected {
                return true;
            }
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn sighup_reloads_endpoints() {
    let a = spawn_backend(b'A');
    let b = spawn_backend(b'B');

    let listen = free_port();

    let cfg_path = std::env::temp_dir().join(format!("realm_reload_test_{}.toml", listen));
    write_config(&cfg_path, listen, a);

    let mut child = Command::new(env!("CARGO_BIN_EXE_realm"))
        .arg("-c")
        .arg(&cfg_path)
        .spawn()
        .expect("spawn realm");

    if !wait_for_tag(listen, b'A', Duration::from_secs(10)) {
        child.kill().ok();
        child.wait().ok();
        fs::remove_file(&cfg_path).ok();
        panic!("realm did not start up and route to backend A within timeout");
    }

    let held_result = (|| -> std::io::Result<TcpStream> {
        let mut held = TcpStream::connect(("127.0.0.1", listen))?;
        held.set_read_timeout(Some(Duration::from_secs(2)))?;
        let mut buf = [0u8; 1];
        held.read_exact(&mut buf)?;
        if buf[0] != b'A' {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "held connection not served by A",
            ));
        }
        Ok(held)
    })();

    let mut held = match held_result {
        Ok(held) => held,
        Err(e) => {
            child.kill().ok();
            child.wait().ok();
            fs::remove_file(&cfg_path).ok();
            panic!("failed to open long-lived connection to A: {}", e);
        }
    };

    write_config(&cfg_path, listen, b);
    send_sighup(&child);

    let reloaded = wait_for_tag(listen, b'B', Duration::from_secs(10));

    let mut drop_buf = [0u8; 64];
    held.set_read_timeout(Some(Duration::from_secs(2))).ok();
    let drop_result = held.read(&mut drop_buf);

    child.kill().ok();
    child.wait().ok();
    fs::remove_file(&cfg_path).ok();

    assert!(
        reloaded,
        "reload did not take effect: new connections were not routed to backend B within timeout"
    );

    match drop_result {
        Ok(0) => {}
        Ok(n) => {
            let mut remaining = held.read(&mut drop_buf);
            loop {
                match remaining {
                    Ok(0) => break,
                    Ok(_) => remaining = held.read(&mut drop_buf),
                    Err(e) => {
                        assert!(
                            e.kind() != std::io::ErrorKind::WouldBlock
                                && e.kind() != std::io::ErrorKind::TimedOut,
                            "held connection still alive after reload (read {} bytes then blocked): brutal-drop guarantee violated",
                            n
                        );
                        break;
                    }
                }
            }
        }
        Err(e) => {
            assert!(
                e.kind() != std::io::ErrorKind::WouldBlock
                    && e.kind() != std::io::ErrorKind::TimedOut,
                "held connection was not torn down after reload (read timed out / would block): brutal-drop guarantee violated, error was {:?}",
                e.kind()
            );
        }
    }
}

// A backend that writes its tag then continuously streams bytes, so any relay
// carrying it is actively copying data (i.e. its spawned relay task is live and
// dereferencing endpoint state) at the moment a reload fires.
fn spawn_streaming_backend(tag: u8) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind streaming backend");
    let port = listener.local_addr().expect("backend local_addr").port();
    thread::spawn(move || {
        while let Ok((stream, _)) = listener.accept() {
            thread::spawn(move || {
                let mut stream = stream;
                let _ = stream.set_nodelay(true);
                let buf = [tag; 512];
                // First byte is the tag (probe()/wait_for_tag rely on it),
                // then keep streaming until the peer goes away.
                loop {
                    if stream.write_all(&buf).is_err() {
                        break;
                    }
                    if stream.flush().is_err() {
                        break;
                    }
                    thread::sleep(Duration::from_millis(1));
                }
            });
        }
    });
    port
}

// Regression test for the hot-reload use-after-free: reload aborts run_tcp,
// freeing the stack frame that its detached per-connection relay tasks pointed
// into via `trick::Ref`. With live traffic in flight during the reload, those
// tasks dereference freed memory -> SIGSEGV. This drives active connections
// while hammering reloads and asserts the process never dies from a signal.
//
// A UAF is racy, so this is probabilistic without a sanitizer; run under ASan
// for a deterministic verdict:
//   RUSTFLAGS="-Zsanitizer=address" cargo +nightly test -Zbuild-std \
//     --target x86_64-unknown-linux-gnu --no-default-features \
//     --features default-slim --test reload reload_under_load_survives
#[test]
fn reload_under_load_survives() {
    use std::os::unix::process::ExitStatusExt;

    let a = spawn_streaming_backend(b'A');
    let b = spawn_streaming_backend(b'B');
    let listen = free_port();

    let cfg_path = std::env::temp_dir().join(format!("realm_reload_load_{}.toml", listen));
    write_config(&cfg_path, listen, a);

    let mut child = Command::new(env!("CARGO_BIN_EXE_realm"))
        .arg("-c")
        .arg(&cfg_path)
        .spawn()
        .expect("spawn realm");

    let cleanup = |child: &mut Child| {
        child.kill().ok();
        child.wait().ok();
        fs::remove_file(&cfg_path).ok();
    };

    if !wait_for_tag(listen, b'A', Duration::from_secs(10)) {
        cleanup(&mut child);
        panic!("realm did not start up and route to backend A within timeout");
    }

    // Keep a pool of active connections draining bytes in background threads.
    // stop flips to true at teardown so the reader threads exit.
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut readers = Vec::new();
    for _ in 0..24 {
        if let Ok(stream) = TcpStream::connect(("127.0.0.1", listen)) {
            stream.set_read_timeout(Some(Duration::from_millis(100))).ok();
            let stop = std::sync::Arc::clone(&stop);
            readers.push(thread::spawn(move || {
                let mut stream = stream;
                let mut scratch = [0u8; 4096];
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    match stream.read(&mut scratch) {
                        Ok(0) => break,
                        Ok(_) => continue,
                        Err(e)
                            if e.kind() == std::io::ErrorKind::WouldBlock
                                || e.kind() == std::io::ErrorKind::TimedOut =>
                        {
                            continue
                        }
                        Err(_) => break,
                    }
                }
            }));
        }
    }

    // Hammer reloads while traffic flows. Open a few fresh connections around
    // each reload to widen the window where a relay task races the frame free.
    let mut crashed = None;
    for i in 0..40 {
        let backend = if i % 2 == 0 { b } else { a };
        write_config(&cfg_path, listen, backend);
        send_sighup(&child);
        for _ in 0..6 {
            let _ = TcpStream::connect(("127.0.0.1", listen))
                .map(|s| s.set_read_timeout(Some(Duration::from_millis(20))));
        }
        thread::sleep(Duration::from_millis(25));
        if let Ok(Some(status)) = child.try_wait() {
            crashed = Some(status);
            break;
        }
    }

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for r in readers {
        r.join().ok();
    }

    if let Some(status) = crashed {
        cleanup(&mut child);
        panic!(
            "realm died during reload-under-load: {:?} (signal={:?}) — use-after-free regression",
            status,
            status.signal()
        );
    }

    // Should still be serving after the storm.
    let still_serving = wait_for_tag(listen, b'A', Duration::from_secs(5))
        || wait_for_tag(listen, b'B', Duration::from_secs(5));
    let final_status = child.try_wait().expect("try_wait");
    cleanup(&mut child);

    assert!(
        final_status.is_none(),
        "realm exited during reload-under-load: {:?}",
        final_status
    );
    assert!(
        still_serving,
        "realm stopped routing after the reload storm"
    );
}

fn write_udp_config(path: &Path, listen: u16, remote: u16) {
    // UDP-only endpoint (no_tcp) so the reload path exercises run_udp. Short
    // udp_timeout keeps associations churning during the reload storm.
    let contents = format!(
        "[network]\nno_tcp = true\nuse_udp = true\nudp_timeout = 3\n\n\
         [[endpoints]]\nlisten = \"127.0.0.1:{}\"\nremote = \"127.0.0.1:{}\"\n",
        listen, remote
    );
    fs::write(path, contents).expect("write udp config");
}

// A UDP backend that, once a source pokes it, continuously streams tag-bytes
// back to every peer it has seen. This keeps realm's per-association `send_back`
// tasks live (recv on the relay socket -> send on the listen socket) at the
// moment a reload fires — the UDP analogue of the TCP UAF window.
fn spawn_udp_streaming_backend(tag: u8, stop: Arc<AtomicBool>) -> u16 {
    let sock = UdpSocket::bind("127.0.0.1:0").expect("bind udp backend");
    let port = sock.local_addr().expect("udp backend local_addr").port();
    sock.set_read_timeout(Some(Duration::from_millis(20))).ok();
    thread::spawn(move || {
        let peers: Mutex<Vec<std::net::SocketAddr>> = Mutex::new(Vec::new());
        let mut buf = [0u8; 2048];
        let out = [tag; 256];
        while !stop.load(Ordering::Relaxed) {
            if let Ok((_, src)) = sock.recv_from(&mut buf) {
                let mut p = peers.lock().unwrap();
                if !p.contains(&src) {
                    p.push(src);
                }
            }
            let snapshot = peers.lock().unwrap().clone();
            for peer in snapshot {
                for _ in 0..4 {
                    let _ = sock.send_to(&out, peer);
                }
            }
        }
    });
    port
}

fn udp_probe_tag(listen: u16, expected: u8, overall_timeout: Duration) -> bool {
    let deadline = Instant::now() + overall_timeout;
    loop {
        if let Ok(client) = UdpSocket::bind("127.0.0.1:0") {
            client.set_read_timeout(Some(Duration::from_millis(300))).ok();
            let _ = client.send_to(&[0u8; 1], ("127.0.0.1", listen));
            let mut buf = [0u8; 1];
            if let Ok((n, _)) = client.recv_from(&mut buf) {
                if n >= 1 && buf[0] == expected {
                    return true;
                }
            }
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(50));
    }
}

// UDP counterpart of reload_under_load_survives: run_udp is also aborted on
// reload, and its detached per-association `send_back` tasks held `trick::Ref`s
// into the freed frame (listen socket / sockmap / conn_opts). Drives active UDP
// associations while hammering reloads and asserts the process never dies from
// a signal. Run under ASan for a deterministic verdict (see the TCP test's
// doc comment for the invocation; use --test reload).
#[test]
fn udp_reload_under_load_survives() {
    use std::os::unix::process::ExitStatusExt;

    let stop = Arc::new(AtomicBool::new(false));
    let a = spawn_udp_streaming_backend(b'A', Arc::clone(&stop));
    let b = spawn_udp_streaming_backend(b'B', Arc::clone(&stop));
    let listen = free_port();

    let cfg_path = std::env::temp_dir().join(format!("realm_reload_udp_{}.toml", listen));
    write_udp_config(&cfg_path, listen, a);

    let mut child = Command::new(env!("CARGO_BIN_EXE_realm"))
        .arg("-c")
        .arg(&cfg_path)
        .spawn()
        .expect("spawn realm");

    let cleanup = |child: &mut Child| {
        child.kill().ok();
        child.wait().ok();
        fs::remove_file(&cfg_path).ok();
    };

    if !udp_probe_tag(listen, b'A', Duration::from_secs(10)) {
        stop.store(true, Ordering::Relaxed);
        cleanup(&mut child);
        panic!("realm did not start up and relay UDP to backend A within timeout");
    }

    // Keep a pool of active UDP clients that poke realm and drain replies,
    // so associations (and their send_back tasks) stay live across reloads.
    let mut clients = Vec::new();
    for _ in 0..16 {
        let stop = Arc::clone(&stop);
        clients.push(thread::spawn(move || {
            let client = match UdpSocket::bind("127.0.0.1:0") {
                Ok(c) => c,
                Err(_) => return,
            };
            client.set_read_timeout(Some(Duration::from_millis(50))).ok();
            let mut scratch = [0u8; 2048];
            while !stop.load(Ordering::Relaxed) {
                let _ = client.send_to(&[0u8; 1], ("127.0.0.1", listen));
                for _ in 0..8 {
                    if client.recv_from(&mut scratch).is_err() {
                        break;
                    }
                }
                thread::sleep(Duration::from_millis(5));
            }
        }));
    }

    let mut crashed = None;
    for i in 0..40 {
        let backend = if i % 2 == 0 { b } else { a };
        write_udp_config(&cfg_path, listen, backend);
        send_sighup(&child);
        for _ in 0..6 {
            if let Ok(c) = UdpSocket::bind("127.0.0.1:0") {
                let _ = c.send_to(&[0u8; 1], ("127.0.0.1", listen));
            }
        }
        thread::sleep(Duration::from_millis(25));
        if let Ok(Some(status)) = child.try_wait() {
            crashed = Some(status);
            break;
        }
    }

    if let Some(status) = crashed {
        stop.store(true, Ordering::Relaxed);
        for c in clients {
            c.join().ok();
        }
        cleanup(&mut child);
        panic!(
            "realm died during UDP reload-under-load: {:?} (signal={:?}) — use-after-free regression",
            status,
            status.signal()
        );
    }

    // Probe while the backends are still alive (they, like the client pool, are
    // gated on `stop`), then tear everything down.
    let still_serving = udp_probe_tag(listen, b'A', Duration::from_secs(5))
        || udp_probe_tag(listen, b'B', Duration::from_secs(5));
    let final_status = child.try_wait().expect("try_wait");

    stop.store(true, Ordering::Relaxed);
    for c in clients {
        c.join().ok();
    }
    cleanup(&mut child);

    assert!(
        final_status.is_none(),
        "realm exited during UDP reload-under-load: {:?}",
        final_status
    );
    assert!(still_serving, "realm stopped relaying UDP after the reload storm");
}
