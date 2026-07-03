#![cfg(unix)]

use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command};
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
