//! TCP relay entrance.

mod socket;
mod middle;
mod plain;

#[cfg(feature = "hook")]
mod hook;

#[cfg(feature = "proxy")]
mod proxy;

#[cfg(feature = "transport")]
mod transport;

use std::io::{ErrorKind, Result};
use std::sync::Arc;
use std::time::Duration;

use crate::endpoint::{BindOpts, Endpoint};

use middle::connect_and_relay;

const BIND_MAX_RETRY: u32 = 5;
const BIND_RETRY_BASE: Duration = Duration::from_millis(100);

async fn bind_with_retry(laddr: &std::net::SocketAddr, bind_opts: BindOpts) -> Result<tokio::net::TcpListener> {
    let mut attempt: u32 = 0;
    loop {
        match socket::bind(laddr, bind_opts.clone()) {
            Ok(lis) => return Ok(lis),
            Err(e) => {
                attempt += 1;
                if e.kind() != ErrorKind::AddrInUse || attempt >= BIND_MAX_RETRY {
                    log::error!("[tcp]failed to bind {} after {} attempt(s): {}", laddr, attempt, e);
                    return Err(e);
                }
                let delay = BIND_RETRY_BASE * attempt;
                log::warn!("[tcp]failed to bind {} (attempt {}/{}): {}, retry in {:?}", laddr, attempt, BIND_MAX_RETRY, e, delay);
                tokio::time::sleep(delay).await;
            }
        }
    }
}

/// Launch a tcp relay.
pub async fn run_tcp(endpoint: Endpoint) -> Result<()> {
    let Endpoint {
        laddr,
        raddr,
        bind_opts,
        conn_opts,
        extra_raddrs,
    } = endpoint;

    // Shared per-connection state must be reference-counted, not a raw `Ref`
    // into this stack frame: on reload `run_tcp` is aborted and its frame is
    // freed while detached relay tasks may still be running, so a `Ref` would
    // dangle (use-after-free -> SIGSEGV). `Arc` keeps the data alive until the
    // last in-flight relay finishes; the tasks themselves are still killed by
    // the runtime drop on reload, so the brutal-drop guarantee is unchanged.
    let raddr = Arc::new(raddr);
    let conn_opts = Arc::new(conn_opts);
    let extra_raddrs = Arc::new(extra_raddrs);

    let lis = bind_with_retry(&laddr, bind_opts).await?;
    let keepalive = socket::keepalive::build(&conn_opts);

    loop {
        let (local, addr) = match lis.accept().await {
            Ok(x) => x,
            Err(e) if e.kind() == ErrorKind::ConnectionAborted => {
                log::warn!("[tcp]failed to accept: {}", e);
                continue;
            }
            Err(e) => {
                log::error!("[tcp]failed to accept: {}", e);
                break;
            }
        };

        // ignore error
        let _ = local.set_nodelay(true);
        // set tcp_keepalive
        if let Some(kpa) = &keepalive {
            use socket::keepalive::SockRef;
            SockRef::from(&local).set_tcp_keepalive(kpa)?;
        }

        let raddr = Arc::clone(&raddr);
        let conn_opts = Arc::clone(&conn_opts);
        let extra_raddrs = Arc::clone(&extra_raddrs);
        tokio::spawn(async move {
            let log_raddr = Arc::clone(&raddr);
            match connect_and_relay(local, raddr, conn_opts, extra_raddrs).await {
                Ok(..) => log::debug!("[tcp]{} => {}, finish", addr, log_raddr.as_ref()),
                Err(e) => log::error!("[tcp]{} => {}, error: {}", addr, log_raddr.as_ref(), e),
            }
        });
    }

    Ok(())
}
