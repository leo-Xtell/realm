//! UDP relay entrance.

mod socket;
mod sockmap;
mod middle;
mod batched;

use std::io::Result;
use std::sync::Arc;

use crate::endpoint::Endpoint;

use sockmap::SockMap;
use middle::associate_and_relay;

/// Launch a udp relay.
pub async fn run_udp(endpoint: Endpoint) -> Result<()> {
    let Endpoint {
        laddr,
        raddr,
        bind_opts,
        conn_opts,
        ..
    } = endpoint;

    let lis = socket::bind(&laddr, bind_opts).unwrap_or_else(|e| panic!("[udp]failed to bind {}: {}", laddr, e));

    // Reference-counted rather than a raw `Ref` into this frame: reload aborts
    // `run_udp` and frees the frame while detached `send_back` tasks (spawned
    // per association) may still hold these, so a `Ref` would dangle. `Arc`
    // keeps the listener/opts/sockmap alive until the last `send_back` exits.
    let lis = Arc::new(lis);
    let raddr = Arc::new(raddr);
    let conn_opts = Arc::new(conn_opts);
    let sockmap = Arc::new(SockMap::new());
    loop {
        if let Err(e) = associate_and_relay(&lis, &raddr, &conn_opts, &sockmap).await {
            log::error!("[udp]error: {}", e);
        }
    }
}
