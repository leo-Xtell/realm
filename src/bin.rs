use std::env;
use cfg_if::cfg_if;

use realm::cmd;
use realm::conf::{Config, FullConf, LogConf, DnsConf, EndpointConf, EndpointInfo, CmdOverride};
use realm::ENV_CONFIG;

cfg_if! {
    if #[cfg(feature = "mi-malloc")] {
        use mimalloc::MiMalloc;
        #[global_allocator]
        static GLOBAL: MiMalloc = MiMalloc;
    } else if #[cfg(all(feature = "jemalloc", not(target_env = "msvc")))] {
        use jemallocator::Jemalloc;
        #[global_allocator]
        static GLOBAL: Jemalloc = Jemalloc;
    } else if #[cfg(all(feature = "page-alloc", unix))] {
        use mmap_allocator::MmapAllocator;
        #[global_allocator]
        static GLOBAL: MmapAllocator = MmapAllocator::new();
    }
}

struct ReloadCtx {
    path: String,
    opts: CmdOverride,
}

impl ReloadCtx {
    fn reload(&self) -> FullConf {
        let mut conf = FullConf::from_conf_file(&self.path);
        conf.apply_global_opts().apply_cmd_opts(self.opts.clone());
        conf
    }
}

enum Action {
    Reload,
    Shutdown,
}

fn main() {
    let (conf, reload) = 'blk: {
        if let Ok(conf_str) = env::var(ENV_CONFIG) {
            if let Ok(conf) = FullConf::from_conf_str(&conf_str) {
                break 'blk (conf, None);
            }
        };

        use cmd::CmdInput;
        match cmd::scan() {
            CmdInput::Endpoint(ep, opts) => {
                let mut conf = FullConf::default();
                conf.add_endpoint(ep).apply_global_opts().apply_cmd_opts(opts);
                (conf, None)
            }
            CmdInput::Config(path, opts) => {
                let mut conf = FullConf::from_conf_file(&path);
                conf.apply_global_opts().apply_cmd_opts(opts.clone());
                (conf, Some(ReloadCtx { path, opts }))
            }
            CmdInput::None => std::process::exit(0),
        }
    };

    start_from_conf(conf, reload);
}

fn start_from_conf(full: FullConf, reload: Option<ReloadCtx>) {
    let FullConf {
        log: log_conf,
        dns: dns_conf,
        endpoints: endpoints_conf,
        ..
    } = full;

    setup_log(log_conf);
    setup_dns(dns_conf);
    setup_transport();

    supervise(endpoints_conf, reload);
}

fn supervise(mut endpoints_conf: Vec<EndpointConf>, reload: Option<ReloadCtx>) {
    loop {
        let endpoints: Vec<EndpointInfo> = endpoints_conf
            .into_iter()
            .map(Config::build)
            .inspect(|x| println!("inited: {}", x.endpoint))
            .collect();

        let action = execute(endpoints, reload.is_some());

        match action {
            Action::Reload => {
                let ctx = reload.as_ref().unwrap();
                println!("reload: {}", &ctx.path);
                endpoints_conf = ctx.reload().endpoints;
            }
            Action::Shutdown => break,
        }
    }
}

fn setup_log(log: LogConf) {
    println!("log: {}", &log);

    let (level, output) = log.build();
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}]{}",
                chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
                record.target(),
                record.level(),
                message
            ))
        })
        .level(level)
        .chain(output)
        .apply()
        .unwrap_or_else(|e| panic!("failed to setup logger: {}", &e))
}

fn setup_dns(dns: DnsConf) {
    println!("dns: {}", &dns);

    let (conf, opts) = dns.build();
    realm::core::dns::build_lazy(conf, opts);
}

fn setup_transport() {
    #[cfg(feature = "transport")]
    {
        realm::core::kaminari::install_tls_provider();
    }
}

fn execute(eps: Vec<EndpointInfo>, reloadable: bool) -> Action {
    cfg_if! {
        if #[cfg(feature = "multi-thread")] {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
        } else {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
        }
    }

    let action = rt.block_on(serve(eps, reloadable));
    rt.shutdown_timeout(std::time::Duration::ZERO);
    action
}

async fn serve(endpoints: Vec<EndpointInfo>, reloadable: bool) -> Action {
    use realm::core::tcp::run_tcp;
    use realm::core::udp::run_udp;
    use futures::future::join_all;

    let mut workers = Vec::with_capacity(2 * endpoints.len());

    for EndpointInfo {
        endpoint,
        no_tcp,
        use_udp,
    } in endpoints
    {
        if use_udp {
            workers.push(tokio::spawn(run_udp(endpoint.clone())));
        }

        if !no_tcp {
            workers.push(tokio::spawn(run_tcp(endpoint)));
        }
    }

    workers.shrink_to_fit();

    if reloadable {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut hangup = signal(SignalKind::hangup()).expect("failed to install SIGHUP handler");
            tokio::select! {
                _ = join_all(workers) => Action::Shutdown,
                _ = hangup.recv() => Action::Reload,
            }
        }
        #[cfg(not(unix))]
        {
            join_all(workers).await;
            Action::Shutdown
        }
    } else {
        join_all(workers).await;
        Action::Shutdown
    }
}
