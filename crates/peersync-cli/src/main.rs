use anyhow::{Context, Result, bail};
use clap::{Args, CommandFactory, FromArgMatches, Parser, Subcommand};
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey, endpoint::presets};
use peersync::{
    CancellationToken, Comparison, FilterMode, FilterRule, SymlinkMode, SyncContext, SyncOptions,
    SyncReport, Syncer, TransferMode,
};
use std::{
    io::{Read, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

const ALPN: &[u8] = b"peersync/1";

mod transport;

#[derive(Parser)]
#[command(
    version,
    about = "One-way directory synchronization over authenticated Iroh streams"
)]
struct Cli {
    /// Emit JSON lines for receiver readiness and the final report (logs on stderr).
    #[arg(long, global = true)]
    json: bool,
    /// Overall timeout in seconds. Zero disables it; Ctrl+C still cancels.
    #[arg(long, global = true, default_value_t = 300)]
    timeout: u64,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Create a private identity key without overwriting an existing file.
    Keygen {
        #[arg(long)]
        out: PathBuf,
    },
    /// Accept one operation from an explicitly authorized peer.
    Receive {
        destination: PathBuf,
        #[command(flatten)]
        network: Network,
        /// Sender identity authorized to modify this destination, including delete.
        #[arg(long)]
        allow_peer: EndpointId,
    },
    /// Send directory contents to an authenticated receiver identity.
    Send {
        source: PathBuf,
        #[command(flatten)]
        network: Network,
        /// Receiver identity, obtained through a trusted channel.
        #[arg(long)]
        peer: EndpointId,
        /// Explicit receiver IP:UDP-port; repeatable, required with --direct.
        #[arg(long, required_if_eq("direct", "true"))]
        addr: Vec<SocketAddr>,
        #[command(flatten)]
        options: Options,
    },
}
#[derive(Args)]
struct Network {
    #[arg(long)]
    key: PathBuf,
    /// Disable relays/address lookup (for LAN or isolated tests).
    #[arg(long)]
    direct: bool,
    #[arg(long)]
    bind: Option<SocketAddr>,
}
#[derive(Args)]
struct Options {
    #[arg(long)]
    checksum: bool,
    #[arg(long)]
    whole_file: bool,
    #[arg(long)]
    delete: bool,
    #[arg(long)]
    dry_run: bool,
    #[arg(long)]
    times: bool,
    #[arg(long)]
    perms: bool,
    #[arg(long)]
    links: bool,
    /// Only root-local and nested .gitignore; never combined with explicit rules.
    #[arg(long, conflicts_with_all = ["include", "exclude"])]
    gitignore: bool,
    #[arg(long, action = clap::ArgAction::Append)]
    include: Vec<String>,
    #[arg(long, action = clap::ArgAction::Append)]
    exclude: Vec<String>,
}
impl Options {
    fn library(&self, matches: &clap::ArgMatches) -> SyncOptions {
        let mut options = SyncOptions::default();
        options.comparison = if self.checksum {
            Comparison::Checksum
        } else {
            Comparison::SizeAndMtime
        };
        options.transfer = if self.whole_file {
            TransferMode::WholeFile
        } else {
            TransferMode::Delta
        };
        options.delete = self.delete;
        options.dry_run = self.dry_run;
        options.preserve_mtime = self.times;
        options.preserve_permissions = self.perms;
        options.symlinks = if self.links {
            SymlinkMode::Preserve
        } else {
            SymlinkMode::Skip
        };
        let mut rules = Vec::new();
        // Preserve interleaving of --include and --exclude, not two grouped lists.
        for (name, values, include) in [
            ("include", &self.include, true),
            ("exclude", &self.exclude, false),
        ] {
            if let Some(indices) = matches.indices_of(name) {
                for (index, pattern) in indices.zip(values) {
                    rules.push((
                        index,
                        if include {
                            FilterRule::Include(pattern.clone())
                        } else {
                            FilterRule::Exclude(pattern.clone())
                        },
                    ));
                }
            }
        }
        rules.sort_by_key(|(i, _)| *i);
        options.filter = if self.gitignore {
            FilterMode::GitIgnore
        } else if rules.is_empty() {
            FilterMode::None
        } else {
            FilterMode::Glob(rules.into_iter().map(|(_, r)| r).collect())
        };
        options
    }
}

fn write_key(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    #[cfg(not(unix))]
    {
        bail!("private key creation currently requires Unix permission semantics");
    }
    let mut file = opts
        .open(path)
        .with_context(|| format!("create {}", path.display()))?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}
fn peer_address(peer: EndpointId, addresses: &[SocketAddr]) -> EndpointAddr {
    addresses
        .iter()
        .fold(EndpointAddr::new(peer), |addr, ip| addr.with_ip_addr(*ip))
}

fn print_ready(endpoint: &Endpoint, json: bool) -> Result<()> {
    let addr = endpoint.addr();
    let addresses: Vec<_> = addr.ip_addrs().map(ToString::to_string).collect();
    let mut stdout = std::io::stdout().lock();
    if json {
        writeln!(
            stdout,
            "{}",
            serde_json::json!({
                "type": "ready", "endpoint_id": addr.id.to_string(), "addresses": addresses,
            })
        )?;
    } else {
        writeln!(stdout, "EndpointId: {}", addr.id)?;
        for address in addresses {
            writeln!(stdout, "Address: {address}")?;
        }
    }
    // Consumers may be waiting on a pipe before they can start the sender.
    stdout.flush()?;
    Ok(())
}
fn read_limited(path: &Path, max: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(max as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > max {
        bail!("{} exceeds input limit", path.display());
    }
    Ok(bytes)
}
fn read_key(path: &Path) -> Result<SecretKey> {
    let info = std::fs::symlink_metadata(path)?;
    if !info.is_file() {
        bail!("key must be a regular file, not a symlink");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if info.permissions().mode() & 0o077 != 0 {
            bail!("key file must not be accessible by group or other users");
        }
    }
    let bytes = read_limited(path, 32)?;
    let raw: [u8; 32] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("key must contain 32 raw bytes from peersync keygen"))?;
    Ok(SecretKey::from_bytes(&raw))
}
async fn endpoint(network: &Network) -> Result<Endpoint> {
    let builder = if network.direct {
        Endpoint::builder(presets::Minimal)
    } else {
        Endpoint::builder(presets::N0)
    };
    let mut builder = builder
        .secret_key(read_key(&network.key)?)
        .alpns(vec![ALPN.to_vec()]);
    if let Some(bind) = network.bind {
        builder = builder.bind_addr(bind)?;
    }
    let endpoint = builder.bind().await?;
    if !network.direct {
        tokio::time::timeout(Duration::from_secs(15), endpoint.online())
            .await
            .context("waiting for relay readiness")?;
    }
    Ok(endpoint)
}
fn print_report(report: &SyncReport, json: bool, error: Option<&str>) {
    if json {
        println!(
            "{}",
            serde_json::json!({"type":"report","ok":error.is_none(),"error":error,"dry_run":report.dry_run,
            "scanned_entries":report.scanned_entries,"completed_items":report.completed_items,"skipped_items":report.skipped_items,
            "failed_items":report.failed_items,"planned_changes":report.planned_changes,"committed_changes":report.committed_changes,
            "sent_bytes":report.sent_bytes,"received_bytes":report.received_bytes,"peer_completion_confirmed":report.peer_completion_confirmed,
            "effects_may_be_incomplete_or_unknown":report.effects_may_be_incomplete_or_unknown,"omitted_issues":report.omitted_issues})
        );
    } else {
        eprintln!(
            "completed={} skipped={} failed={} committed={} sent={} received={}",
            report.completed_items,
            report.skipped_items,
            report.failed_items,
            report.committed_changes,
            report.sent_bytes,
            report.received_bytes
        );
    }
}

async fn run(cli: Cli, matches: &clap::ArgMatches, cancel: CancellationToken) -> Result<()> {
    if let Command::Keygen { out } = &cli.command {
        let key = SecretKey::generate();
        write_key(out, &key.to_bytes())?;
        println!("{}", key.public());
        return Ok(());
    }
    let network = match &cli.command {
        Command::Send { network, .. } | Command::Receive { network, .. } => network,
        _ => unreachable!(),
    };
    let endpoint = tokio::select! { () = cancel.cancelled() => bail!("cancelled"), result = endpoint(network) => result? };
    let result = async {
        let connection = match &cli.command {
            Command::Send {peer,addr,..} => {
                // Identity pins the receiver even when explicit address hints
                // are supplied. Without hints, N0 performs address lookup.
                let addr = peer_address(*peer, addr);
                tokio::select! { ()=cancel.cancelled()=>bail!("cancelled"), result=endpoint.connect(addr,ALPN)=>result? }
            }
            Command::Receive {allow_peer,..} => {
                print_ready(&endpoint, cli.json)?;
                tracing::info!(id=%endpoint.id(),"ready for authorized peer");
                loop {
                    let incoming = tokio::select! { ()=cancel.cancelled()=>bail!("cancelled"), incoming=endpoint.accept()=>incoming.context("endpoint closed")? };
                    let connection = tokio::select! { ()=cancel.cancelled()=>bail!("cancelled"), c=tokio::time::timeout(Duration::from_secs(10),incoming)=>match c {Ok(Ok(c))=>c,_=>continue} };
                    if connection.remote_id() == *allow_peer { break connection; }
                    tracing::warn!(peer=%connection.remote_id(),"rejected unauthorized peer");
                    connection.close(1u32.into(),b"unauthorized");
                }
            }
            _=>unreachable!(),
        };
        let mut observe=|event: &peersync::SyncEvent| tracing::debug!(?event,"sync progress");
        let mut context=SyncContext::new(cancel.clone()).with_progress(&mut observe);
        let syncer=Syncer::new();
        let (mut tx,mut rx)=tokio::select! { ()=cancel.cancelled()=>bail!("cancelled"), stream=async {
            match &cli.command { Command::Send {..}=>connection.open_bi().await, _=>connection.accept_bi().await }
        }=>stream? };
        let result=match &cli.command {
            Command::Send {source,options,..}=>syncer.send(&mut rx,&mut tx,source,&options.library(matches.subcommand_matches("send").unwrap()),&mut context).await,
            Command::Receive {destination,..}=>syncer.receive(&mut rx,&mut tx,destination,&mut context).await,
            _=>unreachable!(),
        };
        match result {
            Ok(mut report)=> {
                let role = if matches!(&cli.command, Command::Send {..}) {
                    transport::Role::Sender
                } else {
                    transport::Role::Receiver
                };
                if let Err(error) = transport::complete(&connection, &mut tx, role, &cancel).await {
                    let message = format!("transport completion uncertain after sync confirmation: {error:#}");
                    report.effects_may_be_incomplete_or_unknown = true;
                    print_report(&report,cli.json,Some(&message));
                    connection.close(1u32.into(),b"completion failed");
                    bail!(message);
                }
                print_report(&report,cli.json,None);
                Ok(())
            }
            Err(failure)=> {
                let _=tx.reset(1u32.into()); let _=rx.stop(1u32.into());
                print_report(&failure.report,cli.json,Some(&failure.error.to_string()));
                connection.close(1u32.into(),b"sync failed"); Err(failure.into())
            }
        }
    }.await;
    endpoint.close().await;
    result
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let matches = Cli::command().get_matches();
    let cli = Cli::from_arg_matches(&matches).unwrap();
    let cancel = CancellationToken::new();
    let signal = cancel.clone();
    let signal_task = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal.cancel();
        }
    });
    let timeout = cli.timeout;
    let timer_cancel = cancel.clone();
    let timer = tokio::spawn(async move {
        if timeout > 0 {
            tokio::time::sleep(Duration::from_secs(timeout)).await;
            timer_cancel.cancel();
        }
    });
    let result = run(cli, &matches, cancel.clone()).await;
    signal_task.abort();
    timer.abort();
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("peersync: {error:#}");
            std::process::ExitCode::from(if cancel.is_cancelled() { 130 } else { 1 })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mixed_filter_modes_rejected_and_rule_order_retained() {
        let peer = SecretKey::generate().public().to_string();
        assert!(
            Cli::try_parse_from([
                "peersync",
                "send",
                ".",
                "--key",
                "key",
                "--peer",
                &peer,
                "--gitignore",
                "--exclude",
                "*"
            ])
            .is_err()
        );
        let matches = Cli::command()
            .try_get_matches_from([
                "peersync",
                "send",
                ".",
                "--key",
                "key",
                "--peer",
                &peer,
                "--exclude",
                "a",
                "--include",
                "b",
                "--exclude",
                "c",
            ])
            .unwrap();
        let cli = Cli::from_arg_matches(&matches).unwrap();
        if let Command::Send { options, .. } = cli.command {
            assert_eq!(
                options
                    .library(matches.subcommand_matches("send").unwrap())
                    .filter,
                FilterMode::Glob(vec![
                    FilterRule::Exclude("a".into()),
                    FilterRule::Include("b".into()),
                    FilterRule::Exclude("c".into())
                ])
            );
        } else {
            panic!()
        }
    }

    #[test]
    fn connection_arguments_and_address_hints() {
        let id = SecretKey::generate().public();
        let peer = id.to_string();
        let base = ["peersync", "send", ".", "--key", "key", "--peer", &peer];
        assert!(Cli::try_parse_from(base).is_ok());
        assert!(Cli::try_parse_from(base.into_iter().chain(["--direct"])).is_err());
        for flags in [
            vec!["--addr", "127.0.0.1:1234"],
            vec![
                "--direct",
                "--addr",
                "127.0.0.1:1234",
                "--addr",
                "[::1]:5678",
            ],
        ] {
            let cli = Cli::try_parse_from(base.into_iter().chain(flags)).unwrap();
            let Command::Send { peer, addr, .. } = cli.command else {
                panic!()
            };
            let endpoint = peer_address(peer, &addr);
            assert_eq!(endpoint.id, id);
            assert_eq!(endpoint.ip_addrs().count(), addr.len());
        }
        assert!(peer_address(id, &[]).addrs.is_empty());
        assert!(Cli::try_parse_from(["peersync", "send", ".", "--key", "key"]).is_err());
        assert!(Cli::try_parse_from(base.into_iter().chain(["--addr", "invalid"])).is_err());
        assert!(Cli::try_parse_from(base.into_iter().chain(["--ticket", "old.json"])).is_err());
        assert!(
            Cli::try_parse_from([
                "peersync",
                "receive",
                ".",
                "--key",
                "key",
                "--allow-peer",
                &peer,
                "--direct"
            ])
            .is_ok()
        );
        assert!(
            Cli::try_parse_from([
                "peersync",
                "receive",
                ".",
                "--key",
                "key",
                "--allow-peer",
                &peer,
                "--ticket",
                "old.json"
            ])
            .is_err()
        );
    }
}
