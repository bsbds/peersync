use std::{
    path::Path,
    process::{Child, Command, Output, Stdio},
    time::{Duration, Instant},
};

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn cli() -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_peersync"));
    c.env("RUST_LOG", "warn");
    c
}
fn key(path: &Path) -> String {
    let output = cli().arg("keygen").arg("--out").arg(path).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}
fn wait(process: &mut Process) {
    let start = Instant::now();
    loop {
        if process.0.try_wait().unwrap().is_some() {
            return;
        }
        assert!(
            start.elapsed() < Duration::from_secs(20),
            "CLI process timed out"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}
fn output(mut process: Process) -> Output {
    use std::io::Read;
    wait(&mut process);
    let status = process.0.try_wait().unwrap().unwrap();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    process
        .0
        .stdout
        .take()
        .unwrap()
        .read_to_end(&mut stdout)
        .unwrap();
    process
        .0
        .stderr
        .take()
        .unwrap()
        .read_to_end(&mut stderr)
        .unwrap();
    Output {
        status,
        stdout,
        stderr,
    }
}
fn ready(process: &mut Process) -> serde_json::Value {
    use std::io::Read;
    let mut stdout = process.0.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let result = (|| -> std::io::Result<serde_json::Value> {
            // Read exactly one record; leave subsequent report bytes on the pipe.
            let mut line = Vec::new();
            loop {
                let mut byte = [0u8];
                stdout.read_exact(&mut byte)?;
                if byte[0] == b'\n' {
                    break;
                }
                if line.len() >= 65536 {
                    return Err(std::io::Error::other("oversized readiness record"));
                }
                line.push(byte[0]);
            }
            Ok(serde_json::from_slice(&line)?)
        })();
        let _ = tx.send((stdout, result));
    });
    let (stdout, result) = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("receiver did not announce readiness");
    process.0.stdout = Some(stdout);
    let record = result.unwrap();
    assert_eq!(record["type"], "ready");
    assert!(!record["addresses"].as_array().unwrap().is_empty());
    record
}

#[test]
fn two_real_cli_processes_sync_and_report() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path();
    let source = p.join("source");
    let dest = p.join("destination");
    std::fs::create_dir(&source).unwrap();
    std::fs::create_dir(&dest).unwrap();
    std::fs::write(source.join("file"), "hello peersync").unwrap();
    std::fs::write(dest.join("extra"), "remove me").unwrap();
    let sender_id = key(&p.join("sender.key"));
    let receiver_id = key(&p.join("receiver.key"));
    let mut receiver = Process(
        cli()
            .arg("receive")
            .arg(&dest)
            .args([
                "--allow-peer",
                &sender_id,
                "--direct",
                "--json",
                "--timeout",
                "15",
                "--key",
            ])
            .arg(p.join("receiver.key"))
            .args(["--bind", "127.0.0.1:0"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let address = ready(&mut receiver);
    assert_eq!(address["endpoint_id"], receiver_id);
    let sender = Process(
        cli()
            .arg("send")
            .arg(&source)
            .args(["--direct", "--delete", "--times", "--json", "--key"])
            .arg(p.join("sender.key"))
            .args([
                "--peer",
                &receiver_id,
                "--addr",
                address["addresses"][0].as_str().unwrap(),
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let sent = output(sender);
    let received = output(receiver);
    assert!(
        sent.status.success(),
        "{}",
        String::from_utf8_lossy(&sent.stderr)
    );
    assert!(
        received.status.success(),
        "{}",
        String::from_utf8_lossy(&received.stderr)
    );
    assert_eq!(std::fs::read(dest.join("file")).unwrap(), b"hello peersync");
    assert!(!dest.join("extra").exists());
    for result in [sent, received] {
        let report: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
        assert_eq!(report["type"], "report");
        assert_eq!(report["ok"], true);
    }
}

#[test]
fn unauthorized_identity_cannot_mutate_destination() {
    // Check both directions of authentication: an unauthorized sender, and a
    // mismatched receiver identity despite the correct IP/port hint.
    for wrong_peer in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path();
        let source = p.join("src");
        let dest = p.join("dst");
        std::fs::create_dir(&source).unwrap();
        std::fs::create_dir(&dest).unwrap();
        std::fs::write(source.join("file"), "new").unwrap();
        std::fs::write(dest.join("file"), "old").unwrap();
        let allowed = key(&p.join("allowed"));
        let wrong_id = key(&p.join("wrong"));
        let receiver_id = key(&p.join("receiver"));
        let mut receiver = Process(
            cli()
                .arg("receive")
                .arg(&dest)
                .args([
                    "--allow-peer",
                    &allowed,
                    "--direct",
                    "--json",
                    "--timeout",
                    "2",
                    "--key",
                ])
                .arg(p.join("receiver"))
                .args(["--bind", "127.0.0.1:0"])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap(),
        );
        let address = ready(&mut receiver);
        let sender = Process(
            cli()
                .arg("send")
                .arg(&source)
                .args(["--direct", "--timeout", "2", "--key"])
                .arg(p.join(if wrong_peer { "allowed" } else { "wrong" }))
                .args([
                    "--peer",
                    if wrong_peer { &wrong_id } else { &receiver_id },
                    "--addr",
                    address["addresses"][0].as_str().unwrap(),
                ])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap(),
        );
        assert!(!output(sender).status.success());
        assert!(!output(receiver).status.success());
        assert_eq!(std::fs::read(dest.join("file")).unwrap(), b"old");
    }
}

#[test]
fn keygen_never_overwrites_existing_key() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("key");
    key(&path);
    let before = std::fs::read(&path).unwrap();
    assert!(
        !cli()
            .arg("keygen")
            .arg("--out")
            .arg(&path)
            .output()
            .unwrap()
            .status
            .success()
    );
    assert_eq!(std::fs::read(&path).unwrap(), before);
}
