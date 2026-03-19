use assert_cmd::Command;
use std::fs;

use akc_protocol::ChunkRecord;

fn mk_file(path: &std::path::Path, contents: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    fs::write(path, contents.as_bytes()).unwrap();
}

#[test]
fn cli_messaging_jsonl_is_valid_and_deterministic() {
    let td = tempfile::tempdir().unwrap();
    let root = td.path();

    let para = "a".repeat(1000);
    let content = [para.clone(), para.clone(), para.clone()].join("\n\n");

    mk_file(&root.join("b.jsonl"), &content);
    mk_file(&root.join("a.jsonl"), &content);
    mk_file(&root.join("sub/z.txt"), &content);

    let mut cmd1 = Command::cargo_bin("akc-ingest").unwrap();
    cmd1.args([
        "--jsonl",
        "messaging",
        "--tenant-id",
        "tenant_a",
        "--run-id",
        "run_1",
    ]);
    cmd1.arg(root.to_string_lossy().to_string());
    let out1 = cmd1.assert().success().get_output().stdout.clone();
    let s1 = String::from_utf8(out1).unwrap();

    let mut cmd2 = Command::cargo_bin("akc-ingest").unwrap();
    cmd2.args([
        "--jsonl",
        "messaging",
        "--tenant-id",
        "tenant_a",
        "--run-id",
        "run_1",
    ]);
    cmd2.arg(root.to_string_lossy().to_string());
    let out2 = cmd2.assert().success().get_output().stdout.clone();
    let s2 = String::from_utf8(out2).unwrap();

    assert_eq!(
        s1, s2,
        "CLI messaging jsonl output must be stable across runs"
    );

    let lines: Vec<&str> = s1.lines().filter(|l| !l.trim().is_empty()).collect();
    assert!(!lines.is_empty(), "expected at least one JSONL record");

    let mut records: Vec<ChunkRecord> = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        let rec: ChunkRecord =
            serde_json::from_str(line).unwrap_or_else(|e| panic!("line {i} invalid JSON: {e}"));
        records.push(rec);
    }

    for r in &records {
        assert!(!r.source_id.is_empty());
        assert!(!r.chunk_id.is_empty());
        assert!(!r.fingerprint.is_empty());
        assert!(r.metadata.contains_key("path"));
        assert!(r.metadata.contains_key("chunk_index"));
    }

    let source_ids: Vec<String> = records.iter().map(|r| r.source_id.clone()).collect();
    let mut sorted: Vec<String> = source_ids.clone();
    sorted.sort();
    assert_eq!(
        source_ids, sorted,
        "records must be emitted in stable source_id order"
    );
}

#[test]
fn cli_api_jsonl_is_valid_and_deterministic() {
    let td = tempfile::tempdir().unwrap();
    let root = td.path();

    let para = "b".repeat(1000);
    let content = [para.clone(), para.clone(), para.clone()].join("\n\n");

    mk_file(&root.join("b.json"), &content);
    mk_file(&root.join("a.json"), &content);

    let mut cmd1 = Command::cargo_bin("akc-ingest").unwrap();
    cmd1.args([
        "--jsonl",
        "api",
        "--tenant-id",
        "tenant_a",
        "--run-id",
        "run_1",
    ]);
    cmd1.arg(root.to_string_lossy().to_string());
    let out1 = cmd1.assert().success().get_output().stdout.clone();
    let s1 = String::from_utf8(out1).unwrap();

    let mut cmd2 = Command::cargo_bin("akc-ingest").unwrap();
    cmd2.args([
        "--jsonl",
        "api",
        "--tenant-id",
        "tenant_a",
        "--run-id",
        "run_1",
    ]);
    cmd2.arg(root.to_string_lossy().to_string());
    let out2 = cmd2.assert().success().get_output().stdout.clone();
    let s2 = String::from_utf8(out2).unwrap();

    assert_eq!(s1, s2, "CLI api jsonl output must be stable across runs");

    let lines: Vec<&str> = s1.lines().filter(|l| !l.trim().is_empty()).collect();
    assert!(!lines.is_empty(), "expected at least one JSONL record");

    let mut records: Vec<ChunkRecord> = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        let rec: ChunkRecord =
            serde_json::from_str(line).unwrap_or_else(|e| panic!("line {i} invalid JSON: {e}"));
        records.push(rec);
    }

    for r in &records {
        assert!(!r.source_id.is_empty());
        assert!(!r.chunk_id.is_empty());
        assert!(!r.fingerprint.is_empty());
        assert!(r.metadata.contains_key("path"));
        assert!(r.metadata.contains_key("chunk_index"));
    }

    let source_ids: Vec<String> = records.iter().map(|r| r.source_id.clone()).collect();
    let mut sorted: Vec<String> = source_ids.clone();
    sorted.sort();
    assert_eq!(
        source_ids, sorted,
        "records must be emitted in stable source_id order"
    );
}
