use std::fs;

use agent_economy_evidence_store::{
    CreateDisposition, EvidenceContext, EvidenceObject, EvidenceStore, FilesystemEvidenceStore,
    StoreError,
};

fn context() -> EvidenceContext {
    EvidenceContext::new("x402-runtime", "2026-09-28").expect("valid context")
}

fn store(root: &std::path::Path) -> FilesystemEvidenceStore {
    let root = fs::canonicalize(root).expect("canonical evidence root");
    FilesystemEvidenceStore::open(root).expect("open evidence store")
}

#[test]
fn writes_content_addressed_evidence_and_replays_verified_bytes() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let store = store(temporary.path());
    let payload = br#"{"status":402}"#;

    let receipt = store.create(&context(), payload).expect("create evidence");

    assert_eq!(CreateDisposition::Created, receipt.disposition);
    assert_eq!(
        "evidence/x402-runtime/2026-09-28/sha256/a7/a73a1d85b2980fa723789e2cb984993dff57aa1f3e9c3362db3da78336f157ee",
        receipt.object.name()
    );
    assert_eq!(
        payload,
        store
            .read(&receipt.object)
            .expect("replay evidence")
            .as_slice()
    );
}

#[test]
fn repeated_identical_write_is_an_idempotent_noop() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let store = store(temporary.path());
    let payload = b"same immutable evidence";

    let first = store.create(&context(), payload).expect("first create");
    let second = store
        .create(&context(), payload)
        .expect("idempotent create");

    assert_eq!(CreateDisposition::Created, first.disposition);
    assert_eq!(CreateDisposition::AlreadyPresent, second.disposition);
    assert_eq!(first.object, second.object);
}

#[test]
fn replay_rejects_tampered_content() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let store = store(temporary.path());
    let receipt = store
        .create(&context(), b"trusted bytes")
        .expect("create evidence");
    let object_path = temporary.path().join(receipt.object.name());
    fs::write(object_path, b"different bytes").expect("tamper fixture");

    let error = store
        .read(&receipt.object)
        .expect_err("tampering must fail closed");

    assert!(matches!(error, StoreError::DigestMismatch { .. }));
}

#[test]
fn rejects_unsafe_source_and_date_prefixes() {
    assert!(EvidenceContext::new("../private", "2026-09-28").is_err());
    assert!(EvidenceContext::new("x402", "2026/09/28").is_err());
    assert!(EvidenceContext::new("X402", "2026-09-28").is_err());
}

#[test]
fn persisted_object_name_can_be_replayed_after_process_restart() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let name = {
        let store = store(temporary.path());
        store
            .create(&context(), b"durable evidence")
            .expect("create evidence")
            .object
            .name()
            .to_owned()
    };

    let object = EvidenceObject::parse(&name).expect("parse persisted object name");
    let restarted_store = store(temporary.path());

    assert_eq!(
        b"durable evidence",
        restarted_store
            .read(&object)
            .expect("replay after restart")
            .as_slice()
    );
}

#[cfg(unix)]
#[test]
fn rejects_symlinked_path_components_beneath_the_evidence_root() {
    use std::os::unix::fs::symlink;

    let temporary = tempfile::tempdir().expect("temporary directory");
    let root = temporary.path().join("root");
    let outside = temporary.path().join("outside");
    fs::create_dir_all(&root).expect("create root");
    fs::create_dir_all(&outside).expect("create outside directory");
    symlink(&outside, root.join("evidence")).expect("install adversarial symlink");
    let store = store(&root);

    let error = store
        .create(&context(), b"must stay contained")
        .expect_err("symlink escape must fail closed");

    assert!(matches!(error, StoreError::UnsafePath { .. }));
    assert_eq!(0, fs::read_dir(&outside).expect("read outside").count());
}

#[cfg(unix)]
#[test]
fn rejects_symlinked_ancestors_of_the_evidence_root() {
    use std::os::unix::fs::symlink;

    let temporary = tempfile::tempdir().expect("temporary directory");
    let temporary_root = fs::canonicalize(temporary.path()).expect("canonical temporary root");
    let outside = temporary_root.join("outside");
    let linked_parent = temporary_root.join("linked-parent");
    fs::create_dir(&outside).expect("create outside directory");
    symlink(&outside, &linked_parent).expect("install adversarial ancestor");

    let error = FilesystemEvidenceStore::open(linked_parent.join("evidence"))
        .expect_err("symlinked ancestor must fail closed");

    assert!(matches!(error, StoreError::UnsafePath { .. }));
    assert_eq!(0, fs::read_dir(&outside).expect("read outside").count());
}

#[cfg(unix)]
#[test]
fn pins_the_opened_root_when_its_ambient_path_is_replaced() {
    use std::os::unix::fs::symlink;

    let temporary = tempfile::tempdir().expect("temporary directory");
    let root = temporary.path().join("root");
    let retained_root = temporary.path().join("retained-root");
    let outside = temporary.path().join("outside");
    fs::create_dir(&root).expect("create root");
    fs::create_dir(&outside).expect("create outside directory");
    let store = store(&root);

    fs::rename(&root, &retained_root).expect("move opened root");
    symlink(&outside, &root).expect("replace ambient path");
    let receipt = store
        .create(&context(), b"capability-bound evidence")
        .expect("create through retained directory handle");

    assert!(retained_root.join(receipt.object.name()).is_file());
    assert_eq!(0, fs::read_dir(&outside).expect("read outside").count());
}
