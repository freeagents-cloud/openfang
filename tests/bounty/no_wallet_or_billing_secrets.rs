use openfang_worker::billing_policy::{
    detect_forbidden_sandbox_env, FORBIDDEN_SANDBOX_ENV_VARS,
};
use std::path::Path;

#[test]
fn worker_configs_and_dockerfile_contain_no_wallet_or_billing_secrets() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let files = [
        manifest_dir.join("../../agents/freeagents-bounty.toml"),
        manifest_dir.join("../../configs/bounty-worker.example.toml"),
        manifest_dir.join("../../docker/Dockerfile.worker"),
    ];

    for path in files {
        let content = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
        for forbidden in FORBIDDEN_SANDBOX_ENV_VARS {
            assert!(
                !content.contains(forbidden),
                "forbidden secret identifier '{forbidden}' found in {}",
                path.display()
            );
        }
        assert!(
            !content.contains("sk_live_") && !content.contains("sk_test_"),
            "Stripe secret key pattern found in {}",
            path.display()
        );
    }

    // Current test process environment must not leak any forbidden sandbox secrets.
    let leaked = detect_forbidden_sandbox_env();
    assert!(
        leaked.is_empty(),
        "forbidden secrets present in worker test environment: {leaked:?}"
    );
}
