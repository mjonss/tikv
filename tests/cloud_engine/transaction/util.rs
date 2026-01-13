// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::mpsc, thread, time::Duration};

use kvproto::kvrpcpb::{Assertion, Op};
use test_cloud_server::{
    ServerCluster,
    client::{ClusterClient, PrewriteExt, RequestOptions, TxnMutations},
};
use tikv_util::{info, time::Instant};
use txn_types::TimeStamp;

use super::helper::*;
use crate::{alloc_node_id_vec, i_to_key, i_to_val};

pub(crate) fn ts(ts: u64) -> TimeStamp {
    TimeStamp::compose(ts, 0)
}

#[derive(Debug, Clone)]
pub(crate) enum TestOperation {
    Get {
        ts: TimeStamp,
        key: Option<Vec<u8>>,
        expected_value: Option<Vec<u8>>,
        check_value: bool,
    },
    PessimisticLock {
        start_ts: TimeStamp,
        for_update_ts: TimeStamp,
        key: Option<Vec<u8>>,
    },
    AsyncPessimisticLock {
        start_ts: TimeStamp,
        for_update_ts: TimeStamp,
        timeout_ms: u64,
        key: Option<Vec<u8>>,
    },
    Sleep {
        duration_ms: u64,
    },
    Prewrite {
        start_ts: TimeStamp,
        expired: bool,
        key: Option<Vec<u8>>,
        for_update_ts: Option<TimeStamp>,
        value: Option<Vec<u8>>,
        assertion: Assertion,
        op_type: Op,
    },
    Commit {
        start_ts: TimeStamp,
        commit_ts: TimeStamp,
        key: Option<Vec<u8>>,
    },
    Rollback {
        start_ts: TimeStamp,
        key: Option<Vec<u8>>,
    },
    CheckTxnStatus {
        lock_ts: TimeStamp,
        caller_start_ts: TimeStamp,
        current_ts: TimeStamp,
        key: Option<Vec<u8>>,
    },
    Concurrent {
        sequences: Vec<ConcurrentSequence>,
    },
}

impl TestOperation {
    pub fn value(mut self, v: &[u8]) -> Self {
        match self {
            TestOperation::Prewrite { ref mut value, .. } => *value = Some(v.to_vec()),
            TestOperation::Get {
                ref mut expected_value,
                ref mut check_value,
                ..
            } => {
                *expected_value = Some(v.to_vec());
                *check_value = true;
            }
            _ => unreachable!("value can only be set for prewrite"),
        }
        self
    }

    pub fn assert(mut self, assertion_val: Assertion) -> Self {
        match self {
            TestOperation::Prewrite {
                ref mut assertion, ..
            } => *assertion = assertion_val,
            _ => unreachable!("assertion can only be set for prewrite"),
        }
        self
    }

    pub fn delete(mut self) -> Self {
        match self {
            TestOperation::Prewrite {
                ref mut op_type, ..
            } => *op_type = Op::Del,
            _ => unreachable!("op_type can only be set for prewrite"),
        }
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ExpectedResult {
    Success,
    Fail,
    Timeout,
    Deadlock,
}

#[derive(Debug, Clone)]
pub(crate) struct TestCase {
    pub(crate) operations: Vec<TestOperation>,
    pub(crate) expected: ExpectedResult,
}

#[derive(Debug, Clone)]
pub(crate) struct ConcurrentSequence {
    pub(crate) operations: Vec<TestOperation>,
    pub(crate) expected: ExpectedResult,
}

macro_rules! get {
    ($ts:expr) => {
        TestOperation::Get {
            ts: ts($ts),
            key: None,
            expected_value: None,
            check_value: false,
        }
    };
    ($ts:expr, $key:expr) => {
        TestOperation::Get {
            ts: ts($ts),
            key: Some($key),
            expected_value: None,
            check_value: false,
        }
    };
}

macro_rules! prewrite {
    ($start_ts:expr,expire) => {
        TestOperation::Prewrite {
            start_ts: ts($start_ts),
            expired: true,
            key: None,
            for_update_ts: None,
            value: None,
            assertion: Assertion::None,
            op_type: Op::Put,
        }
    };
    ($start_ts:expr, $key:expr,expire) => {
        TestOperation::Prewrite {
            start_ts: ts($start_ts),
            expired: true,
            key: Some($key.clone()),
            for_update_ts: None,
            value: None,
            assertion: Assertion::None,
            op_type: Op::Put,
        }
    };
    ($start_ts:expr) => {
        TestOperation::Prewrite {
            start_ts: ts($start_ts),
            expired: false,
            key: None,
            for_update_ts: None,
            value: None,
            assertion: Assertion::None,
            op_type: Op::Put,
        }
    };
    ($start_ts:expr, $key:expr) => {
        TestOperation::Prewrite {
            start_ts: ts($start_ts),
            expired: false,
            key: Some($key.clone()),
            for_update_ts: None,
            value: None,
            assertion: Assertion::None,
            op_type: Op::Put,
        }
    };
}

macro_rules! pessimistic_prewrite {
    ($start_ts:expr, $for_update_ts:expr,expire) => {
        TestOperation::Prewrite {
            start_ts: ts($start_ts),
            expired: true,
            key: None,
            for_update_ts: Some(ts($for_update_ts)),
            value: None,
            assertion: Assertion::None,
            op_type: Op::Put,
        }
    };
    ($start_ts:expr, $for_update_ts:expr, $key:expr,expire) => {
        TestOperation::Prewrite {
            start_ts: ts($start_ts),
            expired: true,
            key: Some($key.clone()),
            for_update_ts: Some(ts($for_update_ts)),
            value: None,
            assertion: Assertion::None,
            op_type: Op::Put,
        }
    };
    ($start_ts:expr, $for_update_ts:expr) => {
        TestOperation::Prewrite {
            start_ts: ts($start_ts),
            expired: false,
            key: None,
            for_update_ts: Some(ts($for_update_ts)),
            value: None,
            assertion: Assertion::None,
            op_type: Op::Put,
        }
    };
    ($start_ts:expr, $for_update_ts:expr, $key:expr) => {
        TestOperation::Prewrite {
            start_ts: ts($start_ts),
            expired: false,
            key: Some($key.clone()),
            for_update_ts: Some(ts($for_update_ts)),
            value: None,
            assertion: Assertion::None,
            op_type: Op::Put,
        }
    };
}

macro_rules! commit {
    ($start_ts:expr, $commit_ts:expr) => {
        TestOperation::Commit {
            start_ts: ts($start_ts),
            commit_ts: ts($commit_ts),
            key: None,
        }
    };
    ($start_ts:expr, $commit_ts:expr, $key:expr) => {
        TestOperation::Commit {
            start_ts: ts($start_ts),
            commit_ts: ts($commit_ts),
            key: Some($key.clone()),
        }
    };
}

macro_rules! lock {
    ($start_ts:expr, $for_update_ts:expr,async, $timeout_ms:expr) => {
        TestOperation::AsyncPessimisticLock {
            start_ts: ts($start_ts),
            for_update_ts: ts($for_update_ts),
            timeout_ms: $timeout_ms,
            key: None,
        }
    };
    ($start_ts:expr, $for_update_ts:expr, $key:expr,async, $timeout_ms:expr) => {
        TestOperation::AsyncPessimisticLock {
            start_ts: ts($start_ts),
            for_update_ts: ts($for_update_ts),
            timeout_ms: $timeout_ms,
            key: Some($key.clone()),
        }
    };
    ($start_ts:expr, $for_update_ts:expr) => {
        TestOperation::PessimisticLock {
            start_ts: ts($start_ts),
            for_update_ts: ts($for_update_ts),
            key: None,
        }
    };
    ($start_ts:expr, $for_update_ts:expr, $key:expr) => {
        TestOperation::PessimisticLock {
            start_ts: ts($start_ts),
            for_update_ts: ts($for_update_ts),
            key: Some($key.clone()),
        }
    };
}

macro_rules! sleep {
    ($duration_ms:expr) => {
        TestOperation::Sleep {
            duration_ms: $duration_ms,
        }
    };
}

macro_rules! check_txn_status {
    ($lock_ts:expr, $caller_start_ts:expr, $current_ts:expr) => {
        TestOperation::CheckTxnStatus {
            lock_ts: ts($lock_ts),
            caller_start_ts: ts($caller_start_ts),
            current_ts: ts($current_ts),
            key: None,
        }
    };
    ($lock_ts:expr, $caller_start_ts:expr, $current_ts:expr, $key:expr) => {
        TestOperation::CheckTxnStatus {
            lock_ts: ts($lock_ts),
            caller_start_ts: ts($caller_start_ts),
            current_ts: ts($current_ts),
            key: Some($key.clone()),
        }
    };
}

macro_rules! rollback {
    ($start_ts:expr) => {
        TestOperation::Rollback {
            start_ts: ts($start_ts),
            key: None,
        }
    };
    ($start_ts:expr, $key:expr) => {
        TestOperation::Rollback {
            start_ts: ts($start_ts),
            key: Some($key.clone()),
        }
    };
}

macro_rules! ok {
    () => {
        ExpectedResult::Success
    };
}

macro_rules! fail {
    () => {
        ExpectedResult::Fail
    };
}

macro_rules! timeout {
    () => {
        ExpectedResult::Timeout
    };
}

macro_rules! deadlock {
    () => {
        ExpectedResult::Deadlock
    };
}

macro_rules! case {
    ($($ops:expr),+ ; $expected:expr) => {
        TestCase {
            operations: vec![$($ops),+],
            expected: $expected,
        }
    };
}

macro_rules! concurrent {
    ($($seq:expr),+) => {
        TestOperation::Concurrent {
            sequences: vec![$($seq),+],
        }
    };
}

macro_rules! sequence {
    ($($ops:expr),+ ; $expected:expr) => {
        ConcurrentSequence {
            operations: vec![$($ops),+],
            expected: $expected,
        }
    };
}

fn execute_operation(
    client: &mut ClusterClient,
    default_key: &[u8],
    operation: &TestOperation,
) -> ExpectedResult {
    match operation {
        TestOperation::Concurrent { sequences } => {
            let mut handles = vec![];

            for (seq_id, sequence) in sequences.iter().enumerate() {
                let ops_clone = sequence.operations.clone();
                let expected_clone = sequence.expected.clone();
                let mut client_clone = client.clone();
                let default_key_clone = default_key.to_vec();
                let handle = thread::spawn(move || {
                    let mut last_result = ExpectedResult::Success;
                    for op in &ops_clone {
                        last_result =
                            execute_single_operation(&mut client_clone, &default_key_clone, op);
                        info!(
                            "Concurrent operation result for sequence {}: {:?}",
                            seq_id, last_result
                        );
                    }

                    (last_result, expected_clone)
                });

                handles.push(handle);
            }

            let mut all_success = true;
            for (seq_id, handle) in handles.into_iter().enumerate() {
                match handle.join() {
                    Ok((actual_result, expected_result)) => {
                        if actual_result != expected_result {
                            info!(
                                "Concurrent sequence {} failed: expected {:?}, got {:?}",
                                seq_id, expected_result, actual_result
                            );
                            all_success = false;
                        }
                    }
                    Err(_) => {
                        info!("Concurrent thread panicked");
                        all_success = false;
                    }
                }
            }

            if all_success {
                ExpectedResult::Success
            } else {
                ExpectedResult::Fail
            }
        }
        _ => execute_single_operation(client, default_key, operation),
    }
}

fn execute_single_operation(
    client: &mut ClusterClient,
    default_key: &[u8],
    operation: &TestOperation,
) -> ExpectedResult {
    match operation {
        TestOperation::Get {
            ts,
            key,
            expected_value,
            check_value,
        } => {
            let target_key = key.as_deref().unwrap_or(default_key);
            // merge empty value and None
            let v_read = client
                .get_key_version_opt(
                    target_key,
                    ts.into_inner(),
                    Instant::now_coarse(),
                    &RequestOptions::no_resolve_lock(),
                )
                .map(|(v, _)| v.and_then(|v| if v.is_empty() { None } else { Some(v) }));
            let expected_value = expected_value
                .clone()
                .and_then(|v| if v.is_empty() { None } else { Some(v) });
            match (v_read, expected_value) {
                (Err(e), _) => {
                    info!("get failed: {:?}", e);
                    ExpectedResult::Fail
                }
                (Ok(v_read), expected) => {
                    if *check_value && v_read != expected {
                        info!("Get failed: expect {:?} but got {:?}", expected, v_read);
                        ExpectedResult::Fail
                    } else {
                        ExpectedResult::Success
                    }
                }
            }
        }
        TestOperation::PessimisticLock {
            start_ts,
            for_update_ts,
            key,
        } => {
            let target_key = key.as_deref().unwrap_or(default_key);
            match client.kv_pessimistic_lock(
                target_key.to_vec().into(),
                vec![target_key.to_vec().into()],
                *start_ts,
                20000,
                *for_update_ts,
                None,
            ) {
                Ok((_, key_errors)) => {
                    if key_errors.is_empty() {
                        ExpectedResult::Success
                    } else if key_errors[0].has_deadlock() {
                        ExpectedResult::Deadlock
                    } else {
                        ExpectedResult::Fail
                    }
                }
                Err(_) => ExpectedResult::Fail,
            }
        }
        TestOperation::AsyncPessimisticLock {
            start_ts,
            for_update_ts,
            timeout_ms,
            key,
        } => {
            let (tx, rx) = mpsc::channel();
            let target_key = key.as_deref().unwrap_or(default_key).to_vec();
            let start_ts_clone = *start_ts;
            let for_update_ts_clone = *for_update_ts;

            // Clone the client for the new thread
            let mut client_clone = client.clone();

            thread::spawn(move || {
                let result = client_clone.kv_pessimistic_lock(
                    target_key.clone().into(),
                    vec![target_key.clone().into()],
                    start_ts_clone,
                    20000,
                    for_update_ts_clone,
                    None,
                );

                let expected_result = match result {
                    Ok((_, key_errors)) => {
                        if key_errors.is_empty() {
                            ExpectedResult::Success
                        } else if key_errors[0].has_deadlock() {
                            ExpectedResult::Deadlock
                        } else {
                            info!("Async pessimistic lock failed: {:?}", key_errors);
                            ExpectedResult::Fail
                        }
                    }
                    Err(_) => ExpectedResult::Fail,
                };

                let _ = tx.send(expected_result);
            });

            match rx.recv_timeout(Duration::from_millis(*timeout_ms)) {
                Ok(result) => result,
                Err(_) => ExpectedResult::Timeout,
            }
        }
        TestOperation::Sleep { duration_ms } => {
            info!("Sleeping for {} ms", duration_ms);
            thread::sleep(Duration::from_millis(*duration_ms));
            ExpectedResult::Success
        }
        TestOperation::Prewrite {
            start_ts,
            expired,
            key,
            for_update_ts,
            value,
            assertion,
            op_type,
        } => {
            let target_key = key.as_deref().unwrap_or(default_key);
            let default_val = i_to_val(2);
            let val = value.as_ref().map(|v| v.as_slice()).unwrap_or(&default_val);

            // Create mutation with assertion support
            let mut mutation = match op_type {
                Op::Put => new_put_mutation(target_key.to_vec(), val.to_vec()),
                Op::Del => new_delete_mutation(target_key.to_vec()),
                Op::Lock => new_lock_mutation(target_key.to_vec()),
                _ => new_put_mutation(target_key.to_vec(), val.to_vec()),
            };

            // Set assertion if specified
            if *assertion != Assertion::None {
                mutation.set_assertion(*assertion);
            }

            let txn_muts = TxnMutations::from_normal(vec![mutation]);
            let ttl = if *expired { 1 } else { 20000 };
            let mut txn = client.begin_transaction(Some(*start_ts));
            match client.kv_prewrite_ext(
                target_key.to_vec().into(),
                None,
                txn_muts,
                &mut txn,
                PrewriteExt {
                    resolve_lock: false,
                    lock_ttl: Duration::from_millis(ttl),
                    for_update_ts: for_update_ts.unwrap_or(TimeStamp::zero()),
                },
            ) {
                Ok(_) => ExpectedResult::Success,
                Err(e) => {
                    info!("prewrite failed: {:?}", e);
                    ExpectedResult::Fail
                }
            }
        }
        TestOperation::Commit {
            start_ts,
            commit_ts,
            key,
        } => {
            let target_key = key.as_deref().unwrap_or(default_key);
            let mutations = vec![new_put_mutation(target_key.to_vec(), i_to_val(2))];
            let txn_muts = TxnMutations::from_normal(mutations);
            match client.kv_commit(txn_muts, *start_ts, *commit_ts, false) {
                Ok(_) => ExpectedResult::Success,
                Err(_) => ExpectedResult::Fail,
            }
        }
        TestOperation::Rollback { start_ts, key } => {
            let target_key = key.as_deref().unwrap_or(default_key);
            let mutations = vec![new_put_mutation(target_key.to_vec(), i_to_val(2))];
            let txn_muts = TxnMutations::from_normal(mutations);
            match client.kv_rollback(txn_muts, *start_ts) {
                Ok(_) => ExpectedResult::Success,
                Err(_) => ExpectedResult::Fail,
            }
        }
        TestOperation::CheckTxnStatus {
            lock_ts,
            caller_start_ts,
            current_ts,
            key,
        } => {
            let target_key = key.as_deref().unwrap_or(default_key);
            let resp = client.kv_check_txn_status(
                target_key,
                *lock_ts,
                *caller_start_ts,
                *current_ts,
                false,
                false,
                true,
                false,
            );
            info!("check_txn_status resp: {:?}", resp);
            match resp {
                Ok(_) => ExpectedResult::Success,
                Err(_) => ExpectedResult::Fail,
            }
        }
        TestOperation::Concurrent { .. } => unreachable!(),
    }
}

pub(crate) fn run_test_cases(cases: Vec<TestCase>) {
    test_util::init_log_for_test();
    let mut cluster = ServerCluster::new(alloc_node_id_vec(1), |_, conf| {
        // Disable `check_backup_ts`. Otherwise, hard code timestamps in test cases will
        // be rejected.
        conf.storage.check_backup_ts = false;
    });
    cluster.wait_region_replicated(&[], 1);
    let mut client = cluster.new_client();

    for (i, case) in cases.iter().enumerate() {
        info!("Running test case {}: {:?}", i + 1, case);

        // Check if the test case mixes custom keys with default keys
        let has_custom_keys = has_any_custom_keys(&case.operations);
        let has_default_keys = has_any_default_keys(&case.operations);

        if has_custom_keys && has_default_keys {
            panic!(
                "Test case {} mixes operations with custom keys and operations without keys. This is not allowed.",
                i + 1
            );
        }

        // Generate default key for this test case
        // Make it large to minimize the chance of collision with custom keys
        let default_key = i_to_key(i * 10000 + 1);

        // Execute all operations except the last one
        let setup_ops = &case.operations[..case.operations.len() - 1];
        for (j, op) in setup_ops.iter().enumerate() {
            let result = execute_operation(&mut client, &default_key, op);
            assert_eq!(
                result,
                ExpectedResult::Success,
                "Setup operation {} in test case {} failed: {:?}",
                j + 1,
                i + 1,
                op
            );
        }

        // Execute the last operation and assert the result
        let last_op = &case.operations[case.operations.len() - 1];
        let actual = execute_operation(&mut client, &default_key, last_op);

        assert_eq!(
            actual,
            case.expected,
            "Test case {} failed: expected {:?}, got {:?}",
            i + 1,
            case.expected,
            actual
        );

        info!("Test case {} passed", i + 1);
    }

    cluster.stop();
}

fn has_any_custom_keys(operations: &[TestOperation]) -> bool {
    for op in operations {
        if operation_has_custom_key(op) {
            return true;
        }
    }
    false
}

fn has_any_default_keys(operations: &[TestOperation]) -> bool {
    for op in operations {
        if operation_has_default_key(op) {
            return true;
        }
    }
    false
}

fn operation_has_custom_key(operation: &TestOperation) -> bool {
    match operation {
        TestOperation::Get { key, .. }
        | TestOperation::PessimisticLock { key, .. }
        | TestOperation::AsyncPessimisticLock { key, .. }
        | TestOperation::Prewrite { key, .. }
        | TestOperation::Commit { key, .. }
        | TestOperation::Rollback { key, .. }
        | TestOperation::CheckTxnStatus { key, .. } => key.is_some(),
        TestOperation::Concurrent { sequences } => sequences
            .iter()
            .any(|seq| seq.operations.iter().any(operation_has_custom_key)),
        TestOperation::Sleep { .. } => false,
    }
}

fn operation_has_default_key(operation: &TestOperation) -> bool {
    match operation {
        TestOperation::Get { key, .. }
        | TestOperation::PessimisticLock { key, .. }
        | TestOperation::AsyncPessimisticLock { key, .. }
        | TestOperation::Prewrite { key, .. }
        | TestOperation::Commit { key, .. }
        | TestOperation::Rollback { key, .. }
        | TestOperation::CheckTxnStatus { key, .. } => key.is_none(),
        TestOperation::Concurrent { sequences } => sequences
            .iter()
            .any(|seq| seq.operations.iter().any(operation_has_default_key)),
        TestOperation::Sleep { .. } => false,
    }
}
