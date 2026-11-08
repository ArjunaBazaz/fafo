use super::attempt::execute;
use super::state::{ReadSet, VersionedState};
use super::tx_context::Runtime;
use super::*;
use exepipe::test_helper::MockADS;
use exepipe_common::acc_data::encode_account_info;
use exepipe_common::slot_entry_value::SlotEntryValue;
use exepipe_common::utils::join_address_index;
use qmdb::def::{OP_CREATE, OP_DELETE, OP_WRITE};
use qmdb::test_helper::EntryBuilder;
use qmdb::utils::{byte0_to_shard_id, changeset::ChangeSet, hasher};
use revm::context::{BlockEnv, TxEnv};
use revm::primitives::{Bytes, TxKind, U256};
use revm::state::AccountInfo;

fn address(id: u16) -> Address {
    Address::left_padding_from(&id.to_be_bytes())
}

fn change(op: u8, key: &[u8], value: &[u8], old: &[u8]) -> ChangeSet {
    let hash = hasher::hash(key);
    let mut changes = ChangeSet::new();
    changes.add_op_with_old_value(
        op,
        byte0_to_shard_id(hash[0]) as u8,
        &hash,
        key,
        value,
        old,
        None,
    );
    changes.sort();
    changes
}

fn runtime(ads: MockADS, dir: &str) -> Runtime<MockADS> {
    Runtime {
        ads,
        code_db: Arc::new(CodeDB::new(dir, -1, 4096, 1024 * 1024)),
        state: Arc::new(VersionedState::default()),
        height: 1,
        env: BlockEnv::default(),
        block_hashes: HashMap::new(),
    }
}

fn transfer(from: u16, to: u16, value: u64) -> Box<Tx> {
    Box::new(Tx {
        tx_hash: B256::ZERO,
        // Deliberately no predictions: execution must discover its accesses.
        crw_sets: None,
        env: TxEnv {
            caller: address(from),
            kind: TxKind::Call(address(to)),
            value: U256::from(value),
            gas_limit: 1_000_000,
            ..TxEnv::default()
        },
    })
}

fn account(r: &Runtime<MockADS>, id: u16) -> Option<AccountInfo> {
    let addr = address(id);
    r.state
        .account(
            &r.ads,
            &r.code_db,
            r.height,
            r.state.version(),
            &addr,
            &hasher::hash(addr),
        )
        .1
}

#[test]
fn versions_cover_missing_values_deletions_and_aba() {
    let state = VersionedState::default();
    let ads = MockADS::new();
    let key = b"a storage key";
    let hash = hasher::hash(key);
    let initial = state.version();
    assert!(state.read(&ads, 1, initial, &hash, key).1.is_none());
    let reads = ReadSet::from([(hash, 0)]);
    state.publish(&change(OP_CREATE, key, b"one", b""), &HashMap::new());
    assert!(!state.validate(&reads));
    assert!(state.read(&ads, 1, initial, &hash, key).1.is_none());
    let present = state.version();
    state.publish(&change(OP_DELETE, key, b"", b"one"), &HashMap::new());
    assert_eq!(
        state.read(&ads, 1, present, &hash, key).1.unwrap().as_ref(),
        b"one"
    );
    assert!(state.read(&ads, 1, state.version(), &hash, key).1.is_none());
    state.publish(&change(OP_CREATE, key, b"one", b""), &HashMap::new());
    assert!(!state.validate(&ReadSet::from([(hash, present)])));
    state.compact();
    assert_eq!(
        state
            .read(&ads, 2, state.version(), &hash, key)
            .1
            .unwrap()
            .as_ref(),
        b"one"
    );
}

#[test]
fn overwritten_base_values_remain_visible_to_older_snapshots() {
    let mut ads = MockADS::new();
    let key = b"existing key";
    ads.add_entry(EntryBuilder::kv(key.as_slice(), b"old".as_slice()).build_and_dump(&[]));
    let state = VersionedState::default();
    let hash = hasher::hash(key);
    state.publish(&change(OP_WRITE, key, b"new", b"old"), &HashMap::new());
    assert_eq!(
        state.read(&ads, 1, 0, &hash, key).1.unwrap().as_ref(),
        b"old"
    );
    assert_eq!(
        state.read(&ads, 1, 1, &hash, key).1.unwrap().as_ref(),
        b"new"
    );
    state.publish(&change(OP_DELETE, key, b"", b"new"), &HashMap::new());
    // The old value still exists in ADS; the tombstone must mask it.
    assert!(state.read(&ads, 1, 2, &hash, key).1.is_none());
}

#[test]
fn speculative_failures_retry_and_match_serial_execution() {
    for missing_recipient in [false, true] {
        let mut outcomes = Vec::new();
        for (workers, window) in [(1, 1), (1, 4), (4, 4)] {
            let temp = tempfile::tempdir().unwrap();
            let mut ads = MockADS::new();
            ads.add_account(&address(100), &AccountInfo::from_balance(U256::from(100)));
            if !missing_recipient {
                ads.add_account(&address(101), &AccountInfo::default());
            }
            let r = runtime(ads, temp.path().to_str().unwrap());
            let txs = vec![
                transfer(100, 101, 10),
                transfer(101, 102, 3),
                transfer(100, 102, 2),
            ];
            let pool = ThreadPoolBuilder::new()
                .num_threads(workers)
                .build()
                .unwrap();
            let mut indices = Vec::new();
            let stale_failure = execute(&r, &txs[1], r.state.version());
            assert!(stale_failure.output.0.is_err());
            let stats = scheduler::run(&r, &txs, &pool, window, |index, attempt| {
                assert!(attempt.output.0.unwrap().result.is_success());
                indices.push(index);
                Ok(())
            })
            .unwrap();
            assert_eq!(indices, vec![0, 1, 2]);
            assert_eq!(stats.finalized, 3);
            assert_eq!(stats.errors, 0);
            // Actual conflicts depend on when workers start their read phase.
            assert_eq!(stats.attempts, stats.finalized + stats.validation_failures);
            assert!(stats.validation_failures <= 2);
            assert!(!r.state.validate(&stale_failure.reads));
            outcomes.push(
                (100..=102)
                    .map(|id| {
                        let info = account(&r, id).unwrap();
                        (info.balance, info.nonce)
                    })
                    .collect::<Vec<_>>(),
            );
        }
        assert_eq!(outcomes[0], outcomes[1]);
        assert_eq!(outcomes[0], outcomes[2]);
        assert_eq!(
            outcomes[0].iter().map(|v| v.0).collect::<Vec<_>>(),
            vec![U256::from(88), U256::from(7), U256::from(5)]
        );
    }
}

#[test]
fn independent_transfers_do_not_retry() {
    let temp = tempfile::tempdir().unwrap();
    let mut ads = MockADS::new();
    for id in [100, 102] {
        ads.add_account(&address(id), &AccountInfo::from_balance(U256::from(100)));
    }
    let r = runtime(ads, temp.path().to_str().unwrap());
    let pool = ThreadPoolBuilder::new().num_threads(2).build().unwrap();
    let stats = scheduler::run(
        &r,
        &[transfer(100, 101, 1), transfer(102, 103, 1)],
        &pool,
        2,
        |_, a| {
            assert!(a.output.0.is_ok());
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(stats.attempts, 2);
    assert_eq!(stats.validation_failures, 0);
}

#[test]
fn contract_storage_is_private_until_commit_and_revalidated() {
    let temp = tempfile::tempdir().unwrap();
    let mut ads = MockADS::new();
    for id in [100, 101] {
        ads.add_account(&address(id), &AccountInfo::from_balance(U256::from(100)));
    }
    // slot[0] += 1; STOP. No predicted access set is supplied.
    let code = Bytecode::new_raw(Bytes::from(vec![
        0x60, 0, 0x54, 0x60, 1, 0x01, 0x60, 0, 0x55, 0,
    ]));
    let contract = address(200);
    let info = AccountInfo {
        code_hash: code.hash_slow(),
        ..AccountInfo::default()
    };
    ads.add_entry(
        EntryBuilder::kv(contract.as_slice(), encode_account_info(&info).as_slice())
            .build_and_dump(&[]),
    );
    let key = join_address_index(&contract, &U256::ZERO);
    let hash = hasher::hash(key);
    let mut value = U256::from(1).to_be_bytes_vec();
    SlotEntryValue::encode(&mut value, 0);
    ads.add_entry(EntryBuilder::kv(key.as_slice(), value.as_slice()).build_and_dump(&[]));
    let r = runtime(ads, temp.path().to_str().unwrap());
    r.code_db.append(&contract, code, 0).unwrap();
    let mut first = transfer(100, 200, 0);
    first.crw_sets = Some(exepipe_common::access_set::CrwSets::default());
    first.env.data = Bytes::from(vec![1]);
    let mut second = transfer(101, 200, 0);
    second.crw_sets = Some(exepipe_common::access_set::CrwSets::default());
    second.env.data = Bytes::from(vec![1]);
    let private = execute(&r, &first, 0);
    assert!(private.output.0.unwrap().result.is_success());
    let (_, unchanged) = r.state.read(&r.ads, 1, 0, &hash, &key);
    assert_eq!(unchanged.unwrap().as_ref(), value.as_slice());
    let pool = ThreadPoolBuilder::new().num_threads(2).build().unwrap();
    let stats = scheduler::run(&r, &[first, second], &pool, 2, |_, a| {
        assert!(a.output.0.unwrap().result.is_success());
        Ok(())
    })
    .unwrap();
    assert!(stats.validation_failures <= 1);
    assert!(!r.state.validate(&private.reads));
    let (_, value) = r.state.read(&r.ads, 1, r.state.version(), &hash, &key);
    let value = value.unwrap();
    assert_eq!(
        U256::from_be_slice(SlotEntryValue::from_bz(&value).get_value()),
        U256::from(3)
    );
}

#[test]
fn empty_blocks_and_final_errors_are_supported() {
    let temp = tempfile::tempdir().unwrap();
    let r = runtime(MockADS::new(), temp.path().to_str().unwrap());
    let pool = ThreadPoolBuilder::new().num_threads(1).build().unwrap();
    let stats = scheduler::run(&r, &[], &pool, 1, |_, _| unreachable!()).unwrap();
    assert_eq!(stats, OccStats::default());
    let stats = scheduler::run(&r, &[transfer(100, 101, 1)], &pool, 1, |_, a| {
        assert!(matches!(a.output.0, Err(ExecutionError::CallerNotFound(_))));
        Ok(())
    })
    .unwrap();
    assert_eq!(stats.errors, 1);
    assert_eq!(stats.applied, 0);
}

#[test]
fn reverted_contract_changes_are_not_published() {
    let temp = tempfile::tempdir().unwrap();
    let mut ads = MockADS::new();
    ads.add_account(&address(100), &AccountInfo::from_balance(U256::from(100)));
    // SSTORE(0, 1), then REVERT(0, 0).
    let code = Bytecode::new_raw(Bytes::from(vec![
        0x60, 1, 0x60, 0, 0x55, 0x60, 0, 0x60, 0, 0xfd,
    ]));
    let contract = address(200);
    let info = AccountInfo {
        code_hash: code.hash_slow(),
        ..AccountInfo::default()
    };
    ads.add_entry(
        EntryBuilder::kv(contract.as_slice(), encode_account_info(&info).as_slice())
            .build_and_dump(&[]),
    );
    let r = runtime(ads, temp.path().to_str().unwrap());
    r.code_db.append(&contract, code, 0).unwrap();
    let pool = ThreadPoolBuilder::new().num_threads(1).build().unwrap();
    scheduler::run(&r, &[transfer(100, 200, 0)], &pool, 1, |_, a| {
        assert!(matches!(
            a.output.0.unwrap().result,
            revm::context::result::ExecutionResult::Revert { .. }
        ));
        Ok(())
    })
    .unwrap();
    let key = join_address_index(&contract, &U256::ZERO);
    assert!(r
        .state
        .read(&r.ads, 1, r.state.version(), &hasher::hash(key), &key)
        .1
        .is_none());
    assert_eq!(account(&r, 100).unwrap().nonce, 1);
}

fn persisted_run(workers: usize, window: usize) -> Vec<[u8; 32]> {
    use exepipe_common::acc_data::decode_account_info_from_bz;
    use qmdb::entryfile::readbuf::ReadBuf;
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path().to_str().unwrap();
    let mut config = qmdb::config::Config::from_dir(dir);
    config.bypass_prefetcher = true;
    config.wrbuf_size = 64 * 1024;
    config.file_segment_size = 8 * 1024 * 1024;
    qmdb::AdsCore::init_dir(&config);
    let mut ads = AdsWrap::<ExeTask>::new(&config);
    let manager = Arc::new(TasksManager::with_size(1));
    manager.set_valid_count(1);
    ads.start_block(1, manager.clone());
    let shared = ads.get_shared();
    shared.insert_extra_data(1, String::new());
    let value = encode_account_info(&AccountInfo::from_balance(U256::from(100)));
    OccExecutor::send_task(
        &shared,
        &manager,
        1,
        0,
        true,
        vec![change(
            OP_CREATE,
            address(100).as_slice(),
            value.as_slice(),
            b"",
        )],
    );
    ads.flush();
    let mut executor = OccExecutor::new(
        ads,
        dir,
        OccConfig {
            workers,
            window,
            ..OccConfig::default()
        },
    )
    .unwrap();
    let block = |height| {
        Block::new(
            height,
            B256::ZERO,
            B256::ZERO,
            U256::ZERO,
            0,
            B256::ZERO,
            B256::ZERO,
            BlockEnv {
                number: height as u64,
                ..BlockEnv::default()
            },
        )
    };
    let result = executor
        .run_block(
            vec![vec![transfer(100, 101, 10), transfer(101, 102, 3)]],
            block(2),
        )
        .unwrap();
    assert_eq!(result.stats.finalized, 2);
    assert_eq!(result.stats.persistence_tasks, 1);
    assert!(result
        .results
        .iter()
        .all(|r| r.as_ref().unwrap().result.is_success()));
    // No intervening flush: the next block must see the committed overlay.
    executor
        .run_block(vec![vec![transfer(102, 100, 2)]], block(3))
        .unwrap();
    let result = executor.run_block(vec![], block(4)).unwrap();
    assert_eq!(result.stats, OccStats::default());
    executor.flush().unwrap();
    executor
        .run_block(vec![vec![transfer(101, 102, 1)]], block(5))
        .unwrap();
    executor.flush().unwrap();
    for (id, balance) in [(100, 92), (101, 6), (102, 2)] {
        let mut buf = ReadBuf::new();
        let addr = address(id);
        shared.read_entry(5, &hasher::hash(addr), addr.as_slice(), &mut buf);
        assert!(!buf.is_empty());
        let info = decode_account_info_from_bz(buf.as_entry_bz().value());
        assert_eq!(info.balance, U256::from(balance));
    }
    // QMDB's combined historical-root helper is unimplemented. Compare the
    // actual persisted roots of every shard after draining the database.
    let meta = executor.ads.get_metadb();
    let meta = meta.read();
    assert_eq!(meta.get_curr_height(), 5);
    let roots: Vec<_> = (0..qmdb::def::SHARD_COUNT)
        .map(|shard| meta.get_root_hash(shard))
        .collect();
    assert!(roots.iter().all(|root| *root != [0; 32]));
    roots
}

#[test]
#[serial_test::serial]
fn qmdb_commits_match_serial_across_blocks_and_overlay_flush() {
    assert_eq!(persisted_run(1, 1), persisted_run(2, 4));
}

#[test]
fn created_bytecode_remains_private_until_validation() {
    let temp = tempfile::tempdir().unwrap();
    let mut ads = MockADS::new();
    ads.add_account(&address(100), &AccountInfo::from_balance(U256::from(100)));
    let r = runtime(ads, temp.path().to_str().unwrap());
    // Init code returns four bytes: PUSH1 0; POP; STOP.
    let init = vec![
        0x60, 4, 0x60, 12, 0x60, 0, 0x39, 0x60, 4, 0x60, 0, 0xf3, 0x60, 0, 0x50, 0,
    ];
    let mut tx = transfer(100, 101, 0);
    tx.env.kind = TxKind::Create;
    tx.env.data = Bytes::from(init);
    let created = address(100).create(0);
    let attempt = execute(&r, &tx, 0);
    assert!(attempt.output.0.unwrap().result.is_success());
    let (_, _, _, pending_codes, _, _) = attempt.output.1.unwrap();
    assert!(pending_codes.contains_key(&created));
    assert!(r
        .state
        .account(&r.ads, &r.code_db, 1, 0, &created, &hasher::hash(created))
        .1
        .is_none());
    let pool = ThreadPoolBuilder::new().num_threads(1).build().unwrap();
    scheduler::run(&r, &[tx], &pool, 1, |_, a| {
        assert!(a.output.0.is_ok());
        Ok(())
    })
    .unwrap();
    // The committed code is available by its immutable hash, even before the
    // persistence callback appends it to CodeDB.
    let (_, info) = r.state.account(
        &r.ads,
        &r.code_db,
        1,
        r.state.version(),
        &created,
        &hasher::hash(created),
    );
    assert_eq!(
        info.unwrap().code.unwrap().original_byte_slice(),
        &[0x60, 0, 0x50, 0]
    );
}

#[test]
fn native_change_sets_match_the_unchanged_fafo_executor() {
    use exepipe_common::access_set::CrwSets;
    use exepipe_common::executor::exector_database::ExectorDatabase;
    use exepipe_common::executor::execute_tx::execute_tx;

    struct NativeDatabase<'a>(&'a Runtime<MockADS>);
    impl ExectorDatabase for NativeDatabase<'_> {
        fn basic(&mut self, address: &Address) -> Option<AccountInfo> {
            self.0
                .state
                .account(
                    &self.0.ads,
                    &self.0.code_db,
                    self.0.height,
                    self.0.state.version(),
                    address,
                    &hasher::hash(address),
                )
                .1
        }
        fn storage_value(&mut self, _: Address, _: &U256) -> Option<&[u8]> {
            unreachable!("native transfer does not access contract storage")
        }
        fn block_hash(&mut self, number: u64) -> Option<B256> {
            self.0.block_hashes.get(&number).copied()
        }
    }

    fn operations(changes: &ChangeSet) -> Vec<(u8, Vec<u8>, Vec<u8>, Vec<u8>)> {
        let mut operations = Vec::new();
        changes.apply_op_in_range(|op, _, key, value, old, _| {
            operations.push((op, key.to_vec(), value.to_vec(), old.to_vec()));
        });
        operations
    }

    let temp = tempfile::tempdir().unwrap();
    let mut ads = MockADS::new();
    ads.add_account(&address(100), &AccountInfo::from_balance(U256::from(100)));
    let r = runtime(ads, temp.path().to_str().unwrap());
    let tx = transfer(100, 101, 3);
    let (original_result, original_effects) =
        execute_tx(NativeDatabase(&r), &r.env, &tx.env, &CrwSets::default());
    let occ = execute(&r, &tx, r.state.version());
    let original_result = original_result.unwrap();
    let occ_result = occ.output.0.unwrap();
    assert_eq!(
        original_result.result.gas_used(),
        occ_result.result.gas_used()
    );
    assert_eq!(
        original_result.result.is_success(),
        occ_result.result.is_success()
    );
    assert_eq!(
        operations(&original_effects.unwrap().0),
        operations(&occ.output.1.unwrap().0)
    );
}
