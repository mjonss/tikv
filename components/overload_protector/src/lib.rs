// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

#[macro_use]
extern crate serde_derive;

use std::{
    collections::HashMap,
    sync::{
        Arc, RwLock,
        atomic::{AtomicUsize, Ordering::Relaxed},
    },
    time::Duration,
};

use online_config::{ConfigChange, ConfigManager, OnlineConfig};
use tikv_util::{
    config::{ReadableDuration, ReadableSize},
    info, mpsc,
    mpsc::{Receiver, Sender},
    time::Instant,
    warn,
};
use txn_types::TimeStamp;

#[derive(Clone, Copy, Debug, Default)]
pub struct CopTaskStats {
    pub task_id: u64,
    pub num_keys: u64,
    pub data_size: u64,
    pub num_reqs: u32,
    pub duration_ms: u32,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct OverloadConfig {
    pub enable: bool,
    // If a transaction cumulatively read more than max_keys or more than max_data_size, it will be
    // considered as an overload transaction. Upcoming coprocesor read requests of the same
    // transaction will be rejected by the coprocessor with an 'OverloadProtection' error.
    pub max_keys: u64,
    pub max_data_size: ReadableSize,

    // If a transaction read less than discard_keys during 'process_interval', it will not be taken
    // into account when calculating the cumulative read requests and data size.
    #[online_config(skip)]
    pub discard_keys: u64,

    // The interval to calculate and update the overload tasks.
    #[online_config(skip)]
    pub process_interval: ReadableDuration,

    // If a task's physical time has a gap to now and exceeds task_expire, and its stats will not
    // be taken into account for the next round of calculation.
    #[online_config(skip)]
    pub task_expire: ReadableDuration,
}

impl Default for OverloadConfig {
    fn default() -> Self {
        Self {
            enable: false,
            max_keys: 200_000_000,
            max_data_size: ReadableSize::gb(16),
            process_interval: ReadableDuration(Duration::from_secs(30)),
            discard_keys: 10_000,
            task_expire: ReadableDuration(Duration::from_secs(10 * 60)),
        }
    }
}

impl OverloadConfig {
    // validate is only called for online change, not for initial config, so it
    // doesn't affect the unit test.
    fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.max_keys < 1000000 {
            return Err("max-keys is too small, less than 1000000".into());
        }
        if self.max_data_size.0 < ReadableSize::gb(1).0 {
            return Err("max-data-size is too small, less than 1GB".into());
        }
        Ok(())
    }
}

pub struct OverloadProtectorWorker {
    config: Box<OverloadConfig>,
    protector: OverloadProtector,

    // The receiver to receive the CopTaskStats after cop task execution.
    receiver: Receiver<WorkerMsg>,

    // Task id (transaction start ts) -> CopTaskStats
    cop_tasks: HashMap<u64, CopTaskStats>,
    // Overload task id -> CopTaskStats
    overload_tasks: HashMap<u64, CopTaskStats>,
    // The last time to calculate the overload tasks.
    last_process_time: Instant,
}

enum WorkerMsg {
    CopTask(CopTaskStats),
    UpdateConfig(Box<OverloadConfig>),
    Stop,
}

impl OverloadProtectorWorker {
    pub fn new(config: OverloadConfig) -> Self {
        let (sender, receiver) = mpsc::unbounded();
        Self {
            cop_tasks: Default::default(),
            receiver,
            protector: OverloadProtector::new(sender),
            last_process_time: Instant::now(),
            overload_tasks: Default::default(),
            config: Box::new(config),
        }
    }

    pub fn run(&mut self) {
        info!("overload protector worker run");
        while let Ok(msg) = self.receiver.recv() {
            match msg {
                WorkerMsg::CopTask(stats) => {
                    if !self.config.enable {
                        continue;
                    }
                    self.add_cop_task_stats(stats);
                    if self.last_process_time.saturating_elapsed() > self.config.process_interval.0
                    {
                        self.process_cop_task_stats();
                        self.last_process_time = Instant::now();
                    }
                }
                WorkerMsg::UpdateConfig(config) => {
                    info!("overload protector worker update config {:?}", config);
                    self.config = config
                }
                WorkerMsg::Stop => {
                    info!("overload protector worker stopped");
                    break;
                }
            }
        }
    }

    pub fn get_protector(&self) -> OverloadProtector {
        self.protector.clone()
    }

    fn add_cop_task_stats(&mut self, stats: CopTaskStats) {
        let task_id = stats.task_id;
        let task_stats = self
            .cop_tasks
            .entry(task_id)
            .or_insert_with(|| CopTaskStats {
                task_id,
                num_reqs: 0,
                num_keys: 0,
                data_size: 0,
                duration_ms: 0,
            });
        task_stats.num_reqs += stats.num_reqs;
        task_stats.num_keys += stats.num_keys;
        task_stats.data_size += stats.data_size;
        task_stats.duration_ms += stats.duration_ms;
    }

    fn process_cop_task_stats(&mut self) {
        let mut not_expired_tasks = HashMap::with_capacity(self.cop_tasks.len());
        // Todo:
        // If a task is calculated as an overload task, it will stay in the
        // overload_tasks map forever. There is a potential risk that the map
        // will grow infinitely. We need to find a way to remove the task from the map
        // after a certain period.
        let mut new_overloads = self.overload_tasks.clone();
        let now = TimeStamp::physical_now();
        let task_expire_ms = self.config.task_expire.as_millis();
        for (task_id, stats) in self.cop_tasks.drain() {
            // The task is not taken into account if it reads less than discard_keys.
            if stats.num_keys < self.config.discard_keys {
                continue;
            }

            // If the task is already overloaded, it will not be calculated again.
            if new_overloads.contains_key(&task_id) {
                continue;
            }

            // If the task reads more than max_keys or max_data_size, it will be considered
            // as an overload task.
            if stats.num_keys > self.config.max_keys
                || stats.data_size > self.config.max_data_size.0
            {
                warn!("cop task is overloaded"; "task_id" => task_id, "stats" => ?stats);
                new_overloads.insert(task_id, stats);
                continue;
            }

            // If the task start time is too old, it will not be taken into account.
            let task_physical_time = TimeStamp::new(task_id).physical();
            if now.saturating_sub(task_physical_time) > task_expire_ms {
                // Todo:
                // In the future if we support stale read, a stale read request's physical time
                // might have a gap with the current time, we probably need to
                // find a better way like amw to handle this case.
                continue;
            }

            // Put these tasks that not expired and not overloaded into the tasks map again.
            not_expired_tasks.insert(task_id, stats);
        }

        self.cop_tasks = not_expired_tasks;
        if !new_overloads.is_empty() || !self.overload_tasks.is_empty() {
            self.overload_tasks = new_overloads.clone();
            self.protector.update(new_overloads);
        }
    }
}

#[derive(Clone)]
pub struct OverloadProtector {
    core: Arc<OverloadTasksCore>,
}

impl OverloadProtector {
    fn new(sender: Sender<WorkerMsg>) -> Self {
        Self {
            core: Arc::new(OverloadTasksCore {
                tasks: RwLock::new(HashMap::new()),
                task_count: AtomicUsize::new(0),
                sender,
            }),
        }
    }

    pub fn is_overloaded(&self, task_id: u64) -> Option<CopTaskStats> {
        if self.core.task_count.load(Relaxed) == 0 {
            return None;
        }
        let tasks = self.core.tasks.read().unwrap();
        tasks.get(&task_id).cloned()
    }

    pub fn report_cop_task_stats(&self, cop_task_stats: CopTaskStats) {
        if cop_task_stats.task_id == 0 || cop_task_stats.task_id == u64::MAX {
            // discard invalid task id.
            return;
        }
        let _ = self.core.sender.send(WorkerMsg::CopTask(cop_task_stats));
    }

    pub fn update_config(&self, new_config: OverloadConfig) {
        let _ = self
            .core
            .sender
            .send(WorkerMsg::UpdateConfig(Box::new(new_config)));
    }

    pub fn stop(&self) {
        let _ = self.core.sender.send(WorkerMsg::Stop);
    }

    fn update(&self, new_tasks: HashMap<u64, CopTaskStats>) {
        let mut tasks = self.core.tasks.write().unwrap();
        *tasks = new_tasks;
        self.core.task_count.store(tasks.len(), Relaxed);
    }
}

struct OverloadTasksCore {
    tasks: RwLock<HashMap<u64, CopTaskStats>>,
    task_count: AtomicUsize,
    sender: Sender<WorkerMsg>,
}

pub struct OverloadConfigManager {
    protector: OverloadProtector,
    config: OverloadConfig,
}

impl OverloadConfigManager {
    pub fn new(protector: OverloadProtector, config: OverloadConfig) -> Self {
        Self { protector, config }
    }
}

impl ConfigManager for OverloadConfigManager {
    fn dispatch(&mut self, change: ConfigChange) -> online_config::Result<()> {
        let mut new_config = self.config.clone();
        new_config.update(change)?;
        new_config.validate()?;
        self.protector.update_config(new_config);
        Ok(())
    }
}

#[test]
fn test_overload_protector() {
    let mut cfg = OverloadConfig::default();
    cfg.enable = true;
    cfg.max_keys = 100;
    cfg.discard_keys = 10;
    cfg.max_data_size = ReadableSize(1024);
    cfg.process_interval = ReadableDuration(Duration::from_millis(100));
    cfg.task_expire = ReadableDuration(Duration::from_millis(1000));
    let mut worker = OverloadProtectorWorker::new(cfg);
    let protector = worker.get_protector();
    std::thread::spawn(move || {
        worker.run();
    });
    let physical_now = TimeStamp::physical_now();
    let task_id_1 = TimeStamp::compose(physical_now, 1).into_inner();
    let task_id_2 = TimeStamp::compose(physical_now, 2).into_inner();
    let task_id_3 = TimeStamp::compose(physical_now, 3).into_inner();
    let task_id_4 = TimeStamp::compose(physical_now, 4).into_inner();
    let task_id_5 = TimeStamp::compose(physical_now, 5).into_inner();
    let report_stats = |task_id: u64, keys: u64, data_size: u64| {
        protector.report_cop_task_stats(CopTaskStats {
            task_id,
            num_keys: keys,
            data_size,
            num_reqs: 1,
            duration_ms: 1,
        })
    };
    for _ in 0..20 {
        std::thread::sleep(Duration::from_millis(15));
        // task id 1 is overloaded by key.
        report_stats(task_id_1, 11, 10);
        // task id 2 is overloaded by data size.
        report_stats(task_id_2, 2, 100);
        // task id 3 is discarded.
        report_stats(task_id_3, 1, 1);
        // task id 4 is is not discarded by not overloaded.
        report_stats(task_id_4, 3, 10);
        // task id 5 is is not discarded by not overloaded.
        report_stats(task_id_5, 3, 10);
    }
    assert!(protector.is_overloaded(task_id_1).is_some());
    assert!(protector.is_overloaded(task_id_2).is_some());
    assert!(protector.is_overloaded(task_id_3).is_none());
    assert!(protector.is_overloaded(task_id_4).is_none());
    assert!(protector.is_overloaded(task_id_5).is_none());

    // task id 4 keep report.
    for _ in 0..20 {
        std::thread::sleep(Duration::from_millis(15));
        report_stats(task_id_4, 3, 10);
    }
    assert!(protector.is_overloaded(task_id_4).is_some());

    // task id 5 expires
    std::thread::sleep(Duration::from_millis(1000));
    for _ in 0..20 {
        std::thread::sleep(Duration::from_millis(15));
        report_stats(task_id_5, 3, 10);
    }
    assert!(protector.is_overloaded(task_id_5).is_none());

    protector.stop();
}
