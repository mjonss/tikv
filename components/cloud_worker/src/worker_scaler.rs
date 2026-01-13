// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap, default::Default, fs, ops::Deref, str::FromStr, sync::Arc, time::Duration,
};

use dashmap::{DashMap, mapref::entry::Entry};
use futures::StreamExt;
use http::Uri;
use k8s_openapi::{
    api::{
        apps::v1::StatefulSet,
        core::v1::{
            EnvVar, PersistentVolumeClaim, Pod, Service, ServicePort, ServiceSpec, VolumeMount,
        },
    },
    apimachinery::pkg::{apis::meta::v1::ObjectMeta, util::intstr::IntOrString},
    serde_json,
};
use kube::{
    Client as KubeClient,
    api::{Api, AttachParams, AttachedProcess, ListParams, PostParams, ResourceExt},
};
use load_data::{
    checkpoint::{CANCELLED_TASK_EXPIRE_SEC, FINISHED_TASK_EXPIRE_SEC, IDLE_TASK_EXPIRE_SEC},
    task::LoadTaskStates,
};
use security::SecurityManager;
use serde_json::json;
use tikv_util::{box_err, config::ReadableSize, error, info, time::Instant, warn};
use tokio::sync::RwLock;

use crate::{
    error::{Error, Result},
    metrics::WORKER_SCALER_QUERY_FAILURES_COUNTER_VEC,
};

const DEFAULT_LOAD_DATA_WORKER_MIN_STORAGE_GB: usize = 20;
const DEFAULT_LOAD_DATA_WORKER_NAME: &str = "load-data-worker";
const DEFAULT_LOAD_DATA_WORKER_PORT: u16 = 19500;
const DEFAULT_MAX_SIZE: ReadableSize = ReadableSize::gb(1024);
const DEFAULT_SPAWN_DATA_SIZE: ReadableSize = ReadableSize::gb(2);
const DEFAULT_SPAWN_RUNNING_TASKS: usize = 8;
const DEFAULT_WORKER_MAX_CORES: f64 = 4.0;
const DEFAULT_NAMESPACE: &str = "tidb-serverless";
const DEFAULT_TEMPLATE_STS_NAME: &str = "tikv-api";
const DEFAULT_WORKER_COUNT_LIMIT: usize = 1024;
const DEFAULT_WAIT_POD_READY_TIMEOUT: u64 = 60 * 5;
const DEFAULT_POD_WARM_UP_SECCONDS: i64 = 60 * 10;

const APP_LABEL: &str = "app";
const K8S_LABEL_NAME: &str = "app.kubernetes.io/name";
const K8S_LABEL_SERVICE: &str = "app.kubernetes.io/service";
const K8S_LABEL_COMPONENT: &str = "app.kubernetes.io/component";
const K8S_NAMESPACE_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/namespace";
const K8S_NODE_SELECTOR_NODE_NAME_KEY: &str = "serverless.tidbcloud.com/node";

const LARGE_DATA_LOAD_DATA_WORKER_NODE_GROUP_NAME: &str = "dedicated";
const LARGE_DATA_LOAD_DATA_WORKER_NODE_GROUP_CPU_NUM: f64 = 48.0;

const NETWORK_PROTOCOL: &str = "TCP";

// The max retry time: 30 * 50 * 10 * 1 = 15000s
const CLEAN_UP_WORKER_TICK_INTERVAL: u64 = 30;
const WORKER_SCALER_QUERY_FAILURES_LIMIT: usize = 50;
const WORKER_SCALER_QUERY_RETRY_COUNT: usize = 10;
const WORKER_SCALER_QUERY_RETRY_INTERVAL: u64 = 1;

pub const LOAD_DATA_WORKER_ENV: &str = "TIKV_LOAD_DATA_WORKER";
pub const LOAD_DATA_WORKER_WORKER_NUM_ENV: &str = "TIKV_LOAD_DATA_WORKER_WORKER_NUM_ENV";

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Default)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct StsConfig {
    pub min_data_size_gb: usize,
    // The following fields are used to generate sts.
    pub request_cpu: f64,
    pub request_memory: f64,
    pub limit_cpu: f64,
    pub limit_memory: f64,
    pub storage_size_gb: usize,
    pub node_group: String,
    // `node_selector_key` and `node_selector_value` are used to select node accurately.
    pub node_selector_key: String,
    pub node_selector_value: String,
    // The following fields are used to configure the load-data-worker.
    pub worker_num: usize,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct WorkerScalerConfig {
    pub run: bool,
    pub namespace: String,
    pub template_sts_name: String,
    pub name: String,
    pub worker_port: u16,
    pub max_size: ReadableSize,
    pub spawn_data_size: ReadableSize,
    pub spawn_running_tasks: usize,
    pub worker_max_cores: f64,
    pub pod_warm_up_sec: i64,
    pub cancelled_task_expire_sec: i64,
    pub finished_task_expire_sec: i64,
    pub idle_task_expire_sec: i64,
    pub worker_min_storage_gb: usize,
    pub worker_count_limit: usize,
    pub sts_configs: Vec<StsConfig>,
}

impl Default for WorkerScalerConfig {
    fn default() -> Self {
        Self {
            run: false,
            namespace: DEFAULT_NAMESPACE.to_string(),
            template_sts_name: DEFAULT_TEMPLATE_STS_NAME.to_string(),
            name: DEFAULT_LOAD_DATA_WORKER_NAME.to_string(),
            worker_port: DEFAULT_LOAD_DATA_WORKER_PORT,
            max_size: DEFAULT_MAX_SIZE,
            spawn_data_size: DEFAULT_SPAWN_DATA_SIZE,
            spawn_running_tasks: DEFAULT_SPAWN_RUNNING_TASKS,
            worker_max_cores: DEFAULT_WORKER_MAX_CORES,
            pod_warm_up_sec: DEFAULT_POD_WARM_UP_SECCONDS,
            cancelled_task_expire_sec: CANCELLED_TASK_EXPIRE_SEC,
            finished_task_expire_sec: FINISHED_TASK_EXPIRE_SEC,
            idle_task_expire_sec: IDLE_TASK_EXPIRE_SEC,
            worker_min_storage_gb: DEFAULT_LOAD_DATA_WORKER_MIN_STORAGE_GB,
            sts_configs: vec![],
            worker_count_limit: DEFAULT_WORKER_COUNT_LIMIT,
        }
    }
}

impl WorkerScalerConfig {
    fn label_selector(&self) -> String {
        format!("{}={}", K8S_LABEL_NAME, self.name)
    }

    fn merge_sts_configs(&mut self, mut sts_configs: HashMap<usize, StsConfig>) {
        for sts_config in self.sts_configs.drain(..) {
            // make sure stst_config is valid
            if sts_config.request_cpu == 0.0
                || sts_config.limit_cpu == 0.0
                || sts_config.worker_num == 0
            {
                continue;
            }
            sts_configs.insert(sts_config.min_data_size_gb, sts_config);
        }
        let mut sts_configs_list: Vec<_> = sts_configs.into_iter().map(|c| c.1).collect();
        sts_configs_list.sort_by(|a, b| a.min_data_size_gb.cmp(&b.min_data_size_gb));
        self.sts_configs = sts_configs_list;
        info!(
            "worker scaler config final sts configs: {:?}",
            self.sts_configs
        );
    }
}

#[derive(Clone)]
pub(crate) struct WorkerScaler {
    core: Arc<WorkerScalerCore>,
}

impl Deref for WorkerScaler {
    type Target = WorkerScalerCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

pub(crate) struct WorkerScalerCore {
    sts_template: StatefulSet,
    sts_api: Api<StatefulSet>,
    svc_api: Api<Service>,
    pvc_api: Api<PersistentVolumeClaim>,
    pod_api: Api<Pod>,
    config: WorkerScalerConfig,
    cluster_id: u64,
    in_k8s: bool,
    http_client: security::HttpClient,
    pods_map: DashMap<String, Arc<RwLock<WorkerPod>>>,
    security_mgr: Arc<SecurityManager>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct WorkerPod {
    name: String,
    started_at: i64,
    updated_at: i64,
    svc_name: String,
    cleanup: bool,
    finished: bool,
    stopped_at: i64,
    flushed_files: usize,
    created_files: usize,
    ingested_regions: usize,
    query_failures: usize,
}

impl WorkerPod {
    fn new(name: String) -> Self {
        Self {
            name,
            ..Default::default()
        }
    }

    fn init(&mut self, task_id: &str, pod: &Pod) {
        self.name = pod.name_any();
        if let Some(status) = &pod.status {
            if let Some(conditions) = &status.conditions {
                if let Some(ready_condition) = conditions.iter().find(|c| c.type_ == "Ready") {
                    if ready_condition.status == "True" {
                        if let Some(t) = &ready_condition.last_transition_time {
                            self.started_at = t.0.timestamp();
                            self.updated_at = t.0.timestamp();
                            self.svc_name = new_worker_svc_name(task_id);
                        }
                    }
                }
            }
        }
    }

    fn update_task_states(
        &mut self,
        tasks: Option<Vec<LoadTaskStates>>,
        now_timestamp: i64,
        pod_warm_up_sec: i64,
        cancelled_task_expire_sec: i64,
        finished_task_expire_sec: i64,
        idle_task_expire_sec: i64,
    ) {
        if self.started_at == 0 || (now_timestamp - self.started_at < pod_warm_up_sec) {
            return;
        }

        if let Some(tasks) = tasks {
            if tasks.is_empty() {
                // the task is cleaned up by client and GCed by the load data worker.
                info!("task {} is GCed by the load data worker", self.name);
                self.cleanup = true;
            } else {
                let task = &tasks[0];
                if task.canceled && self.stopped_at == 0 {
                    // the task is cleaned up by client.
                    info!("task {} is cleaned up by the client", self.name);
                    self.stopped_at = now_timestamp;
                }

                self.finished = task.finished;
                if self.flushed_files != task.flushed_files
                    || self.created_files != task.created_files
                    || self.ingested_regions != task.ingested_regions
                {
                    self.flushed_files = task.flushed_files;
                    self.created_files = task.created_files;
                    self.ingested_regions = task.ingested_regions;
                    self.updated_at = now_timestamp;
                    info!("task {} progress updated", self.name;
                        "flushed_files" => self.flushed_files,
                        "created_files" => self.created_files,
                        "ingested_regions" => self.ingested_regions,
                        "updated_at" => self.updated_at,
                    );
                }
            }
            // reset query_failures
            self.query_failures = 0;
            WORKER_SCALER_QUERY_FAILURES_COUNTER_VEC
                .with_label_values(&[&self.name])
                .reset();
        } else {
            warn!("failed get pod {} task states", self.name);
            WORKER_SCALER_QUERY_FAILURES_COUNTER_VEC
                .with_label_values(&[&self.name])
                .inc();
            self.query_failures += 1;
            if self.query_failures > WORKER_SCALER_QUERY_FAILURES_LIMIT {
                self.cleanup = true;
            }
        }

        if self.stopped_at > 0 {
            let duration = now_timestamp - self.stopped_at;
            if (self.finished && duration > finished_task_expire_sec)
                || duration > cancelled_task_expire_sec
            {
                let log_msg = if self.finished {
                    "finished"
                } else {
                    "canceled"
                };
                info!("clean up {} task {}", log_msg, self.name);
                self.cleanup = true
            }
        } else {
            let updated_duration = now_timestamp - self.updated_at;
            if updated_duration > idle_task_expire_sec {
                warn!(
                    "pod {} is cleanuped up for no progress for {} seconds",
                    self.name, updated_duration
                );
                self.cleanup = true;
            }
        }
    }
}

impl WorkerScaler {
    pub(crate) async fn new(
        cfg: &WorkerScalerConfig,
        cluster_id: u64,
        security_mgr: Arc<SecurityManager>,
    ) -> kube::Result<Self> {
        let kube_client = KubeClient::try_default().await?;
        let pvc_api: Api<PersistentVolumeClaim> =
            Api::namespaced(kube_client.clone(), &cfg.namespace);
        let sts_api: Api<StatefulSet> = Api::namespaced(kube_client.clone(), &cfg.namespace);
        let svc_api: Api<Service> = Api::namespaced(kube_client.clone(), &cfg.namespace);
        let pod_api: Api<Pod> = Api::namespaced(kube_client.clone(), &cfg.namespace);
        let sts_template = sts_api.get(&cfg.template_sts_name).await?;
        let in_k8s = fs::read(K8S_NAMESPACE_PATH).is_ok();
        let mut config = cfg.clone();
        config.merge_sts_configs(gen_default_sts_configs());
        let worker_scaler = Self {
            core: Arc::new(WorkerScalerCore {
                sts_template,
                svc_api,
                sts_api,
                pvc_api,
                pod_api,
                pods_map: dashmap::DashMap::default(),
                config,
                cluster_id,
                in_k8s,
                http_client: security_mgr.http_client(hyper::Client::builder()).unwrap(),
                security_mgr,
            }),
        };
        worker_scaler.init().await?;
        Ok(worker_scaler)
    }

    pub(crate) async fn run(&self) {
        info!("worker scaler started");
        let interval = Duration::from_secs(CLEAN_UP_WORKER_TICK_INTERVAL);
        let mut timer = tokio::time::interval(interval);
        let pvc_ticks = 10;
        let mut tick_cnt = 0;
        loop {
            timer.tick().await;
            self.clean_up_finished_workers().await;
            tick_cnt += 1;
            if tick_cnt % pvc_ticks == 0 {
                self.clean_up_orphan_pvcs().await;
            }
        }
    }

    async fn init(&self) -> kube::Result<()> {
        let list_params = ListParams::default()
            .labels(&self.config.label_selector())
            .timeout(15);
        // TODO: the number of pods may not be consistent with the number of sts.
        let mut pods = self.pod_api.list(&list_params).await?;
        for pod in pods.items.drain(..) {
            let name = pod.name_any();
            let task_id = parse_task_id_by_pod_name(name.as_str());
            assert!(!task_id.is_empty());
            info!("load pod {}", name);
            let worker_pod = WorkerPod::new(name);
            self.pods_map
                .insert(task_id, Arc::new(RwLock::new(worker_pod)));
        }
        Ok(())
    }

    async fn clean_up_finished_workers(&self) {
        let task_ids: Vec<String> = self.pods_map.iter().map(|x| x.key().clone()).collect();
        for task_id in task_ids {
            self.maybe_clean_up(&task_id).await;
        }
    }

    async fn clean_up_orphan_pvcs(&self) {
        let list_params = ListParams::default()
            .labels(&self.config.label_selector())
            .timeout(15);
        let res = self.pvc_api.list(&list_params).await;
        if let Err(err) = res {
            error!("list pvc err {:?}", err);
            return;
        }
        let mut pvcs = res.unwrap();
        let mut orphan_pvcs = vec![];
        for pvc in pvcs.items.iter_mut() {
            let name = pvc.name_any();
            let task_id = parse_task_id_by_pvc_name(&name);
            assert!(!task_id.is_empty());
            if !self.pods_map.contains_key(&task_id) {
                orphan_pvcs.push(name);
            }
        }
        self.delete_pvc_by_names(orphan_pvcs).await;
    }

    pub(crate) async fn create_worker(&self, task_id: &str, data_size_gb: usize) -> Result<()> {
        if self.pods_map.len() >= self.config.worker_count_limit {
            return Err(Error::CheckError(format!(
                "worker count limit {} reached",
                self.config.worker_count_limit
            )));
        }

        match self.pods_map.entry(task_id.to_string()) {
            Entry::Occupied(_) => {
                // don't return directly because the pod may be not ready.
            }
            Entry::Vacant(e) => {
                let mut worker_pod = WorkerPod::default();
                worker_pod.name = new_worker_pod_name(task_id);
                e.insert(Arc::new(RwLock::new(worker_pod)));
            }
        }

        let sts_config = get_sts_config(data_size_gb, &self.config);
        let sts_name = new_worker_sts_name(task_id);
        let svc_name = new_worker_svc_name(task_id);
        match self.create_sts(task_id, sts_name.clone(), sts_config).await {
            Ok(()) => {
                info!("created sts {}", sts_name);
            }
            Err(kube::Error::Api(e)) if e.code == 409 => {
                info!("sts {} has created", sts_name);
            }
            Err(e) => {
                error!("failed to create sts {}, error {}", sts_name, e.to_string());
                return Err(Error::K8sError(e.to_string()));
            }
        }

        match self
            .create_svc(
                svc_name.clone(),
                sts_name.clone(),
                self.config.worker_port as i32,
            )
            .await
        {
            Ok(()) => {
                info!("created svc {}", svc_name);
            }
            Err(kube::Error::Api(e)) if e.code == 409 => {
                info!("svc {} has created", svc_name);
            }
            Err(e) => {
                error!("failed to create svc {}, error {}", svc_name, e.to_string());
                return Err(Error::K8sError(e.to_string()));
            }
        }
        info!("created sts {:?} with svc {:?}", sts_name, svc_name);

        match self
            .wait_worker_pod_ready(task_id, Duration::from_secs(DEFAULT_WAIT_POD_READY_TIMEOUT))
            .await
        {
            Ok(pod) => {
                let locked_worker_pod = self.pods_map.get_mut(task_id).unwrap().clone();
                let mut worker_pod = locked_worker_pod.write().await;
                if worker_pod.started_at == 0 {
                    worker_pod.init(task_id, &pod);
                }
                Ok(())
            }
            Err(err) => {
                error!(
                    "{} wait worker pod ready error {}",
                    task_id,
                    err.to_string()
                );
                Err(Error::CheckError(err.to_string()))
            }
        }
    }

    async fn create_sts(
        &self,
        task_id: &str,
        sts_name: String,
        sts_config: StsConfig,
    ) -> kube::Result<()> {
        let mut sts = self.sts_template.clone();
        sts.metadata = serde_json::from_value(json!({
            "name": sts_name.clone(),
            "namespace": self.config.namespace.clone(),
            "labels": {
                K8S_LABEL_NAME: self.config.name.clone()
            },
        }))
        .unwrap();
        let spec = sts.spec.as_mut().unwrap();
        spec.selector = serde_json::from_value(json!({
            "matchLabels": {
                K8S_LABEL_NAME: self.config.name.clone()
            }
        }))
        .unwrap();
        spec.replicas = Some(1);
        let pod_template = &mut spec.template;
        let pod_metadata = pod_template.metadata.as_mut().unwrap();
        let mut labels = pod_metadata.labels.take().unwrap_or_default();
        labels.insert(APP_LABEL.to_string(), self.config.name.clone());
        labels.insert(K8S_LABEL_NAME.to_string(), self.config.name.clone());
        labels.insert(K8S_LABEL_COMPONENT.to_string(), self.config.name.clone());
        labels.insert(K8S_LABEL_SERVICE.to_string(), sts_name.clone());
        pod_metadata.labels = Some(labels);
        let pod_template_spec = pod_template.spec.as_mut().unwrap();

        if !sts_config.node_group.is_empty() {
            let node_selector = if !sts_config.node_selector_key.is_empty()
                && !sts_config.node_selector_value.is_empty()
            {
                json!({
                    K8S_NODE_SELECTOR_NODE_NAME_KEY: sts_config.node_group,
                    sts_config.node_selector_key: sts_config.node_selector_value,
                })
            } else {
                json!({
                    K8S_NODE_SELECTOR_NODE_NAME_KEY: sts_config.node_group,
                })
            };
            pod_template_spec.node_selector = Some(serde_json::from_value(node_selector).unwrap());
            let tolerations = pod_template_spec.tolerations.as_mut().unwrap();
            tolerations.first_mut().unwrap().value = Some(sts_config.node_group);
        }

        let pod_container = pod_template_spec.containers.first_mut().unwrap();
        let request_cpu = format!("{}", sts_config.request_cpu);
        let request_memory = format!("{}Gi", sts_config.request_memory);
        let limit_cpu = format!("{}", sts_config.limit_cpu);
        let limit_memory = format!("{}Gi", sts_config.limit_memory);
        let resources = serde_json::from_value(json!({
            "requests": {
                "cpu": request_cpu,
                "memory": request_memory
            },
            "limits": {
                "cpu": limit_cpu,
                "memory": limit_memory
            },
        }))
        .unwrap();
        pod_container.resources = Some(resources);

        // disable liveness probe because we don't want the pod to be restarted by k8s.
        pod_container.liveness_probe = None;
        // add environment variable to prevent the load-data-worker to run
        // worker-scaler.
        let env = pod_container.env.as_mut().unwrap();
        env.push(EnvVar {
            name: LOAD_DATA_WORKER_ENV.to_string(),
            value: Some("true".to_string()),
            value_from: None,
        });
        env.push(EnvVar {
            name: LOAD_DATA_WORKER_WORKER_NUM_ENV.to_string(),
            value: Some(sts_config.worker_num.to_string()),
            value_from: None,
        });

        let pvcs = spec.volume_claim_templates.as_mut().unwrap();
        let first_pvc = pvcs.first_mut().unwrap();
        first_pvc.metadata.labels = Some(
            serde_json::from_value(json!({
                K8S_LABEL_NAME: self.config.name.clone()
            }))
            .unwrap(),
        );

        let mut pvc_template = first_pvc.clone();
        let volume_mounts = pod_container.volume_mounts.as_mut().unwrap();
        let root_path = volume_mounts.first().unwrap().mount_path.clone();
        for worker_id in 0..sts_config.worker_num {
            let name = format!("worker{}", worker_id);
            pvc_template.metadata.name = Some(name.clone());
            let pvc_template_spec = pvc_template.spec.as_mut().unwrap();
            pvc_template_spec.resources = Some(
                serde_json::from_value(json!({
                    "requests": {
                        "storage": format!("{}Gi", sts_config.storage_size_gb)
                    }
                }))
                .unwrap(),
            );
            pvcs.push(pvc_template.clone());
            volume_mounts.push(VolumeMount {
                mount_path: format!("{}/{}/worker-{}", root_path, task_id, worker_id),
                mount_propagation: None,
                name,
                read_only: None,
                sub_path: None,
                sub_path_expr: None,
            })
        }

        info!("create sts {:?}", sts_name);
        self.sts_api.create(&PostParams::default(), &sts).await?;
        Ok(())
    }

    async fn wait_worker_pod_ready(&self, task_id: &str, timeout: Duration) -> kube::Result<Pod> {
        let start = Instant::now();
        let pod_name = new_worker_pod_name(task_id);
        loop {
            if start.saturating_elapsed() > timeout {
                return Err(kube::Error::Service(box_err!(format!(
                    "wait worker {} ready timeout",
                    pod_name
                ))));
            }
            tokio::time::sleep(Duration::from_secs(3)).await;

            let pod = self.pod_api.get(&pod_name).await?;
            if pod.status.is_none() {
                continue;
            }
            let status = pod.status.as_ref().unwrap();
            if status.phase == Some("Running".into()) {
                return Ok(pod);
            }
        }
    }

    async fn delete_sts(&self, task_id: &str) {
        let sts_name = new_worker_sts_name(task_id);
        if let Err(err) = self.sts_api.delete(&sts_name, &Default::default()).await {
            warn!("delete sts {} err {:?}", sts_name, err);
        }
    }

    async fn delete_svc(&self, task_id: &str) {
        let svc_name = new_worker_svc_name(task_id);
        if let Err(err) = self.svc_api.delete(&svc_name, &Default::default()).await {
            warn!("delete sts {} err {:?}", svc_name, err);
        }
    }

    async fn list_pvcs_by_task_id(&self, task_id: String) -> kube::Result<Vec<String>> {
        let list_params = ListParams::default()
            .labels(&self.config.label_selector())
            .timeout(15);
        let pvcs = self.pvc_api.list(&list_params).await?;
        let mut pvc_names = vec![];
        for pvc in pvcs.items.iter() {
            let name = pvc.name_any();
            let cur_task_id = parse_task_id_by_pvc_name(&name);
            assert!(!task_id.is_empty());
            if cur_task_id == task_id {
                pvc_names.push(name);
            }
        }
        Ok(pvc_names)
    }

    async fn delete_pvc(&self, task_id: &str) {
        match self.list_pvcs_by_task_id(task_id.to_string()).await {
            Ok(pvc_names) => {
                self.delete_pvc_by_names(pvc_names).await;
            }
            Err(err) => {
                warn!("list {} pvc err {:?}", task_id, err);
            }
        }
    }

    async fn delete_pvc_by_names(&self, pvc_names: Vec<String>) {
        for pvc_name in &pvc_names {
            if let Err(err) = self.pvc_api.delete(pvc_name, &Default::default()).await {
                warn!("delete pvc {} err {:?}", pvc_name, err);
            }
        }
    }

    async fn get_pod_task_states(
        &self,
        worker_pod_name: &str,
        worker_addr: &str,
    ) -> Option<Vec<LoadTaskStates>> {
        let load_data_url = self.get_worker_tasks_url(worker_addr);
        let resp_body = if self.in_k8s {
            let uri = Uri::from_str(load_data_url.as_str()).unwrap();
            let resp_fut = self.http_client.get(uri);
            let timeout_dur = Duration::from_secs(5);
            let result = tokio::time::timeout(timeout_dur, resp_fut).await.ok()?;
            let resp = result.ok()?;
            if !resp.status().is_success() {
                error!("pod task status error: {}", resp.status());
                return None;
            }
            let body = hyper::body::to_bytes(resp.into_body()).await.ok()?;
            String::from_utf8(body.to_vec()).ok()?
        } else {
            let ap = AttachParams::default();
            let proc = self
                .pod_api
                .exec(worker_pod_name, vec!["curl", load_data_url.as_str()], &ap)
                .await
                .ok()?;
            get_proc_output(proc).await
        };
        let tasks: Vec<LoadTaskStates> = serde_json::from_str(&resp_body).ok()?;
        Some(tasks)
    }

    async fn get_pod_task_states_with_retry(
        &self,
        worker_pod_name: &str,
        worker_addr: &str,
    ) -> Option<Vec<LoadTaskStates>> {
        for _ in 0..WORKER_SCALER_QUERY_RETRY_COUNT {
            let tasks = self.get_pod_task_states(worker_pod_name, worker_addr).await;
            if tasks.is_some() {
                return tasks;
            }
            info!(
                "{} can't get tasks with addr {}",
                worker_pod_name, worker_addr
            );
            tokio::time::sleep(Duration::from_secs(WORKER_SCALER_QUERY_RETRY_INTERVAL)).await;
        }
        None
    }

    async fn maybe_clean_up(&self, task_id: &str) {
        info!("try to clean up {}", task_id);
        let pod_name = new_worker_pod_name(task_id);
        let pod = self.pod_api.get(&pod_name).await;
        if let Err(err) = pod {
            warn!("{} get pod {} err {:?}", task_id, pod_name, err);
            return;
        }
        let pod = pod.unwrap();
        if pod.status.is_none() || pod.status.clone().unwrap().phase.unwrap() != "Running" {
            warn!("{} is not running", pod_name);
            return;
        }

        let svc_name = new_worker_svc_name(task_id);
        if let Err(err) = self.svc_api.get(&svc_name).await {
            warn!("check svc {} err {:?}", svc_name, err);
            return;
        }
        let locked_worker_pod = self.pods_map.get_mut(task_id).unwrap().clone();
        let mut worker_pod = locked_worker_pod.write().await;
        if worker_pod.started_at == 0 {
            worker_pod.init(task_id, &pod);
        }

        // `canceled` flag will be set. We did not clean up the task
        // immediately, this way we can ensure that the pod can survive for
        // `CLEAN_UP_WORKER_TICK_INTERVAL` after completion.
        if worker_pod.cleanup {
            self.clean_up_task(task_id).await;
            return;
        }
        let worker_addr = self.get_worker_addr(&worker_pod);
        if worker_addr.is_none() {
            error!("{} cann't get worker addr", task_id);
            return;
        }
        let worker_pod_name = worker_pod.name.clone();
        // release lock
        drop(worker_pod);

        let tasks = self
            .get_pod_task_states_with_retry(&worker_pod_name, &worker_addr.unwrap())
            .await;
        let now_timestamp = chrono::Utc::now().timestamp();

        let mut worker_pod = locked_worker_pod.write().await;
        worker_pod.update_task_states(
            tasks,
            now_timestamp,
            self.config.pod_warm_up_sec,
            self.config.cancelled_task_expire_sec,
            self.config.finished_task_expire_sec,
            self.config.idle_task_expire_sec,
        );
    }

    async fn clean_up_task(&self, task_id: &str) {
        info!("{} task is being cleaned up in scaler", task_id);
        self.delete_sts(task_id).await;
        self.delete_svc(task_id).await;
        self.delete_pvc(task_id).await;
        self.pods_map.remove(task_id);
    }

    pub(crate) async fn delete_by_task_id_prefix(&self, task_id_prefix: &str) {
        let task_ids: Vec<String> = self
            .pods_map
            .iter()
            .filter(|entry| entry.key().starts_with(task_id_prefix))
            .map(|entry| entry.key().clone())
            .collect();

        for task_id in task_ids {
            self.clean_up_task(&task_id).await;
        }
    }

    pub(crate) fn get_worker_addr(&self, worker_pod: &WorkerPod) -> Option<String> {
        let svc_name = worker_pod.svc_name.as_str();
        if svc_name.is_empty() {
            return None;
        }
        self.security_mgr
            .build_uri(format!(
                "{}.{}.svc.cluster.local:{}/load_data",
                svc_name, &self.config.namespace, self.config.worker_port,
            ))
            .ok()
            .map(|uri| uri.to_string())
    }

    pub(crate) async fn get_worker_addr_by_task_id(&self, task_id: &str) -> Option<String> {
        let start = Instant::now();
        let timeout = Duration::from_secs(DEFAULT_WAIT_POD_READY_TIMEOUT);
        loop {
            {
                let locked_worker_pod = self.pods_map.get(task_id)?;
                let worker_pod = locked_worker_pod.read().await;
                if !worker_pod.svc_name.is_empty() {
                    return self.get_worker_addr(&worker_pod);
                }
            }
            if start.saturating_elapsed() > timeout {
                warn!("{} wait for load data worker timeout", task_id);
                return None;
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    pub(crate) fn get_worker_tasks_url(&self, worker_addr: &str) -> String {
        format!("{}?cluster_id={}", worker_addr, self.cluster_id)
    }

    pub(crate) async fn create_svc(
        &self,
        name: String,
        service: String,
        port: i32,
    ) -> kube::Result<()> {
        let svc = Service {
            metadata: ObjectMeta {
                name: Some(name.clone()),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                // create a headless svc
                cluster_ip: None,
                selector: Some(
                    [
                        (K8S_LABEL_NAME.to_string(), self.config.name.to_string()),
                        (K8S_LABEL_SERVICE.to_string(), service.to_string()),
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                ),
                ports: Some(vec![ServicePort {
                    port,
                    protocol: Some(NETWORK_PROTOCOL.to_string()),
                    target_port: Some(IntOrString::Int(port)),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        info!("create svc {:?}", name);
        self.svc_api.create(&PostParams::default(), &svc).await?;
        Ok(())
    }
}

fn parse_task_id_by_pod_name(pod_name: &str) -> String {
    // pod name format: ldw-{task-id}-0
    // task-id:         {keyspace-id}-{lightning-task-id}-{table-id}-{engine-id}
    //
    let fields: Vec<&str> = pod_name.split('-').collect();
    if fields.len() == 6 {
        // ldw-1-1701753296955293956-139-0-0
        return format!("{}-{}-{}-{}", fields[1], fields[2], fields[3], fields[4]);
    } else if fields.len() == 7 {
        // ldw-1-11701753296955293956-139--1-0
        return format!("{}-{}-{}--{}", fields[1], fields[2], fields[3], fields[5]);
    }
    "".to_string()
}

fn parse_task_id_by_pvc_name(pvc_name: &str) -> String {
    // pvc name format: {pvc-template-name}-ldw-{task-id}-0
    // task-id:         {keyspace-id}-{lightning-task-id}-{table-id}-{engine-id}
    //
    let fields: Vec<&str> = pvc_name.split('-').collect();
    if fields.len() == 7 {
        return format!("{}-{}-{}-{}", fields[2], fields[3], fields[4], fields[5]);
    } else if fields.len() == 8 {
        return format!("{}-{}-{}--{}", fields[2], fields[3], fields[4], fields[6]);
    }
    "".to_string()
}

fn new_worker_sts_name(task_id: &str) -> String {
    format!("ldw-{}", task_id)
}

fn new_worker_svc_name(task_id: &str) -> String {
    format!("ldw-{}", task_id)
}

fn new_worker_pod_name(task_id: &str) -> String {
    format!("ldw-{}-0", task_id)
}

async fn get_proc_output(mut attached: AttachedProcess) -> String {
    let stdout = tokio_util::io::ReaderStream::new(attached.stdout().unwrap());
    let out = stdout
        .filter_map(|r| async { r.ok().and_then(|v| String::from_utf8(v.to_vec()).ok()) })
        .collect::<Vec<_>>()
        .await
        .join("");
    attached.join().await.unwrap();
    out
}

fn gen_default_sts_configs() -> HashMap<usize, StsConfig> {
    // In order to let the load data worker pod exclusively occupy the node of the
    // node group, we use one-half of the node's CPU number as the request.
    let large_data_request_cpu = LARGE_DATA_LOAD_DATA_WORKER_NODE_GROUP_CPU_NUM / 2.0 + 1.0;
    let sts_configs_list = vec![
        StsConfig {
            min_data_size_gb: 500,
            request_cpu: large_data_request_cpu,
            limit_cpu: LARGE_DATA_LOAD_DATA_WORKER_NODE_GROUP_CPU_NUM,
            node_group: LARGE_DATA_LOAD_DATA_WORKER_NODE_GROUP_NAME.to_string(),
            worker_num: 16,
            ..Default::default()
        },
        StsConfig {
            min_data_size_gb: 100,
            request_cpu: 14.0,
            limit_cpu: 14.0,
            worker_num: 4,
            ..Default::default()
        },
        StsConfig {
            min_data_size_gb: 50,
            request_cpu: 7.0,
            limit_cpu: 7.0,
            worker_num: 1,
            ..Default::default()
        },
        StsConfig {
            min_data_size_gb: 10,
            request_cpu: 3.5,
            limit_cpu: 3.5,
            worker_num: 1,
            ..Default::default()
        },
        StsConfig {
            min_data_size_gb: 1,
            request_cpu: 2.0,
            limit_cpu: 2.0,
            worker_num: 1,
            ..Default::default()
        },
        StsConfig {
            min_data_size_gb: 0,
            request_cpu: 1.0,
            limit_cpu: 1.0,
            worker_num: 1,
            ..Default::default()
        },
    ];
    let sts_configs_map: HashMap<_, _> = sts_configs_list
        .into_iter()
        .map(|c| (c.min_data_size_gb, c))
        .collect();
    sts_configs_map
}

fn get_sts_config(data_size_gb: usize, config: &WorkerScalerConfig) -> StsConfig {
    let mut sts_config = find_sts_config(data_size_gb, config);
    if sts_config.storage_size_gb == 0 {
        sts_config.storage_size_gb = data_size_gb / sts_config.worker_num * 3;
    }
    sts_config.storage_size_gb = config.worker_min_storage_gb.max(sts_config.storage_size_gb);

    sts_config.request_cpu = config.worker_max_cores.min(sts_config.request_cpu);
    if sts_config.request_memory == 0.0 {
        sts_config.request_memory = sts_config.request_cpu * 4.0;
    }
    if sts_config.limit_memory == 0.0 {
        sts_config.limit_memory = sts_config.limit_cpu * 4.0;
    }
    sts_config
}

fn find_sts_config(data_size_gb: usize, config: &WorkerScalerConfig) -> StsConfig {
    let index = config
        .sts_configs
        .partition_point(|c| c.min_data_size_gb <= data_size_gb);
    config.sts_configs[index - 1].clone()
}

#[cfg(test)]
mod tests {
    use k8s_openapi::{
        api::core::v1::{Pod, PodCondition},
        apimachinery::pkg::apis::meta::v1::Time,
    };
    use load_data::task::LoadTaskStates;
    use rand::prelude::*;

    use crate::worker_scaler::{
        StsConfig, WORKER_SCALER_QUERY_FAILURES_LIMIT, WorkerPod, WorkerScalerConfig,
        find_sts_config, gen_default_sts_configs, new_worker_pod_name, new_worker_svc_name,
        parse_task_id_by_pod_name, parse_task_id_by_pvc_name,
    };

    fn new_worker_pvc_name(pvc_template_name: &str, task_id: &str) -> String {
        format!("{}-ldw-{}-0", pvc_template_name, task_id)
    }

    #[test]
    fn test_worker_pod_update() {
        let mut pod = Pod::default();
        let task_id = "taskid-1-0";
        let pod_name = new_worker_pod_name(task_id);
        pod.metadata.name = Some(pod_name.clone());
        pod.status = Some(Default::default());
        let status = pod.status.as_mut().unwrap();
        status.pod_ip = Some("127.0.0.1".to_string());
        let now = chrono::Utc::now();
        let now_ts = now.timestamp();
        status.conditions = Some(vec![PodCondition {
            last_transition_time: Some(Time(now)),
            message: Some("pod has been ready".to_string()),
            reason: Some("PodReady".to_string()),
            status: "True".to_string(),
            type_: "Ready".to_string(),
            ..Default::default()
        }]);

        let mut worker_pod = WorkerPod::new(pod_name.clone());
        worker_pod.init(task_id, &pod);
        assert_eq!(worker_pod.started_at, now_ts);
        assert_eq!(worker_pod.updated_at, now_ts);
        assert_eq!(worker_pod.svc_name, new_worker_svc_name(task_id));

        // worker pod is not canceled if no task states and pod don't warn up.
        worker_pod.update_task_states(None, now_ts + 40, 50, 150, 50, 200);
        assert!(!worker_pod.cleanup);

        // worker pod is not canceled if no task states and expired,
        // and retry retry count is less than the limit
        for _i in 0..WORKER_SCALER_QUERY_FAILURES_LIMIT {
            worker_pod.update_task_states(None, now_ts + 100, 50, 150, 50, 200);
            assert!(!worker_pod.cleanup);
        }

        // worker pod is canceled if no task states and pod warn up.
        worker_pod.update_task_states(None, now_ts + 100, 50, 150, 50, 200);
        assert!(worker_pod.cleanup);

        // worker pod is canceled if task is empty and expired.
        worker_pod = WorkerPod::new(pod_name.clone());
        worker_pod.init(task_id, &pod);
        worker_pod.update_task_states(Some(vec![]), now_ts + 100, 50, 150, 50, 200);
        assert!(worker_pod.cleanup);

        // worker pod is canceled if task is canceled.
        let mut task_states = LoadTaskStates::default();
        task_states.canceled = true;
        worker_pod = WorkerPod::new(pod_name.clone());
        worker_pod.init(task_id, &pod);
        worker_pod.update_task_states(
            Some(vec![task_states.clone()]),
            now_ts + 100,
            50,
            150,
            50,
            300,
        );
        assert!(!worker_pod.cleanup);
        worker_pod.update_task_states(Some(vec![task_states]), now_ts + 260, 50, 150, 50, 300);
        assert!(worker_pod.cleanup);

        // worker pod is canceled if task is finished and canceled, expired after
        // finish time.
        let mut task_states = LoadTaskStates::default();
        task_states.finished = true;
        task_states.canceled = true;
        worker_pod = WorkerPod::new(pod_name.clone());
        worker_pod.init(task_id, &pod);
        worker_pod.update_task_states(
            Some(vec![task_states.clone()]),
            now_ts + 120,
            50,
            200,
            100,
            300,
        );
        // worker pod is not canceled if task finished duration not exceed expire time.
        assert!(!worker_pod.cleanup);
        worker_pod.update_task_states(Some(vec![task_states]), now_ts + 240, 50, 200, 100, 300);
        // worker pod is canceled if task finished duration exceed expire time.
        assert!(worker_pod.cleanup);

        // worker pod is canceled if not finished and no progress for a long time.
        worker_pod = WorkerPod::new(pod_name);
        worker_pod.init(task_id, &pod);
        task_states = LoadTaskStates::default();
        task_states.created_files = 1;
        worker_pod.update_task_states(
            Some(vec![task_states.clone()]),
            now_ts + 100,
            50,
            200,
            100,
            300,
        );
        assert_eq!(worker_pod.created_files, 1);
        assert_eq!(worker_pod.updated_at, now_ts + 100);
        assert!(!worker_pod.cleanup);
        task_states.created_files = 2;
        worker_pod.update_task_states(
            Some(vec![task_states.clone()]),
            now_ts + 180,
            50,
            150,
            100,
            300,
        );
        assert_eq!(worker_pod.created_files, 2);
        assert_eq!(worker_pod.updated_at, now_ts + 180);
        assert!(!worker_pod.cleanup);
        worker_pod.update_task_states(Some(vec![task_states]), now_ts + 500, 50, 150, 100, 300);
        assert_eq!(worker_pod.created_files, 2);
        assert_eq!(worker_pod.updated_at, now_ts + 180);
        assert!(worker_pod.cleanup);
    }

    #[test]
    fn test_parse_task_id() {
        let task_ids = vec!["1-taskid-1-0", "1-taskid-1--0"];
        for task_id in task_ids {
            let pvc_template_name = "data0";
            let pod_name = new_worker_pod_name(task_id);
            let pvc_name = new_worker_pvc_name(pvc_template_name, task_id);
            assert_eq!(task_id, parse_task_id_by_pod_name(pod_name.as_str()));
            assert_eq!(task_id, parse_task_id_by_pvc_name(pvc_name.as_str()));
        }
    }

    #[test]
    fn test_config_merge_sts_configs() {
        let mut config = WorkerScalerConfig::default();
        // test override default config
        config.sts_configs.push(StsConfig {
            min_data_size_gb: 10,
            request_cpu: 100.0,
            limit_cpu: 100.0,
            worker_num: 1,
            ..Default::default()
        });
        // test invalid config that doesn't set `request_cpu`
        config.sts_configs.push(StsConfig {
            min_data_size_gb: 50,
            limit_cpu: 200.0,
            ..Default::default()
        });
        // test new config
        config.sts_configs.push(StsConfig {
            min_data_size_gb: 200,
            request_cpu: 200.0,
            limit_cpu: 200.0,
            node_group: "load-data-worker".to_string(),
            worker_num: 1,
            ..Default::default()
        });

        let mut sts_configs_map = gen_default_sts_configs();
        config.merge_sts_configs(sts_configs_map.clone());
        assert!(
            config.sts_configs.first().unwrap().min_data_size_gb
                < config.sts_configs.last().unwrap().min_data_size_gb
        );

        let sts_config = sts_configs_map.get_mut(&10).unwrap();
        sts_config.request_cpu = 100.0;
        sts_config.limit_cpu = 100.0;
        sts_config.worker_num = 1;
        sts_configs_map.insert(
            200,
            StsConfig {
                min_data_size_gb: 200,
                request_cpu: 200.0,
                limit_cpu: 200.0,
                node_group: "load-data-worker".to_string(),
                worker_num: 1,
                ..Default::default()
            },
        );

        let mut target_sts_configs: Vec<_> = sts_configs_map.into_iter().map(|c| c.1).collect();
        target_sts_configs.sort_by(|a, b| a.min_data_size_gb.cmp(&b.min_data_size_gb));
        assert_eq!(config.sts_configs, target_sts_configs);
    }

    #[test]
    fn test_find_sts_config() {
        let mut rng = thread_rng();
        let mut test_cases = vec![];
        let n = rng.gen_range(0..100);
        for i in 0..n {
            test_cases.push(rng.gen_range(0..i * 10 + 1));
        }

        let mut config = WorkerScalerConfig::default();
        let default_sts_configs = gen_default_sts_configs();
        config.merge_sts_configs(default_sts_configs.clone());

        for data_size_gb in test_cases.drain(..) {
            let sts_config = find_sts_config(data_size_gb, &config);
            let key: usize = match data_size_gb {
                500.. => 500,
                100..=499 => 100,
                50..=99 => 50,
                10..=49 => 10,
                1..=9 => 1,
                _ => 0,
            };
            let target_sts_config = default_sts_configs.get(&key).unwrap().clone();
            assert_eq!(sts_config, target_sts_config);
        }
    }
}
