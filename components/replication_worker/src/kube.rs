// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use k8s_openapi::{
    Metadata,
    api::{
        apps::v1::StatefulSet,
        core::v1::{EnvVar, PersistentVolumeClaim, Service, ServicePort, ServiceSpec},
    },
    apimachinery::pkg::{apis::meta::v1::ObjectMeta, util::intstr::IntOrString},
};
use kube::{Api, api::PostParams};
use pd_client::PdClient;
use security::SecurityConfig;
use tikv_util::{box_try, info, warn};

use crate::{
    Error, KeyspaceService, KeyspaceStates, ReplicationWorkerConfig, Result, bootstrap,
    util::new_keyspace_pd_client,
};

pub(crate) const K8S_LABEL_NAME: &str = "app.kubernetes.io/name";
const K8S_ANNOTATION_KEYSPACE_ID: &str = "serverless.tidbcloud.com/keyspace-id";

const ENV_REP_PD_STS_NAME: &str = "REP_PD_STS_NAME";
const ENV_REP_CDC_STS_NAME: &str = "REP_CDC_STS_NAME";
const ENV_REP_CDC_GC_TTL: &str = "REP_CDC_GC_TTL";

pub(crate) const PD_PORT: i32 = 2379;
pub(crate) const CDC_PORT: i32 = 8300;

pub(crate) struct KeyspaceKubeService {
    keyspace_id: u32,
    kube_api: Arc<KubeApi>,
    conf: ReplicationWorkerConfig,
    sec_conf: SecurityConfig,
    task_states: KeyspaceStates,
    pd_client: Option<Arc<dyn PdClient>>,
    scheme: String,
}

impl KeyspaceKubeService {
    pub(crate) fn new(
        keyspace_id: u32,
        kube_api: Arc<KubeApi>,
        conf: &ReplicationWorkerConfig,
        sec_conf: &SecurityConfig,
        mut task_states: KeyspaceStates,
    ) -> Self {
        // Always overwrite the pd/cdc info in task_states. In case we change the
        // deployment.
        task_states.pd_sts_name = kube_api.pd_sts_name(keyspace_id);
        task_states.cdc_sts_name = kube_api.cdc_sts_name(keyspace_id);
        let scheme = if sec_conf.ca_path.is_empty() {
            "http"
        } else {
            "https"
        }
        .to_string();
        task_states.pd_url = kube_api.compose_url(&scheme, &task_states.pd_sts_name, PD_PORT);
        task_states.cdc_addr = kube_api.compose_svc_addr(&task_states.cdc_sts_name, CDC_PORT);
        Self {
            keyspace_id,
            kube_api,
            conf: conf.clone(),
            sec_conf: sec_conf.clone(),
            task_states,
            pd_client: None,
            scheme,
        }
    }
}

#[async_trait]
impl KeyspaceService for KeyspaceKubeService {
    fn keyspace_id(&self) -> u32 {
        self.keyspace_id
    }

    async fn start(&mut self) -> Result<()> {
        let api = &self.kube_api;
        let start_pd = async {
            api.create_pd(&self.scheme, self.keyspace_id).await?;
            let pd_url = self.task_states.pd_url.clone();
            let pd_client = new_keyspace_pd_client(pd_url, &self.sec_conf).await;
            bootstrap(
                pd_client.clone(),
                self.conf.merged_engine.merged_store_id,
                self.conf.advertise_addr.clone(),
            )
            .await?;
            Ok::<_, Error>(pd_client)
        };
        let gc_ttl = self.conf.safepoint.gc_ttl.0;
        let start_cdc = async { api.create_cdc(&self.scheme, self.keyspace_id, gc_ttl).await };
        let (res_pd, res_cdc) = futures::join!(start_pd, start_cdc);
        self.pd_client = Some(box_try!(res_pd));
        let _ = box_try!(res_cdc);
        Ok(())
    }

    async fn destroy(&mut self) -> Result<()> {
        self.kube_api.delete_cdc(self.keyspace_id).await?;
        self.kube_api.delete_pd(self.keyspace_id).await?;
        Ok(())
    }

    fn get_states(&self) -> &KeyspaceStates {
        &self.task_states
    }

    fn get_states_mut(&mut self) -> &mut KeyspaceStates {
        &mut self.task_states
    }

    fn get_pd_client(&self) -> Arc<dyn PdClient> {
        self.pd_client.clone().unwrap()
    }
}

pub(crate) struct KubeApi {
    sts_api: Api<StatefulSet>,
    svc_api: Api<Service>,
    pvc_api: Api<PersistentVolumeClaim>,
    pd_sts_template_name: String,
    cdc_sts_template_name: String,
    namespace: String,
}

impl KubeApi {
    pub(crate) async fn new(
        pd_sts_template_name: String,
        cdc_sts_template_name: String,
        namespace: String,
    ) -> Result<Self> {
        let client = kube::Client::try_default().await?;
        let sts_api = Api::namespaced(client.clone(), &namespace);
        let svc_api = Api::namespaced(client.clone(), &namespace);
        let pvc_api = Api::namespaced(client.clone(), &namespace);
        Ok(Self {
            sts_api,
            svc_api,
            pvc_api,
            pd_sts_template_name,
            cdc_sts_template_name,
            namespace,
        })
    }

    pub(crate) async fn create_pd(&self, scheme: &str, keyspace_id: u32) -> Result<()> {
        let pd_sts_name = self.pd_sts_name(keyspace_id);
        self.create_svc(pd_sts_name.clone(), PD_PORT).await?;
        if self.sts_api.get_opt(&pd_sts_name).await?.is_some() {
            info!("sts {} exists", pd_sts_name);
            return Ok(());
        }
        let mut pd_sts = self.sts_api.get(&self.pd_sts_template_name).await?;
        pd_sts.metadata.name = Some(pd_sts_name.clone());
        pd_sts.metadata.resource_version = None;
        let spec = pd_sts.spec.as_mut().unwrap();
        spec.replicas = Some(1);
        spec.service_name = pd_sts_name.clone();
        spec.selector.match_labels = Some(Self::create_label_name(&pd_sts_name));
        let metadata = spec.template.metadata.as_mut().unwrap();
        Self::patch_labels(metadata, &pd_sts_name);
        Self::patch_annotations(metadata, keyspace_id);
        let pod_spec = spec.template.spec.as_mut().unwrap();
        let container = pod_spec.containers.first_mut().unwrap();

        let envs = container.env.get_or_insert_default();
        envs.push(EnvVar {
            name: ENV_REP_PD_STS_NAME.into(),
            value: Some(pd_sts_name.clone()),
            ..Default::default()
        });

        // For backward compatibility. TODO: remove after deployment upgrade.
        let command = container.command.as_mut().unwrap();
        if command.first().is_some_and(|x| !x.contains("/bin/sh")) {
            let pd_client_url = self.compose_url(scheme, &pd_sts_name, PD_PORT);
            let pd_peer_url = self.compose_url(scheme, &pd_sts_name, PD_PORT + 1);
            command.push(format!("--advertise-client-urls={}", pd_client_url));
            command.push(format!("--advertise-peer-urls={}", pd_peer_url));
        }

        match self.sts_api.create(&Default::default(), &pd_sts).await {
            Ok(_) => Ok(()),
            Err(e) => {
                warn!("Failed to create StatefulSet"; "keyspace id" => keyspace_id, "error" => ?e);
                Err(Error::KubeError(e))
            }
        }?;
        Ok(())
    }

    pub(crate) async fn create_cdc(
        &self,
        scheme: &str,
        keyspace_id: u32,
        gc_ttl: Duration,
    ) -> Result<()> {
        let cdc_sts_name = self.cdc_sts_name(keyspace_id);
        self.create_svc(cdc_sts_name.clone(), CDC_PORT).await?;
        if self.sts_api.get_opt(&cdc_sts_name).await?.is_some() {
            info!("sts {} already exists", cdc_sts_name);
            return Ok(());
        }
        let mut cdc_sts = self.sts_api.get(&self.cdc_sts_template_name).await?;
        cdc_sts.metadata.name = Some(cdc_sts_name.clone());
        cdc_sts.metadata.resource_version = None;
        let spec = cdc_sts.spec.as_mut().unwrap();
        spec.replicas = Some(1);
        spec.service_name = cdc_sts_name.clone();
        spec.selector.match_labels = Some(Self::create_label_name(&cdc_sts_name));
        let metadata = spec.template.metadata.as_mut().unwrap();
        Self::patch_labels(metadata, &cdc_sts_name);
        Self::patch_annotations(metadata, keyspace_id);
        let pod_spec = spec.template.spec.as_mut().unwrap();
        let container = pod_spec.containers.first_mut().unwrap();
        let pd_sts_name = self.pd_sts_name(keyspace_id);

        // For backward compatibility. TODO: remove after deployment upgrade.
        let command = container.command.as_mut().unwrap();
        if command.first().is_some_and(|x| !x.contains("/bin/sh")) {
            command.push(format!("--addr={}", self.compose_listen_addr(CDC_PORT)));
            command.push(format!(
                "--advertise-addr={}",
                self.compose_svc_addr(&cdc_sts_name, CDC_PORT)
            ));
            command.push(format!(
                "--pd={}",
                self.compose_url(scheme, &pd_sts_name, PD_PORT)
            ));
        }

        let envs = container.env.get_or_insert_default();
        envs.push(EnvVar {
            name: ENV_REP_PD_STS_NAME.into(),
            value: Some(pd_sts_name),
            ..Default::default()
        });
        envs.push(EnvVar {
            name: ENV_REP_CDC_STS_NAME.into(),
            value: Some(cdc_sts_name),
            ..Default::default()
        });
        envs.push(EnvVar {
            name: ENV_REP_CDC_GC_TTL.into(),
            value: Some(gc_ttl.as_secs().to_string()),
            ..Default::default()
        });

        match self.sts_api.create(&Default::default(), &cdc_sts).await {
            Ok(_) => Ok(()),
            Err(e) => {
                warn!("Failed to create StatefulSet"; "keyspace_id" => keyspace_id, "error" => ?e);
                Err(Error::KubeError(e))
            }
        }
    }

    fn compose_url(&self, scheme: &str, sts_name: &str, port: i32) -> String {
        format!("{}://{}", scheme, self.compose_svc_addr(sts_name, port))
    }

    fn compose_svc_addr(&self, sts_name: &str, port: i32) -> String {
        format!("{}.{}.svc.cluster.local:{}", sts_name, self.namespace, port)
    }

    fn compose_listen_addr(&self, port: i32) -> String {
        format!("0.0.0.0:{}", port)
    }

    fn create_label_name(sts_name: &str) -> BTreeMap<String, String> {
        BTreeMap::from_iter(vec![(K8S_LABEL_NAME.to_string(), sts_name.to_string())])
    }

    fn patch_labels(metadata: &mut ObjectMeta, sts_name: &str) {
        let labels = metadata.labels.get_or_insert_default();
        labels.extend(Self::create_label_name(sts_name));
    }

    fn patch_annotations(metadata: &mut ObjectMeta, keyspace_id: u32) {
        let annotations = metadata.annotations.get_or_insert_default();
        annotations.insert(
            K8S_ANNOTATION_KEYSPACE_ID.to_string(),
            keyspace_id.to_string(),
        );
    }

    fn pod_name(sts_name: &str) -> String {
        format!("{}-0", sts_name)
    }

    fn pvc_name(sts_name: &str) -> String {
        format!("data-{}", Self::pod_name(sts_name))
    }

    pub(crate) async fn create_svc(&self, sts_name: String, port: i32) -> kube::Result<()> {
        if self.svc_api.get_opt(&sts_name).await?.is_some() {
            info!("service {} exists", sts_name);
            return Ok(());
        }
        let mut svc = Service::default();
        let metadata = svc.metadata_mut();
        metadata.name = Some(sts_name.clone());
        let mut service_port = ServicePort::default();
        service_port.port = port;
        service_port.target_port = Some(IntOrString::Int(port));
        svc.spec = Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            selector: Some(Self::create_label_name(&sts_name)),
            ports: Some(vec![service_port]),
            ..Default::default()
        });
        self.svc_api.create(&PostParams::default(), &svc).await?;
        info!("create svc {:?}", sts_name);
        Ok(())
    }

    pub(crate) async fn delete_sts_svc_pvc(&self, sts_name: &str) -> kube::Result<()> {
        let res = self.delete_sts(sts_name).await;
        if res.is_err() {
            warn!("Failed to delete StatefulSet"; "sts_name" => sts_name, "error" => ?res);
        }
        let res = self.delete_svc(sts_name).await;
        if res.is_err() {
            warn!("Failed to delete Service"; "sts_name" => sts_name, "error" => ?res);
        }
        let res = self.delete_pvc(sts_name).await;
        if res.is_err() {
            warn!("Failed to delete PVC"; "sts_name" => sts_name, "error" => ?res);
        }
        Ok(())
    }

    pub(crate) async fn delete_sts(&self, sts_name: &str) -> kube::Result<()> {
        self.sts_api.delete(sts_name, &Default::default()).await?;
        info!("delete sts {:?}", sts_name);
        Ok(())
    }

    pub(crate) async fn delete_svc(&self, sts_name: &str) -> kube::Result<()> {
        self.svc_api.delete(sts_name, &Default::default()).await?;
        info!("delete svc {:?}", sts_name);
        Ok(())
    }

    pub(crate) async fn delete_pvc(&self, sts_name: &str) -> kube::Result<()> {
        let pvc_name = Self::pvc_name(sts_name);
        self.pvc_api.delete(&pvc_name, &Default::default()).await?;
        info!("delete pvc {:?}", pvc_name);
        Ok(())
    }

    pub(crate) async fn delete_pd(&self, keyspace_id: u32) -> Result<()> {
        let pd_sts_name = self.pd_sts_name(keyspace_id);
        self.delete_sts_svc_pvc(&pd_sts_name).await?;
        Ok(())
    }

    pub(crate) async fn delete_cdc(&self, keyspace_id: u32) -> Result<()> {
        let cdc_sts_name = self.cdc_sts_name(keyspace_id);
        self.delete_sts_svc_pvc(&cdc_sts_name).await?;
        Ok(())
    }

    fn cdc_sts_name(&self, keyspace_id: u32) -> String {
        format!("{}-ks{}", self.cdc_sts_template_name, keyspace_id)
    }

    fn pd_sts_name(&self, keyspace_id: u32) -> String {
        format!("{}-ks{}", self.pd_sts_template_name, keyspace_id)
    }
}

#[test]
#[ignore] // Require K8S env. For manual testing only
fn test_sts_create() {
    test_util::init_log_for_test();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let f = async {
        let api = KubeApi::new("rep-pd".into(), "rep-cdc".into(), "default".to_string())
            .await
            .unwrap();
        api.create_pd("http", 1).await.unwrap();
        let pd_name = api.pd_sts_name(1);
        let sts_res = api.sts_api.get(&pd_name).await;
        info!("pd_sts: {:?}", sts_res);
        let svc_res = api.svc_api.get(&pd_name).await;
        info!("pd_svc: {:?}", svc_res);
        let pvc_res = api.pvc_api.get(&KubeApi::pvc_name(&pd_name)).await;
        info!("pd_pvc: {:?}", pvc_res);

        api.create_cdc("http", 1, Duration::from_secs(86400))
            .await
            .unwrap();
        let cdc_name = api.cdc_sts_name(1);
        let sts_res = api.sts_api.get(&cdc_name).await;
        info!("cdc_sts: {:?}", sts_res);
        let svc_res = api.svc_api.get(&cdc_name).await;
        info!("cdc_svc: {:?}", svc_res);
        let pvc_res = api.pvc_api.get(&KubeApi::pvc_name(&cdc_name)).await;
        info!("cdc_pvc: {:?}", pvc_res);

        // api.delete_pd(1).await.unwrap();
    };
    rt.block_on(f);
}

#[test]
#[ignore] // Require K8S env. For manual testing only
fn test_sts_delete() {
    test_util::init_log_for_test();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let f = async {
        let api = KubeApi::new("rep-pd".into(), "rep-cdc".into(), "default".to_string())
            .await
            .unwrap();
        let res = api.delete_pd(1).await;
        info!("delete pd {}", res.is_ok());
        let res = api.delete_cdc(1).await;
        info!("delete cdc {}", res.is_ok());
    };
    rt.block_on(f);
}
