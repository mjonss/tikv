// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    borrow::Cow,
    collections::{HashMap, VecDeque, hash_map::Entry},
    fs,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering::Relaxed},
    },
};

use dashmap::DashSet;
use http::{Method, Request, Response, StatusCode};
use hyper::Body;
use lazy_static::lazy_static;

lazy_static! {
    static ref KEYSPACE_WHITE_LIST: DashSet<u32> = DashSet::new();
    static ref KEYSPACE_REJECTS: Mutex<KeyspaceRejects> = Mutex::new(KeyspaceRejects::new(10000));
    static ref RECOVERY_MODE: AtomicBool = AtomicBool::new(false);
}

pub fn is_in_recovery_mode() -> bool {
    RECOVERY_MODE.load(Relaxed)
}

#[inline]
pub fn check_request_rejected(keyspace_id: u32) -> bool {
    if !is_in_recovery_mode() {
        return false;
    }
    let rejected = !in_white_list(keyspace_id);
    if rejected {
        add_rejected_keyspace(keyspace_id);
    }
    rejected
}

pub fn set_recovery_mode(enable: bool) {
    RECOVERY_MODE.store(enable, Relaxed);
}

pub fn add_white_list(keyspace: u32) {
    KEYSPACE_WHITE_LIST.insert(keyspace);
}

pub fn in_white_list(keyspace: u32) -> bool {
    KEYSPACE_WHITE_LIST.contains(&keyspace)
}

pub fn add_rejected_keyspace(keyspace: u32) {
    KEYSPACE_REJECTS
        .lock()
        .unwrap()
        .add_rejected_keyspace(keyspace);
}

pub fn handle_mode<F>(req: Request<Body>, on_update: F) -> Response<Body>
where
    F: Fn(),
{
    match *req.method() {
        Method::GET => make_response(StatusCode::OK, is_in_recovery_mode().to_string()),
        Method::POST => {
            let query = parse_query(&req);
            let enable = get_query_param::<bool>(&query, "enable").unwrap_or(false);
            set_recovery_mode(enable);
            on_update();
            make_response(StatusCode::OK, enable.to_string())
        }
        _ => make_response(StatusCode::METHOD_NOT_ALLOWED, "Method Not Allowed"),
    }
}

pub fn handle_rejected(req: Request<Body>, pending_compact: Vec<(u64, u32)>) -> Response<Body> {
    match *req.method() {
        Method::GET => {
            let mut rejected = KEYSPACE_REJECTS.lock().unwrap().get_rejects();
            for (_, keyspace_id) in pending_compact {
                rejected.push(keyspace_id);
            }
            rejected.sort();
            rejected.dedup();
            rejected.retain(|id| !KEYSPACE_WHITE_LIST.contains(id));
            let body_str = serde_json::to_string(&rejected).unwrap();
            make_response(StatusCode::OK, body_str)
        }
        _ => make_response(StatusCode::METHOD_NOT_ALLOWED, "Method Not Allowed"),
    }
}

pub fn handle_while_list<F>(req: Request<Body>, on_update: F) -> Response<Body>
where
    F: Fn(),
{
    match *req.method() {
        Method::GET => {
            let white_list = KEYSPACE_WHITE_LIST
                .iter()
                .map(|r| *r.key())
                .collect::<Vec<u32>>();
            let body_str = serde_json::to_string(&white_list).unwrap();
            make_response(StatusCode::OK, body_str)
        }
        Method::POST => {
            let query = parse_query(&req);
            let keyspace_ids = parse_keyspace_ids(&query);
            if keyspace_ids.is_empty() {
                make_response(StatusCode::BAD_REQUEST, "invalid keyspace_ids parameter")
            } else {
                for keyspace_id in keyspace_ids {
                    add_white_list(keyspace_id);
                }
                on_update();
                let white_list = KEYSPACE_WHITE_LIST
                    .iter()
                    .map(|r| *r.key())
                    .collect::<Vec<u32>>();
                let body_str = serde_json::to_string(&white_list).unwrap();
                make_response(StatusCode::OK, body_str)
            }
        }
        _ => make_response(StatusCode::METHOD_NOT_ALLOWED, "Method Not Allowed"),
    }
}

const BLACK_LIST_FILE_NAME: &str = "recovery_black_list";

fn black_list_file_path(data_dir: &str) -> PathBuf {
    Path::new(data_dir).join(BLACK_LIST_FILE_NAME)
}

pub fn load_black_list(data_dir: &str) -> Vec<u32> {
    let black_list_path = black_list_file_path(data_dir);
    if black_list_path.exists() {
        fs::read(black_list_path.as_path())
            .map(|data| serde_json::from_slice::<Vec<u32>>(&data).unwrap_or_default())
            .unwrap_or_default()
    } else {
        Vec::new()
    }
}

pub fn handle_black_list(req: Request<Body>, data_dir: &str) -> Response<Body> {
    let black_list_path = black_list_file_path(data_dir);
    match *req.method() {
        Method::GET => {
            let black_list = load_black_list(data_dir);
            make_response(StatusCode::OK, serde_json::to_vec(&black_list).unwrap())
        }
        Method::POST => {
            let file_data = fs::read(black_list_path.as_path()).unwrap_or_default();
            let mut black_list = serde_json::from_slice::<Vec<u32>>(&file_data).unwrap_or_default();
            let query = parse_query(&req);
            let ids = parse_keyspace_ids(&query);
            if !ids.is_empty() {
                let remove = get_query_param::<bool>(&query, "remove").unwrap_or(false);
                if remove {
                    black_list.retain(|id| !ids.contains(id));
                } else {
                    black_list.extend_from_slice(&ids);
                    black_list.sort();
                    black_list.dedup();
                }
            }
            let new_black_list = serde_json::to_vec(&black_list).unwrap();
            fs::write(black_list_path.as_path(), &new_black_list).unwrap();
            make_response(StatusCode::OK, new_black_list)
        }
        _ => make_response(StatusCode::METHOD_NOT_ALLOWED, "Method Not Allowed"),
    }
}

fn parse_query(req: &Request<Body>) -> HashMap<Cow<'_, str>, Cow<'_, str>> {
    let query = req.uri().query().unwrap_or("");
    url::form_urlencoded::parse(query.as_bytes()).collect()
}

fn get_query_param<T: FromStr>(
    query_pairs: &HashMap<Cow<'_, str>, Cow<'_, str>>,
    key: &str,
) -> Option<T> {
    let str = query_pairs.get(key).map(|v| v.as_ref())?;
    T::from_str(str).ok()
}

fn parse_keyspace_ids(query_pairs: &HashMap<Cow<'_, str>, Cow<'_, str>>) -> Vec<u32> {
    let keyspace_ids_str = query_pairs
        .get("keyspace_ids")
        .map(|v| v.as_ref())
        .unwrap_or("");
    keyspace_ids_str
        .split(',')
        .filter_map(|s| u32::from_str(s).ok())
        .collect()
}

fn make_response<T>(status_code: StatusCode, message: T) -> Response<Body>
where
    T: Into<Body>,
{
    Response::builder()
        .status(status_code)
        .body(message.into())
        .unwrap()
}

struct KeyspaceRejects {
    queue_len: usize,
    queue: VecDeque<u32>,
    map: HashMap<u32, usize>,
}

impl KeyspaceRejects {
    fn new(queue_len: usize) -> Self {
        KeyspaceRejects {
            queue_len,
            queue: VecDeque::new(),
            map: HashMap::new(),
        }
    }

    fn add_rejected_keyspace(&mut self, keyspace: u32) {
        self.queue.push_back(keyspace);
        let count = self.map.entry(keyspace).or_insert(0);
        *count += 1;
        if self.queue.len() >= self.queue_len {
            let front = self.queue.pop_front().unwrap();
            match self.map.entry(front) {
                Entry::Occupied(mut e) => {
                    if *e.get() == 1 {
                        e.remove();
                    } else {
                        *e.get_mut() -= 1;
                    }
                }
                Entry::Vacant(_) => unreachable!(),
            }
        }
    }

    fn get_rejects(&self) -> Vec<u32> {
        self.map.keys().copied().collect()
    }
}
