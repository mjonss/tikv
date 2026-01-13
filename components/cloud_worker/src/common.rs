// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    borrow::Cow,
    collections::HashMap,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use http::{Response, StatusCode, header};
use hyper::Body;

pub(crate) fn get_param<T: FromStr>(
    query_pairs: &HashMap<Cow<'_, str>, Cow<'_, str>>,
    name: &str,
) -> Option<T> {
    query_pairs.get(name).and_then(|x| T::from_str(x).ok())
}

pub(crate) fn make_response<T>(status_code: StatusCode, message: T) -> Response<Body>
where
    T: Into<Body>,
{
    Response::builder()
        .status(status_code)
        .body(message.into())
        .unwrap()
}

pub(crate) fn make_json_response<T>(status_code: StatusCode, resp: &T) -> Response<Body>
where
    T: ?Sized + serde::Serialize,
{
    let json = serde_json::to_string(resp).unwrap();
    Response::builder()
        .status(status_code)
        .header(header::CONTENT_TYPE, "application/json")
        .body(json.into())
        .unwrap()
}

pub struct RunningController(Arc<AtomicBool>);

impl Default for RunningController {
    fn default() -> Self {
        Self(Arc::new(AtomicBool::new(true)))
    }
}

impl RunningController {
    pub fn stop(&self) {
        self.0.store(false, Ordering::Release);
    }

    pub fn handle(&self) -> Running {
        Running(self.0.clone())
    }
}

impl Drop for RunningController {
    fn drop(&mut self) {
        self.stop();
    }
}

#[derive(Clone)]
pub struct Running(Arc<AtomicBool>);

impl Running {
    pub fn get(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}
