// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::PathBuf;

use bytes::Bytes;
use clap::Args;
use http::{Request, StatusCode, Uri};
use hyper::Body;
use kvproto::metapb::Store;

use crate::common::{CommonConfig, CtlContext};

#[derive(Args)]
pub struct HttpArgs {
    #[clap(long, default_value = "")]
    pub config: PathBuf,
    #[clap(short, long)]
    post: bool,
    #[clap(long, default_value = "")]
    body: String,
    #[clap(long)]
    path: String,
    #[clap(long, default_value = "")]
    pd: String,
    #[clap(long, default_value = "")]
    exclude_stores: String,
}

pub fn execute_http(args: HttpArgs) {
    let config = CommonConfig::from_args(args.config.as_path(), &args.pd);
    let ctx = config.create_context();
    let stores = ctx.get_tikv_stores(&args.exclude_stores);
    for store in stores {
        execute_http_on_store(&ctx, &store, &args.path, args.post, &args.body);
    }
}

pub fn build_uri(ctx: &CtlContext, store: &Store, path: &str) -> Uri {
    ctx.security_mgr
        .build_uri(format!("{}/{}", store.get_status_address(), path))
        .unwrap()
}

pub fn send_request(ctx: &CtlContext, post: bool, uri: Uri, body: &str) -> (StatusCode, Bytes) {
    let req = if post {
        Request::post(uri)
            .body(Body::from(body.to_owned()))
            .unwrap()
    } else {
        Request::get(uri).body(Body::empty()).unwrap()
    };
    let runtime = ctx.dfs.get_runtime();
    let resp = runtime.block_on(ctx.http_client.request(req)).unwrap();
    let status = resp.status();
    let body = runtime
        .block_on(hyper::body::to_bytes(resp.into_body()))
        .unwrap();
    (status, body)
}

pub fn execute_http_on_store(ctx: &CtlContext, store: &Store, path: &str, post: bool, body: &str) {
    let uri = build_uri(ctx, store, path);
    println!("sending request {} to store {}", uri, store.id);
    let (status, body) = send_request(ctx, post, uri, body);
    println!("status:{}", status);
    println!("{}", String::from_utf8_lossy(&body));
}
