// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

extern crate benchmark;
extern crate clap;
extern crate futures;
extern crate grpc;
extern crate grpc_proto;
extern crate tokio_core;

use std::sync::Arc;

use benchmark::Worker;
use clap::{App, Arg};
use futures::sync::oneshot;
use grpc::{Environment, ServerBuilder};
use grpc_proto::testing::services_grpc;
use tokio_core::reactor::Core;

fn main() {
    let matches = App::new("Benchmark QpsWorker")
        .about("ref http://www.grpc.io/docs/guides/benchmarking.html")
        .arg(Arg::with_name("port")
                 .long("driver_port")
                 .help("The port the worker should listen on. For example, \"8080\"")
                 .takes_value(true))
        .get_matches();
    let port: u32 = matches
        .value_of("port")
        .unwrap_or("8080")
        .parse()
        .unwrap();

    let mut core = Core::new().unwrap();
    let env = Arc::new(Environment::new(2));
    let (tx, rx) = oneshot::channel();
    let worker = Worker::new(env.clone(), core.remote(), tx);
    let service = services_grpc::create_worker_service(worker);
    let mut server = ServerBuilder::new(env)
        .register_service(service)
        .bind("[::]", port)
        .build();

    for &(ref host, port) in server.bind_addrs() {
        println!("listening on {}:{}", host, port);
    }

    server.start();

    let _ = core.run(rx);
}

