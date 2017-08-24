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


use grpc_proto::testing::services_grpc::BenchmarkService;
use grpc_proto::testing::messages::{SimpleRequest, SimpleResponse};
use grpc_proto::util;
use grpc::{self, DuplexSink, Method, MethodType, RequestStream, RpcContext, ServiceBuilder,
           UnarySink, WriteFlags};
use futures::{Future, Sink, Stream};

fn gen_resp(req: SimpleRequest) -> SimpleResponse {
    let payload = util::new_payload(req.get_response_size() as usize);
    let mut resp = SimpleResponse::new();
    resp.set_payload(payload);
    resp
}

#[derive(Clone)]
pub struct Benchmark;

impl BenchmarkService for Benchmark {
    fn unary_call(&self, ctx: RpcContext, req: SimpleRequest, sink: UnarySink<SimpleResponse>) {
        let resp = gen_resp(req);
        ctx.spawn(
            sink.success(resp)
                .map_err(|e| println!("failed to handle unary: {:?}", e)),
        )
    }

    fn streaming_call(
        &self,
        ctx: RpcContext,
        stream: RequestStream<SimpleRequest>,
        sink: DuplexSink<SimpleResponse>,
    ) {
        ctx.spawn(
            sink.send_all(stream.map(|req| (gen_resp(req), WriteFlags::default())))
                .map_err(|e| println!("failed to handle streaming: {:?}", e))
                .map(|_| {}),
        )
    }
}

#[derive(Clone)]
pub struct Generic;

impl Generic {
    pub fn streaming_call(
        &self,
        ctx: RpcContext,
        stream: RequestStream<Vec<u8>>,
        sink: DuplexSink<Vec<u8>>,
    ) {
        ctx.spawn(
            sink.send_all(stream.map(|req| (req, WriteFlags::default())))
                .map_err(|e| println!("failed to handle streaming: {:?}", e))
                .map(|_| {}),
        )
    }
}

#[inline]
pub fn bin_ser(t: &Vec<u8>, buf: &mut Vec<u8>) {
    buf.extend_from_slice(t)
}

#[inline]
pub fn bin_de(buf: &[u8]) -> grpc::Result<Vec<u8>> {
    Ok(buf.to_vec())
}

pub const METHOD_BENCHMARK_SERVICE_GENERIC_CALL: Method<Vec<u8>, Vec<u8>> = Method {
    ty: MethodType::Duplex,
    name: "/grpc.testing.BenchmarkService/StreamingCall",
    req_mar: ::grpc::Marshaller {
        ser: bin_ser,
        de: bin_de,
    },
    resp_mar: ::grpc::Marshaller {
        ser: bin_ser,
        de: bin_de,
    },
};

pub fn create_generic_service(s: Generic) -> ::grpc::Service {
    ServiceBuilder::new()
        .add_duplex_streaming_handler(
            &METHOD_BENCHMARK_SERVICE_GENERIC_CALL,
            move |ctx, req, resp| s.streaming_call(ctx, req, resp),
        )
        .build()
}
