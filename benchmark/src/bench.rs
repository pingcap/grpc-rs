// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![allow(renamed_and_removed_lints)]

use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures::prelude::*;
use futures::sink::SinkExt;
use grpc::{
    self, ClientStreamingSink, DuplexSink, MessageReader, Method, MethodType, RequestStream,
    RpcContext, ServiceBuilder, UnarySink, WriteFlags,
};
use grpc_proto::testing::messages::{SimpleRequest, SimpleResponse};
use grpc_proto::testing::services_grpc::BenchmarkService;
use grpc_proto::util;
use grpcio::GrpcSlice;

fn gen_resp(req: &SimpleRequest) -> SimpleResponse {
    let payload = util::new_payload(req.get_response_size() as usize);
    let mut resp = SimpleResponse::default();
    resp.set_payload(payload);
    resp
}

#[derive(Clone)]
pub struct Benchmark {
    pub keep_running: Arc<AtomicBool>,
}

impl BenchmarkService for Benchmark {
    fn unary_call(&mut self, ctx: RpcContext, req: SimpleRequest, sink: UnarySink<SimpleResponse>) {
        let f = sink.success(gen_resp(&req));
        let keep_running = self.keep_running.clone();
        spawn!(ctx, keep_running, "unary", f)
    }

    fn streaming_call(
        &mut self,
        ctx: RpcContext,
        stream: RequestStream<SimpleRequest>,
        mut sink: DuplexSink<SimpleResponse>,
    ) {
        let f = async move {
            sink.send_all(
                &mut stream.map(|req| req.map(|req| (gen_resp(&req), WriteFlags::default()))),
            )
            .await?;
            sink.close().await?;
            Ok(())
        };
        let keep_running = self.keep_running.clone();
        spawn!(ctx, keep_running, "streaming", f)
    }

    fn streaming_from_client(
        &mut self,
        ctx: RpcContext,
        mut stream: RequestStream<SimpleRequest>,
        sink: ClientStreamingSink<SimpleResponse>,
    ) {
        let f = async move {
            let mut req = SimpleRequest::default();
            while let Some(r) = stream.try_next().await? {
                req = r;
            }
            sink.success(gen_resp(&req));
            Ok(())
        };
        let keep_running = self.keep_running.clone();
        spawn!(ctx, keep_running, "streaming from client", f)
    }
}

#[derive(Clone)]
pub struct Generic {
    pub keep_running: Arc<AtomicBool>,
}

impl Generic {
    pub fn streaming_call(
        &self,
        ctx: &RpcContext,
        stream: RequestStream<Vec<u8>>,
        mut sink: DuplexSink<Vec<u8>>,
    ) {
        let f = async move {
            sink.send_all(&mut stream.map(|req| req.map(|req| (req, WriteFlags::default()))))
                .await?;
            sink.close().await?;
            Ok(())
        };
        let keep_running = self.keep_running.clone();
        spawn!(ctx, keep_running, "streaming", f)
    }
}

#[inline]
#[allow(clippy::ptr_arg)]
pub fn bin_ser(t: &Vec<u8>, buf: &mut GrpcSlice) -> grpc::Result<()> {
    unsafe {
        let bytes = buf.realloc(t.len());
        let b = &mut *(bytes as *mut [std::mem::MaybeUninit<u8>] as *mut [u8]);
        b.copy_from_slice(t);
    }
    Ok(())
}

#[inline]
pub fn bin_de(mut reader: MessageReader) -> grpc::Result<Vec<u8>> {
    let mut buf = vec![];
    reader.read_to_end(&mut buf).unwrap();
    Ok(buf)
}

pub const METHOD_BENCHMARK_SERVICE_GENERIC_CALL: Method<Vec<u8>, Vec<u8>> = Method {
    ty: MethodType::Duplex,
    name: "/grpc.testing.BenchmarkService/StreamingCall",
    req_mar: crate::grpc::Marshaller {
        ser: bin_ser,
        de: bin_de,
    },
    resp_mar: crate::grpc::Marshaller {
        ser: bin_ser,
        de: bin_de,
    },
};

pub fn create_generic_service(s: Generic) -> crate::grpc::Service {
    ServiceBuilder::new()
        .add_duplex_streaming_handler(
            &METHOD_BENCHMARK_SERVICE_GENERIC_CALL,
            move |ctx, req, resp| s.streaming_call(&ctx, req, resp),
        )
        .build()
}
