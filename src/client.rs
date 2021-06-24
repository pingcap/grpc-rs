// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::call::client::{
    CallOption, ClientCStreamReceiver, ClientCStreamSender, ClientDuplexReceiver,
    ClientDuplexSender, ClientSStreamReceiver, ClientUnaryReceiver, FullClientUnaryReceiver,
};
use crate::call::{Call, Method};
use crate::channel::Channel;
use crate::error::Result;
use crate::task::Executor;
use crate::task::Kicker;
use crate::Metadata;
use futures::executor::block_on;
use futures::Future;

/// A generic client for making RPC calls.
#[derive(Clone)]
pub struct Client {
    channel: Channel,
    // Used to kick its completion queue.
    kicker: Kicker,
}

impl Client {
    /// Initialize a new [`Client`].
    pub fn new(channel: Channel) -> Client {
        let kicker = channel.create_kicker().unwrap();
        Client { channel, kicker }
    }

    /// Create a synchronized unary RPC call.
    ///
    /// It uses futures::executor::block_on to wait for the futures. It's recommended to use
    /// the asynchronous version.
    pub fn unary_call<Req, Resp>(
        &self,
        method: &Method<Req, Resp>,
        req: &Req,
        opt: CallOption,
    ) -> Result<Resp> {
        block_on(self.unary_call_async(method, req, opt)?)
    }

    /// Create a synchronized unary RPC call with metadata.
    ///
    /// It uses futures::executor::block_on to wait for the futures. It's recommended to use
    /// the asynchronous version.
    pub fn unary_call_full<Req, Resp>(
        &self,
        method: &Method<Req, Resp>,
        req: &Req,
        opt: CallOption,
    ) -> Result<(Option<Metadata>, Resp, Option<Metadata>)> {
        block_on(self.unary_call_full_async(method, req, opt)?)
    }

    /// Create an asynchronized unary RPC call.
    pub fn unary_call_async<Req, Resp>(
        &self,
        method: &Method<Req, Resp>,
        req: &Req,
        opt: CallOption,
    ) -> Result<ClientUnaryReceiver<Resp>> {
        Call::unary_async(&self.channel, method, req, opt)
    }

    /// Create an asynchronous unary RPC call with metadata support.
    pub fn unary_call_full_async<Req, Resp>(
        &self,
        method: &Method<Req, Resp>,
        req: &Req,
        opt: CallOption,
    ) -> Result<FullClientUnaryReceiver<Resp>> {
        Call::full_unary_async(&self.channel, method, req, opt)
    }

    /// Create an asynchronized client streaming call.
    ///
    /// Client can send a stream of requests and server responds with a single response.
    pub fn client_streaming<Req, Resp>(
        &self,
        method: &Method<Req, Resp>,
        opt: CallOption,
    ) -> Result<(ClientCStreamSender<Req>, ClientCStreamReceiver<Resp>)> {
        Call::client_streaming(&self.channel, method, opt)
    }

    /// Create an asynchronized server streaming call.
    ///
    /// Client sends on request and server responds with a stream of responses.
    pub fn server_streaming<Req, Resp>(
        &self,
        method: &Method<Req, Resp>,
        req: &Req,
        opt: CallOption,
    ) -> Result<ClientSStreamReceiver<Resp>> {
        Call::server_streaming(&self.channel, method, req, opt)
    }

    /// Create an asynchronized duplex streaming call.
    ///
    /// Client sends a stream of requests and server responds with a stream of responses.
    /// The response stream is completely independent and both side can be sending messages
    /// at the same time.
    pub fn duplex_streaming<Req, Resp>(
        &self,
        method: &Method<Req, Resp>,
        opt: CallOption,
    ) -> Result<(ClientDuplexSender<Req>, ClientDuplexReceiver<Resp>)> {
        Call::duplex_streaming(&self.channel, method, opt)
    }

    /// Spawn the future into current gRPC poll thread.
    ///
    /// This can reduce a lot of context switching, but please make
    /// sure there is no heavy work in the future.
    pub fn spawn<F>(&self, f: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let kicker = self.kicker.clone();
        Executor::new(self.channel.cq()).spawn(f, kicker)
    }
}
