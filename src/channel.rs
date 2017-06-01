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


use std::{cmp, mem, ptr, usize};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::ffi::CString;
use std::sync::Arc;
use std::time::Duration;

use libc::{c_char, c_int};
use grpc_sys::{self, GprTimespec, GrpcChannel, GrpcChannelArgs};

use CallOption;
use call::{Call, Method};
use credentials::ChannelCredentials;
use cq::CompletionQueue;
use env::Environment;


// hack: add a '\0' to be compatible with c string without extra allocation.
const OPT_DEFAULT_AUTHORITY: &'static [u8] = b"grpc.default_authority\0";
const OPT_MAX_CONCURRENT_STREAMS: &'static [u8] = b"grpc.max_concurrent_streams\0";
const OPT_MAX_RECEIVE_MESSAGE_LENGTH: &'static [u8] = b"grpc.max_receive_message_length\0";
const OPT_MAX_SEND_MESSAGE_LENGTH: &'static [u8] = b"grpc.max_send_message_length\0";
const OPT_MAX_RECONNECT_BACKOFF_MS: &'static [u8] = b"grpc.max_reconnect_backoff_ms\0";
const OPT_INITIAL_RECONNECT_BACKOFF_MS: &'static [u8] = b"grpc.initial_reconnect_backoff_ms\0";
const OPT_HTTP2_INITIAL_SEQUENCE_NUMBER: &'static [u8] = b"grpc.http2.initial_sequence_number\0";
const OPT_SO_REUSE_PORT: &'static [u8] = b"grpc.so_reuseport\0";
const OPT_SSL_TARGET_NAME_OVERRIDE: &'static [u8] = b"grpc.ssl_target_name_override\0";
const PRIMARY_USER_AGENT_STRING: &'static [u8] = b"grpc.primary_user_agent\0";

/// Ref: http://www.grpc.io/docs/guides/wire.html#user-agents
fn format_user_agent_string(agent: &str) -> CString {
    let version = env!("CARGO_PKG_VERSION");
    let trimed_agent = agent.trim();
    let val = if trimed_agent.is_empty() {
        format!("grpc-rust/{}", version)
    } else {
        format!("{} grpc-rust/{}", trimed_agent, version)
    };
    CString::new(val).unwrap()
}

fn dur_to_ms(dur: Duration) -> usize {
    let millis = dur.as_secs() * 1000 + dur.subsec_nanos() as u64 / 1_000_000;
    cmp::min(usize::MAX as u64, millis) as usize
}

enum Options {
    Integer(usize),
    String(CString),
}

/// Channel configuration object.
pub struct ChannelBuilder {
    env: Arc<Environment>,
    options: HashMap<&'static [u8], Options>,
}

impl ChannelBuilder {
    pub fn new(env: Arc<Environment>) -> ChannelBuilder {
        ChannelBuilder {
            env: env,
            options: HashMap::new(),
        }
    }

    /// Default authority to pass if none specified on call construction.
    pub fn default_authority<S: Into<Vec<u8>>>(mut self, authority: S) -> ChannelBuilder {
        let authority = CString::new(authority).unwrap();
        self.options
            .insert(OPT_DEFAULT_AUTHORITY, Options::String(authority));
        self
    }

    /// Maximum number of concurrent incoming streams to allow on a http2 connection.
    pub fn max_concurrent_stream(mut self, num: usize) -> ChannelBuilder {
        self.options
            .insert(OPT_MAX_CONCURRENT_STREAMS, Options::Integer(num));
        self
    }

    /// Maximum message length that the channel can receive. usize::MAX means unlimited.
    pub fn max_receive_message_len(mut self, len: usize) -> ChannelBuilder {
        self.options
            .insert(OPT_MAX_RECEIVE_MESSAGE_LENGTH, Options::Integer(len));
        self
    }

    /// Maximum message length that the channel can send. -1 means unlimited.
    pub fn max_send_message_len(mut self, len: usize) -> ChannelBuilder {
        self.options
            .insert(OPT_MAX_SEND_MESSAGE_LENGTH, Options::Integer(len));
        self
    }

    /// The maximum time between subsequent connection attempts.
    pub fn max_reconnect_backoff(mut self, backoff: Duration) -> ChannelBuilder {
        self.options
            .insert(OPT_MAX_RECONNECT_BACKOFF_MS,
                    Options::Integer(dur_to_ms(backoff)));
        self
    }

    /// The time between the first and second connection attempts.
    pub fn initial_reconnect_backoff(mut self, backoff: Duration) -> ChannelBuilder {
        self.options
            .insert(OPT_INITIAL_RECONNECT_BACKOFF_MS,
                    Options::Integer(dur_to_ms(backoff)));
        self
    }

    /// Initial sequence number for http2 transports.
    pub fn https_initial_seq_number(mut self, number: usize) -> ChannelBuilder {
        self.options
            .insert(OPT_HTTP2_INITIAL_SEQUENCE_NUMBER, Options::Integer(number));
        self
    }

    /// Primary user agent: goes at the start of the user-agent metadata sent on each request.
    pub fn primary_user_agent(mut self, agent: &str) -> ChannelBuilder {
        let agent_string = format_user_agent_string(agent);
        self.options
            .insert(PRIMARY_USER_AGENT_STRING, Options::String(agent_string));
        self
    }

    /// If enable, allow the use of SO_REUSEPORT if it's available (default true).
    pub fn reuse_port(mut self, reuse: bool) -> ChannelBuilder {
        let opt = if reuse { 1 } else { 0 };
        self.options
            .insert(OPT_SO_REUSE_PORT, Options::Integer(opt));
        self
    }

    /// The caller of the secure_channel_create functions may override the target name used for SSL
    /// host name checking using this channel argument. This *should* be used for testing only.
    pub fn override_ssl_target<S: Into<Vec<u8>>>(mut self, target: S) -> ChannelBuilder {
        let target = CString::new(target).unwrap();
        self.options
            .insert(OPT_SSL_TARGET_NAME_OVERRIDE, Options::String(target));
        self
    }

    /// Build a channel args from the current configuration.
    pub fn build_args(&self) -> ChannelArgs {
        let args = unsafe { grpc_sys::grpcwrap_channel_args_create(self.options.len()) };
        for (i, (k, v)) in self.options.iter().enumerate() {
            let key = k.as_ptr() as *const c_char;
            match *v {
                Options::Integer(val) => unsafe {
                    grpc_sys::grpcwrap_channel_args_set_integer(args, i, key, val as c_int)
                },
                Options::String(ref val) => {
                    unsafe {
                        grpc_sys::grpcwrap_channel_args_set_string(args, i, key, val.as_ptr())
                    }
                }
            }
        }
        ChannelArgs { args: args }
    }

    /// Build an insure connection to the address.
    pub fn connect(self, addr: &str) -> Channel {
        self.connect_with_creds(addr, None)
    }

    fn connect_with_creds(mut self, addr: &str, creds: Option<ChannelCredentials>) -> Channel {
        let addr = CString::new(addr).unwrap();
        if let Entry::Vacant(e) = self.options.entry(PRIMARY_USER_AGENT_STRING) {
            e.insert(Options::String(format_user_agent_string("")));
        }
        let args = self.build_args();
        let addr_ptr = addr.as_ptr();
        let channel = unsafe {
            match creds {
                None => {
                    grpc_sys::grpc_insecure_channel_create(addr_ptr, args.args, ptr::null_mut())
                }
                Some(mut creds) => {
                    grpc_sys::grpc_secure_channel_create(creds.as_mut_ptr(),
                                                         addr_ptr,
                                                         args.args,
                                                         ptr::null_mut())
                }
            }
        };

        Channel {
            cq: self.env.pick_cq(),
            inner: Arc::new(ChannelInner {
                                _env: self.env,
                                channel: channel,
                            }),
        }
    }

    pub fn secure_connect(self, addr: &str, creds: ChannelCredentials) -> Channel {
        self.connect_with_creds(addr, Some(creds))
    }
}

pub struct ChannelArgs {
    args: *mut GrpcChannelArgs,
}

impl ChannelArgs {
    pub fn as_ptr(&self) -> *const GrpcChannelArgs {
        self.args
    }
}

impl Drop for ChannelArgs {
    fn drop(&mut self) {
        unsafe { grpc_sys::grpcwrap_channel_args_destroy(self.args) }
    }
}

struct ChannelInner {
    _env: Arc<Environment>,
    channel: *mut GrpcChannel,
}

impl Drop for ChannelInner {
    fn drop(&mut self) {
        unsafe {
            grpc_sys::grpc_channel_destroy(self.channel);
        }
    }
}

/// The Channel struct allows creation of Call objects.
#[derive(Clone)]
pub struct Channel {
    inner: Arc<ChannelInner>,
    cq: Arc<CompletionQueue>,
}

unsafe impl Send for Channel {}
unsafe impl Sync for Channel {}

impl Channel {
    /// Create a call using the method and option.
    pub fn create_call<P, Q>(&self, method: &Method<P, Q>, opt: &CallOption) -> Call {
        let raw_call = unsafe {
            let ch = self.inner.channel;
            let cq = self.cq.as_ptr();
            let method_ptr = method.name.as_ptr();
            let method_len = method.name.len();
            let timeout = opt.get_timeout()
                .map_or_else(GprTimespec::inf_future, GprTimespec::from);
            grpc_sys::grpcwrap_channel_create_call(ch,
                                                   ptr::null_mut(),
                                                   0,
                                                   cq,
                                                   method_ptr as *const _,
                                                   method_len,
                                                   ptr::null(),
                                                   0,
                                                   timeout,
                                                   ptr::null_mut())
        };

        unsafe { Call::from_raw(raw_call) }
    }

    pub fn cq(&self) -> &CompletionQueue {
        self.cq.as_ref()
    }
}
