// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// Copyright (c) 2016, Stepan Koltsov
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

use std::collections::HashMap;
use std::io::Write;

use protobuf::compiler_plugin;
use protobuf::descriptor::*;
use protobuf::descriptorx::*;

struct CodeWriter<'a> {
    writer: &'a mut (dyn Write + 'a),
    indent: String,
}

impl<'a> CodeWriter<'a> {
    pub fn new(writer: &'a mut dyn Write) -> CodeWriter<'a> {
        CodeWriter {
            writer,
            indent: "".to_string(),
        }
    }

    pub fn write_line<S: AsRef<str>>(&mut self, line: S) {
        (if line.as_ref().is_empty() {
            self.writer.write_all(b"\n")
        } else {
            let s: String = [self.indent.as_ref(), line.as_ref(), "\n"].concat();
            self.writer.write_all(s.as_bytes())
        })
        .unwrap();
    }

    pub fn write_generated(&mut self) {
        self.write_line("// This file is generated. Do not edit");
        self.write_generated_common();
    }

    fn write_generated_common(&mut self) {
        // https://secure.phabricator.com/T784
        self.write_line("// @generated");

        self.write_line("");
        self.comment("https://github.com/Manishearth/rust-clippy/issues/702");
        self.write_line("#![allow(unknown_lints)]");
        self.write_line("#![allow(clippy::all)]");
        self.write_line("");
        self.write_line("#![allow(box_pointers)]");
        self.write_line("#![allow(dead_code)]");
        self.write_line("#![allow(missing_docs)]");
        self.write_line("#![allow(non_camel_case_types)]");
        self.write_line("#![allow(non_snake_case)]");
        self.write_line("#![allow(non_upper_case_globals)]");
        self.write_line("#![allow(trivial_casts)]");
        self.write_line("#![allow(unsafe_code)]");
        self.write_line("#![allow(unused_imports)]");
        self.write_line("#![allow(unused_results)]");
    }

    pub fn indented<F>(&mut self, cb: F)
    where
        F: Fn(&mut CodeWriter),
    {
        cb(&mut CodeWriter {
            writer: self.writer,
            indent: format!("{}    ", self.indent),
        });
    }

    #[allow(dead_code)]
    pub fn commented<F>(&mut self, cb: F)
    where
        F: Fn(&mut CodeWriter),
    {
        cb(&mut CodeWriter {
            writer: self.writer,
            indent: format!("// {}", self.indent),
        });
    }

    pub fn block<F>(&mut self, first_line: &str, last_line: &str, cb: F)
    where
        F: Fn(&mut CodeWriter),
    {
        self.write_line(first_line);
        self.indented(cb);
        self.write_line(last_line);
    }

    pub fn expr_block<F>(&mut self, prefix: &str, cb: F)
    where
        F: Fn(&mut CodeWriter),
    {
        self.block(&format!("{} {{", prefix), "}", cb);
    }

    pub fn impl_self_block<S: AsRef<str>, F>(&mut self, name: S, cb: F)
    where
        F: Fn(&mut CodeWriter),
    {
        self.expr_block(&format!("impl {}", name.as_ref()), cb);
    }

    pub fn pub_struct<S: AsRef<str>, F>(&mut self, name: S, cb: F)
    where
        F: Fn(&mut CodeWriter),
    {
        self.expr_block(&format!("pub struct {}", name.as_ref()), cb);
    }

    pub fn pub_trait<F>(&mut self, name: &str, cb: F)
    where
        F: Fn(&mut CodeWriter),
    {
        self.expr_block(&format!("pub trait {}", name), cb);
    }

    pub fn field_entry(&mut self, name: &str, value: &str) {
        self.write_line(&format!("{}: {},", name, value));
    }

    pub fn field_decl(&mut self, name: &str, field_type: &str) {
        self.write_line(&format!("{}: {},", name, field_type));
    }

    pub fn comment(&mut self, comment: &str) {
        if comment.is_empty() {
            self.write_line("//");
        } else {
            self.write_line(&format!("// {}", comment));
        }
    }

    pub fn fn_block<F>(&mut self, public: bool, sig: &str, cb: F)
    where
        F: Fn(&mut CodeWriter),
    {
        if public {
            self.expr_block(&format!("pub fn {}", sig), cb);
        } else {
            self.expr_block(&format!("fn {}", sig), cb);
        }
    }

    pub fn pub_fn<F>(&mut self, sig: &str, cb: F)
    where
        F: Fn(&mut CodeWriter),
    {
        self.fn_block(true, sig, cb);
    }
}

use super::util::{self, fq_grpc, to_snake_case, MethodType};

struct MethodGen<'a> {
    proto: &'a MethodDescriptorProto,
    service_name: String,
    service_path: String,
    root_scope: &'a RootScope<'a>,
}

impl<'a> MethodGen<'a> {
    fn new(
        proto: &'a MethodDescriptorProto,
        service_name: String,
        service_path: String,
        root_scope: &'a RootScope<'a>,
    ) -> MethodGen<'a> {
        MethodGen {
            proto,
            service_name,
            service_path,
            root_scope,
        }
    }

    fn input(&self) -> String {
        format!(
            "super::{}",
            self.root_scope
                .find_message(self.proto.get_input_type())
                .rust_fq_name()
        )
    }

    fn output(&self) -> String {
        format!(
            "super::{}",
            self.root_scope
                .find_message(self.proto.get_output_type())
                .rust_fq_name()
        )
    }

    fn method_type(&self) -> (MethodType, String) {
        match (
            self.proto.get_client_streaming(),
            self.proto.get_server_streaming(),
        ) {
            (false, false) => (MethodType::Unary, fq_grpc("MethodType::Unary")),
            (true, false) => (
                MethodType::ClientStreaming,
                fq_grpc("MethodType::ClientStreaming"),
            ),
            (false, true) => (
                MethodType::ServerStreaming,
                fq_grpc("MethodType::ServerStreaming"),
            ),
            (true, true) => (MethodType::Duplex, fq_grpc("MethodType::Duplex")),
        }
    }

    fn service_name(&self) -> String {
        to_snake_case(&self.service_name)
    }

    fn name(&self) -> String {
        to_snake_case(self.proto.get_name())
    }

    fn fq_name(&self) -> String {
        format!("\"{}/{}\"", self.service_path, &self.proto.get_name())
    }

    fn const_method_name(&self) -> String {
        format!(
            "METHOD_{}_{}",
            self.service_name().to_uppercase(),
            self.name().to_uppercase()
        )
    }

    fn write_definition(&self, w: &mut CodeWriter) {
        let head = format!(
            "const {}: {}<{}, {}> = {} {{",
            self.const_method_name(),
            fq_grpc("Method"),
            self.input(),
            self.output(),
            fq_grpc("Method")
        );
        let pb_mar = format!(
            "{} {{ ser: {}, de: {} }}",
            fq_grpc("Marshaller"),
            fq_grpc("pb_ser"),
            fq_grpc("pb_de")
        );
        w.block(&head, "};", |w| {
            w.field_entry("ty", &self.method_type().1);
            w.field_entry("name", &self.fq_name());
            w.field_entry("req_mar", &pb_mar);
            w.field_entry("resp_mar", &pb_mar);
        });
    }

    // Method signatures
    fn unary(&self, method_name: &str) -> String {
        format!(
            "{}(&self, req: &{}) -> {}<{}>",
            method_name,
            self.input(),
            fq_grpc("Result"),
            self.output()
        )
    }

    fn unary_opt(&self, method_name: &str) -> String {
        format!(
            "{}_opt(&self, req: &{}, opt: {}) -> {}<{}>",
            method_name,
            self.input(),
            fq_grpc("CallOption"),
            fq_grpc("Result"),
            self.output()
        )
    }

    fn unary_full(&self, method_name: &str) -> String {
        format!(
            "{}_full(&self, req: &{}, opt: {}) -> {}<(Option<{}>, {}, Option<{}>)>",
            method_name,
            self.input(),
            fq_grpc("CallOption"),
            fq_grpc("Result"),
            fq_grpc("Metadata"),
            self.output(),
            fq_grpc("Metadata"),
        )
    }

    fn unary_async(&self, method_name: &str) -> String {
        format!(
            "{}_async(&self, req: &{}) -> {}<{}<{}>>",
            method_name,
            self.input(),
            fq_grpc("Result"),
            fq_grpc("ClientUnaryReceiver"),
            self.output()
        )
    }

    fn unary_async_opt(&self, method_name: &str) -> String {
        format!(
            "{}_async_opt(&self, req: &{}, opt: {}) -> {}<{}<{}>>",
            method_name,
            self.input(),
            fq_grpc("CallOption"),
            fq_grpc("Result"),
            fq_grpc("ClientUnaryReceiver"),
            self.output()
        )
    }

    fn unary_async_full(&self, method_name: &str) -> String {
        format!(
            "{}_async_full(&self, req: &{}, opt: {}) -> {}<{}<{}>>",
            method_name,
            self.input(),
            fq_grpc("CallOption"),
            fq_grpc("Result"),
            fq_grpc("FullClientUnaryReceiver"),
            self.output()
        )
    }

    fn client_streaming(&self, method_name: &str) -> String {
        format!(
            "{}(&self) -> {}<({}<{}>, {}<{}>)>",
            method_name,
            fq_grpc("Result"),
            fq_grpc("ClientCStreamSender"),
            self.input(),
            fq_grpc("ClientCStreamReceiver"),
            self.output()
        )
    }

    fn client_streaming_opt(&self, method_name: &str) -> String {
        format!(
            "{}_opt(&self, opt: {}) -> {}<({}<{}>, {}<{}>)>",
            method_name,
            fq_grpc("CallOption"),
            fq_grpc("Result"),
            fq_grpc("ClientCStreamSender"),
            self.input(),
            fq_grpc("ClientCStreamReceiver"),
            self.output()
        )
    }

    fn server_streaming(&self, method_name: &str) -> String {
        format!(
            "{}(&self, req: &{}) -> {}<{}<{}>>",
            method_name,
            self.input(),
            fq_grpc("Result"),
            fq_grpc("ClientSStreamReceiver"),
            self.output()
        )
    }

    fn server_streaming_opt(&self, method_name: &str) -> String {
        format!(
            "{}_opt(&self, req: &{}, opt: {}) -> {}<{}<{}>>",
            method_name,
            self.input(),
            fq_grpc("CallOption"),
            fq_grpc("Result"),
            fq_grpc("ClientSStreamReceiver"),
            self.output()
        )
    }

    fn duplex_streaming(&self, method_name: &str) -> String {
        format!(
            "{}(&self) -> {}<({}<{}>, {}<{}>)>",
            method_name,
            fq_grpc("Result"),
            fq_grpc("ClientDuplexSender"),
            self.input(),
            fq_grpc("ClientDuplexReceiver"),
            self.output()
        )
    }

    fn duplex_streaming_opt(&self, method_name: &str) -> String {
        format!(
            "{}_opt(&self, opt: {}) -> {}<({}<{}>, {}<{}>)>",
            method_name,
            fq_grpc("CallOption"),
            fq_grpc("Result"),
            fq_grpc("ClientDuplexSender"),
            self.input(),
            fq_grpc("ClientDuplexReceiver"),
            self.output()
        )
    }

    fn write_client(&self, w: &mut CodeWriter) {
        let method_name = self.name();
        match self.method_type().0 {
            // Unary
            MethodType::Unary => {
                w.pub_fn(&self.unary_full(&method_name), |w| {
                    w.write_line(&format!(
                        "self.client.unary_call_full(&{}, req, opt)",
                        self.const_method_name()
                    ));
                });
                w.write_line("");

                w.pub_fn(&self.unary_opt(&method_name), |w| {
                    w.write_line(&format!(
                        "self.client.unary_call(&{}, req, opt)",
                        self.const_method_name()
                    ));
                });
                w.write_line("");

                w.pub_fn(&self.unary(&method_name), |w| {
                    w.write_line(&format!(
                        "self.{}_opt(req, {})",
                        method_name,
                        fq_grpc("CallOption::default()")
                    ));
                });
                w.write_line("");

                w.pub_fn(&self.unary_async_full(&method_name), |w| {
                    w.write_line(&format!(
                        "self.client.unary_call_full_async(&{}, req, opt)",
                        self.const_method_name()
                    ));
                });
                w.write_line("");

                w.pub_fn(&self.unary_async_opt(&method_name), |w| {
                    w.write_line(&format!(
                        "self.client.unary_call_async(&{}, req, opt)",
                        self.const_method_name()
                    ));
                });
                w.write_line("");

                w.pub_fn(&self.unary_async(&method_name), |w| {
                    w.write_line(&format!(
                        "self.{}_async_opt(req, {})",
                        method_name,
                        fq_grpc("CallOption::default()")
                    ));
                });
            }

            // Client streaming
            MethodType::ClientStreaming => {
                w.pub_fn(&self.client_streaming_opt(&method_name), |w| {
                    w.write_line(&format!(
                        "self.client.client_streaming(&{}, opt)",
                        self.const_method_name()
                    ));
                });
                w.write_line("");

                w.pub_fn(&self.client_streaming(&method_name), |w| {
                    w.write_line(&format!(
                        "self.{}_opt({})",
                        method_name,
                        fq_grpc("CallOption::default()")
                    ));
                });
            }

            // Server streaming
            MethodType::ServerStreaming => {
                w.pub_fn(&self.server_streaming_opt(&method_name), |w| {
                    w.write_line(&format!(
                        "self.client.server_streaming(&{}, req, opt)",
                        self.const_method_name()
                    ));
                });
                w.write_line("");

                w.pub_fn(&self.server_streaming(&method_name), |w| {
                    w.write_line(&format!(
                        "self.{}_opt(req, {})",
                        method_name,
                        fq_grpc("CallOption::default()")
                    ));
                });
            }

            // Duplex streaming
            MethodType::Duplex => {
                w.pub_fn(&self.duplex_streaming_opt(&method_name), |w| {
                    w.write_line(&format!(
                        "self.client.duplex_streaming(&{}, opt)",
                        self.const_method_name()
                    ));
                });
                w.write_line("");

                w.pub_fn(&self.duplex_streaming(&method_name), |w| {
                    w.write_line(&format!(
                        "self.{}_opt({})",
                        method_name,
                        fq_grpc("CallOption::default()")
                    ));
                });
            }
        };
    }

    fn write_service(&self, w: &mut CodeWriter) {
        let req_stream_type = format!("{}<{}>", fq_grpc("RequestStream"), self.input());
        let (req, req_type, resp_type) = match self.method_type().0 {
            MethodType::Unary => ("req", self.input(), "UnarySink"),
            MethodType::ClientStreaming => ("stream", req_stream_type, "ClientStreamingSink"),
            MethodType::ServerStreaming => ("req", self.input(), "ServerStreamingSink"),
            MethodType::Duplex => ("stream", req_stream_type, "DuplexSink"),
        };
        let sig = format!(
            "{}(&mut self, ctx: {}, _{}: {}, sink: {}<{}>)",
            self.name(),
            fq_grpc("RpcContext"),
            req,
            req_type,
            fq_grpc(resp_type),
            self.output()
        );
        w.fn_block(false, &sig, |w| {
            w.write_line("grpcio::unimplemented_call!(ctx, sink)");
        });
    }

    fn write_bind(&self, w: &mut CodeWriter) {
        let add = match self.method_type().0 {
            MethodType::Unary => "add_unary_handler",
            MethodType::ClientStreaming => "add_client_streaming_handler",
            MethodType::ServerStreaming => "add_server_streaming_handler",
            MethodType::Duplex => "add_duplex_streaming_handler",
        };
        w.block(
            &format!(
                "builder = builder.{}(&{}, move |ctx, req, resp| {{",
                add,
                self.const_method_name()
            ),
            "});",
            |w| {
                w.write_line(&format!("instance.{}(ctx, req, resp)", self.name()));
            },
        );
    }
}

struct ServiceGen<'a> {
    proto: &'a ServiceDescriptorProto,
    methods: Vec<MethodGen<'a>>,
}

impl<'a> ServiceGen<'a> {
    fn new(
        proto: &'a ServiceDescriptorProto,
        file: &FileDescriptorProto,
        root_scope: &'a RootScope,
    ) -> ServiceGen<'a> {
        let service_path = if file.get_package().is_empty() {
            format!("/{}", proto.get_name())
        } else {
            format!("/{}.{}", file.get_package(), proto.get_name())
        };
        let methods = proto
            .get_method()
            .iter()
            .map(|m| {
                MethodGen::new(
                    m,
                    util::to_camel_case(proto.get_name()),
                    service_path.clone(),
                    root_scope,
                )
            })
            .collect();

        ServiceGen { proto, methods }
    }

    fn service_name(&self) -> String {
        util::to_camel_case(self.proto.get_name())
    }

    fn client_name(&self) -> String {
        format!("{}Client", self.service_name())
    }

    fn write_client(&self, w: &mut CodeWriter) {
        w.write_line("#[derive(Clone)]");
        w.pub_struct(&self.client_name(), |w| {
            w.field_decl("client", "::grpcio::Client");
        });

        w.write_line("");

        w.impl_self_block(&self.client_name(), |w| {
            w.pub_fn("new(channel: ::grpcio::Channel) -> Self", |w| {
                w.expr_block(&self.client_name(), |w| {
                    w.field_entry("client", "::grpcio::Client::new(channel)");
                });
            });

            for method in &self.methods {
                w.write_line("");
                method.write_client(w);
            }
            w.pub_fn(
                "spawn<F>(&self, f: F) where F: ::futures::Future<Output = ()> + Send + 'static",
                |w| {
                    w.write_line("self.client.spawn(f)");
                },
            )
        });
    }

    fn write_server(&self, w: &mut CodeWriter) {
        w.pub_trait(&self.service_name(), |w| {
            for method in &self.methods {
                method.write_service(w);
            }
        });

        w.write_line("");

        let s = format!(
            "create_{}<S: {} + Send + Clone + 'static>(s: S) -> {}",
            to_snake_case(&self.service_name()),
            self.service_name(),
            fq_grpc("Service")
        );
        w.pub_fn(&s, |w| {
            w.write_line("let mut builder = ::grpcio::ServiceBuilder::new();");
            for method in &self.methods[0..self.methods.len() - 1] {
                w.write_line("let mut instance = s.clone();");
                method.write_bind(w);
            }

            w.write_line("let mut instance = s;");
            self.methods[self.methods.len() - 1].write_bind(w);

            w.write_line("builder.build()");
        });
    }

    fn write_method_definitions(&self, w: &mut CodeWriter) {
        for (i, method) in self.methods.iter().enumerate() {
            if i != 0 {
                w.write_line("");
            }

            method.write_definition(w);
        }
    }

    fn write(&self, w: &mut CodeWriter) {
        self.write_method_definitions(w);
        w.write_line("");
        self.write_client(w);
        w.write_line("");
        self.write_server(w);
    }
}

fn gen_file(
    file: &FileDescriptorProto,
    root_scope: &RootScope,
) -> Option<compiler_plugin::GenResult> {
    if file.get_service().is_empty() {
        return None;
    }

    let base = protobuf::descriptorx::proto_path_to_rust_mod(file.get_name());

    let mut v = Vec::new();
    {
        let mut w = CodeWriter::new(&mut v);
        w.write_generated();

        for service in file.get_service() {
            w.write_line("");
            ServiceGen::new(service, file, root_scope).write(&mut w);
        }
    }

    Some(compiler_plugin::GenResult {
        name: base + "_grpc.rs",
        content: v,
    })
}

pub fn gen(
    file_descriptors: &[FileDescriptorProto],
    files_to_generate: &[String],
) -> Vec<compiler_plugin::GenResult> {
    let files_map: HashMap<&str, &FileDescriptorProto> =
        file_descriptors.iter().map(|f| (f.get_name(), f)).collect();

    let root_scope = RootScope { file_descriptors };

    let mut results = Vec::new();

    for file_name in files_to_generate {
        let file = files_map[&file_name[..]];

        if file.get_service().is_empty() {
            continue;
        }

        results.extend(gen_file(file, &root_scope).into_iter());
    }

    results
}

pub fn protoc_gen_grpc_rust_main() {
    compiler_plugin::plugin_main(gen);
}
