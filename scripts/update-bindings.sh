#!/bin/bash

export UPDATE_BIND=1
cargo build
rustfmt grpc-sys/bindings/*
