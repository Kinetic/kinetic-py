#!/bin/sh

BASE_DIR=$(cd "$(dirname "$0")"; pwd)
PROTO_DIR=$BASE_DIR/kinetic-protocol

protoc -I $PROTO_DIR --python_out=$BASE_DIR/kinetic $PROTO_DIR/kinetic.proto
