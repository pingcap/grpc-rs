git submodule update --init grpc-sys/grpc
cd grpc-sys/grpc
git submodule update --init third_party/boringssl
git submodule update --init third_party/cares/cares
cd third_party/zlib
git clean -f
git reset --hard
