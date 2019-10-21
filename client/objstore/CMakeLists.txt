add_compile_options(-std=c++11)

SET(LIBRARY_DEPENDENCIES
    gRPC::grpc++_reflection
    gRPC::grpc
    gRPC::grpc++
    protobuf::libprotobuf
    protobuf::libprotobuf-lite
    protobuf::libprotoc
    anna-proto
    zmq
    hydro-zmq
    yaml-cpp
)

ADD_EXECUTABLE(anna-server annaobjstore.cpp)
TARGET_LINK_LIBRARIES(anna-server ${LIBRARY_DEPENDENCIES})
ADD_DEPENDENCIES(anna-server zeromq zeromqcpp)