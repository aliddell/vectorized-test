cmake_minimum_required(VERSION 4.0)
project(vectorized_test)

set(CMAKE_CXX_STANDARD 20)

if (WIN32)
    set(PLATFORM_FILE_SINK_CPP win32/file.sink.impl.cpp)
else ()
    set(PLATFORM_FILE_SINK_CPP posix/file.sink.impl.cpp)
endif ()

add_executable(vectorized_test main.cpp file.sink.cpp vectorized.file.writer.cpp ${PLATFORM_FILE_SINK_CPP})
