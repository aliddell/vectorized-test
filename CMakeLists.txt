cmake_minimum_required(VERSION 3.23)
project(vectorized_test)

set(CMAKE_CXX_STANDARD 20)

if (WIN32)
    set(PLATFORM_FILE_SINK_CPP win32/file.sink.impl.cpp)
else ()
    set(PLATFORM_FILE_SINK_CPP posix/file.sink.impl.cpp)
endif ()

# OpenMP support
if (APPLE)
    # Determine architecture
    if (${CMAKE_HOST_SYSTEM_PROCESSOR} STREQUAL "arm64")
        # Apple Silicon
        set(LIBOMP_PATH "/opt/homebrew/opt/libomp")
        set(CMAKE_OSX_ARCHITECTURES "arm64")
    else ()
        # Intel Mac
        set(LIBOMP_PATH "/usr/local/opt/libomp")
        set(CMAKE_OSX_ARCHITECTURES "x86_64")
    endif ()

    # OpenMP support
    set(OpenMP_C_FLAGS "-Xclang -fopenmp -I${LIBOMP_PATH}/include")
    set(OpenMP_CXX_FLAGS "-Xclang -fopenmp -I${LIBOMP_PATH}/include")
    set(OpenMP_C_LIB_NAMES "omp")
    set(OpenMP_CXX_LIB_NAMES "omp")

    # Use static library
    set(OpenMP_omp_LIBRARY "${LIBOMP_PATH}/lib/libomp.a")

    # Ensure consistent architecture for all libraries
    add_compile_options(-march=native)

    # Add linking flags
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -L${LIBOMP_PATH}/lib")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -L${LIBOMP_PATH}/lib -lomp")

    # Disable universal binaries as they can cause architecture issues
    set(CMAKE_XCODE_ATTRIBUTE_ONLY_ACTIVE_ARCH "YES")
endif ()
find_package(OpenMP REQUIRED)

add_executable(vectorized_test main.cpp file.sink.cpp vectorized.file.writer.cpp ${PLATFORM_FILE_SINK_CPP})
target_link_libraries(vectorized_test
        OpenMP::OpenMP_CXX)