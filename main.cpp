#include "file.sink.hh"
#include "vectorized.file.writer.hh"

#include <chrono>
#include <iostream>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <vector>

namespace fs = std::filesystem;

namespace {
    std::vector<std::vector<uint8_t>> make_data(size_t nchunks, size_t bytes_per_chunk) {
        std::vector<std::vector<uint8_t>> data(nchunks);
        for (auto &d: data) {
            d.resize(bytes_per_chunk);
        }

        return data;
    }

    void write_vectorized(const std::vector<std::vector<uint8_t>> &data, const std::string &path) {
        zarr::VectorizedFileWriter vfw(path);
        vfw.write_vectors(data, 0);
    }

    void consolidate_and_write(const std::vector<std::vector<uint8_t>> &data, const std::string &path) {
        size_t shard_size = 0;
        for (const auto &d: data) {
            shard_size += d.size();
        }

        std::vector<uint8_t> shard(shard_size);
        shard_size = 0;
        for (const auto &d: data) {
            memcpy(shard.data() + shard_size, d.data(), d.size());
            shard_size += d.size();
        }

        zarr::FileSink filesink(path);
        filesink.write(0, shard);
    }
}

void kernel(size_t nchunks, size_t &consolidated, size_t &vectorized) {
    const auto chunk_data = make_data(nchunks, 128 * 128 * 128);

    // time the consolidated write
    auto start = std::chrono::high_resolution_clock::now();
    consolidate_and_write(chunk_data, "consolidated.bin");
    auto end = std::chrono::high_resolution_clock::now();
    consolidated = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    // time the vectorized write
    start = std::chrono::high_resolution_clock::now();
    write_vectorized(chunk_data, "vectorized.bin");
    end = std::chrono::high_resolution_clock::now();
    vectorized = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
}

int main() {
    const size_t bytes_per_chunk = 128 * 128 * 128; // 2 MiB per chunk
    size_t consolidated_time, vectorized_time;

    std::ofstream results_csv("results.csv");
    std::cout << "bytes_written,consolidated_time,vectorized_time" << std::endl;
    results_csv << "bytes_written,consolidated_time,vectorized_time" << std::endl;

    for (auto nchunks = 32; nchunks < 1024; nchunks += 32) { // 1024 is IOV_MAX on Linux and macOS
        try {
            kernel(nchunks, consolidated_time, vectorized_time);
        } catch (const std::exception &exc) {
            std::cerr << "Error: " << exc.what() << std::endl;
            continue;
        }

        std::stringstream ss;
        ss << nchunks * bytes_per_chunk << "," << consolidated_time << ","
           << vectorized_time;

        std::cout << ss.str() << std::endl;
        results_csv << ss.str() << std::endl;

        // cleanup
        if (fs::exists("consolidated.bin")) {
            fs::remove("consolidated.bin");
        }
        if (fs::exists("vectorized.bin")) {
            fs::remove("vectorized.bin");
        }
    }

    return 0;
}