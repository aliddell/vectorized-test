#pragma once

#include <cstddef>
#include <cstdint> // uint8_t
#include <string>
#include <vector>

namespace zarr {
    class FileSink {
    public:
        explicit FileSink(const std::string& filename);
        ~FileSink();

        bool write(size_t offset, const std::vector<uint8_t> &data);

    protected:
        bool flush_();

    private:
        void *handle_;
    };
} // namespace zarr
