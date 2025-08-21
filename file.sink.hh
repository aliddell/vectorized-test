#pragma once

#include <cstddef>
#include <vector>

namespace zarr {
    class FileSink {
    public:
        FileSink(std::string_view filename);

        ~FileSink();

        bool write(size_t offset, const std::vector<uint8_t> &data);

    protected:
        bool flush_();

    private:
        std::mutex mutex_;

        void *handle_;
    };
} // namespace zarr
