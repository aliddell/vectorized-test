#include "file.sink.hh"

#include <span>
#include <string_view>

void
init_handle(void **, std::string_view);

void
destroy_handle(void **);

bool
seek_and_write(void **, size_t, const std::vector<uint8_t> &);

bool
flush_file(void **);

zarr::FileSink::FileSink(const std::string& filename) {
    init_handle(&handle_, filename);
}

zarr::FileSink::~FileSink() {
    destroy_handle(&handle_);
}

bool
zarr::FileSink::write(size_t offset, const std::vector<uint8_t> &data) {
    if (data.data() == nullptr || data.empty()) {
        return true;
    }

    return seek_and_write(&handle_, offset, data);
}

bool
zarr::FileSink::flush_() {
    return flush_file(&handle_);
}