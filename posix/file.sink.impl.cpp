#include <iostream>
#include <string>
#include <vector>

#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <sys/uio.h>
#include <unistd.h>

std::string
get_last_error_as_string() {
    return strerror(errno);
}

void
init_handle(void **handle, std::string_view filename) {
    if (handle == nullptr) {
        throw std::runtime_error("Expected nonnull file handle");
    }
    auto *fd = new int;

    *fd = open(filename.data(), O_WRONLY | O_CREAT, 0644);
    if (*fd < 0) {
        const auto err = get_last_error_as_string();
        delete fd;
        throw std::runtime_error("Failed to open file: '" +
                                 std::string(filename) + "': " + err);
    }
    *handle = (void *) fd;
}

bool
seek_and_write(void **handle, size_t offset, const std::vector<uint8_t> &data) {
    if (handle == nullptr) {
        throw std::runtime_error("Expected nonnull file handle");
    }
    auto *fd = reinterpret_cast<int *>(*handle);

    auto *cur = reinterpret_cast<const char *>(data.data());
    auto *end = cur + data.size();

    int retries = 0;
    const auto max_retries = 3;
    while (cur < end && retries < max_retries) {
        size_t remaining = end - cur;
        ssize_t written = pwrite(*fd, cur, remaining, offset);
        if (written < 0) {
            const auto err = get_last_error_as_string();
            throw std::runtime_error("Failed to write to file: " + err);
        }
        retries += (written == 0) ? 1 : 0;
        offset += written;
        cur += written;
    }

    return (retries < max_retries);
}

bool
flush_file(void **handle) {
    if (handle == nullptr) {
        throw std::runtime_error("Expected nonnull file handle");
    }
    auto *fd = reinterpret_cast<int *>(*handle);

    const auto res = fsync(*fd);
    if (res < 0) {
        std::cerr << "Failed to flush file: " << get_last_error_as_string() << std::endl;
    }

    return res == 0;
}

void
destroy_handle(void **handle) {
    auto *fd = reinterpret_cast<int *>(*handle);
    if (fd) {
        if (*fd >= 0) {
            close(*fd);
        }
        delete fd;
    }
}