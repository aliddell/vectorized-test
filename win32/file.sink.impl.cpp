#include <iostream>
#include <string>

#include <windows.h>

std::string
get_last_error_as_string() {
    auto error_message_id = ::GetLastError();
    if (error_message_id == 0) {
        return std::string(); // No error message has been recorded
    }

    LPSTR message_buffer = nullptr;

    const auto format = FORMAT_MESSAGE_ALLOCATE_BUFFER |
                        FORMAT_MESSAGE_FROM_SYSTEM |
                        FORMAT_MESSAGE_IGNORE_INSERTS;
    const auto lang_id = MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT);
    size_t size = FormatMessageA(format,
                                 nullptr,
                                 error_message_id,
                                 lang_id,
                                 reinterpret_cast<LPSTR>(&message_buffer),
                                 0,
                                 nullptr);

    std::string message(message_buffer, size);

    LocalFree(message_buffer);

    return message;
}

void
init_handle(void **handle, std::string_view filename) {
    if (handle == nullptr) {
        throw std::runtime_error("Expected nonnull file handle");
    }
    auto *fd = new HANDLE;

    *fd = CreateFileA(filename.data(),
                      GENERIC_WRITE,
                      0, // No sharing
                      nullptr,
                      OPEN_ALWAYS,
                      FILE_FLAG_OVERLAPPED,
                      nullptr);

    if (*fd == INVALID_HANDLE_VALUE) {
        const auto err = get_last_error_as_string();
        delete fd;
        throw std::runtime_error("Failed to open file: '" +
                                 std::string(filename) + "': " + err);
    }
    *handle = reinterpret_cast<void *>(fd);
}

bool
seek_and_write(void **handle, size_t offset, const std::vector<uint8_t> &data) {
    if (handle == nullptr) {
        throw std::runtime_error("Expected nonnull file handle");
    }
    auto *fd = reinterpret_cast<HANDLE *>(*handle);

    auto *cur = reinterpret_cast<const char *>(data.data());
    auto *end = cur + data.size();

    int retries = 0;
    OVERLAPPED overlapped = {0};
    overlapped.hEvent = CreateEventA(nullptr, TRUE, FALSE, nullptr);

    const auto max_retries = 3;
    while (cur < end && retries < max_retries) {
        DWORD written = 0;
        auto remaining = static_cast<DWORD>(end - cur); // may truncate
        overlapped.Pointer = reinterpret_cast<void *>(offset);
        if (!WriteFile(*fd, cur, remaining, nullptr, &overlapped) &&
            GetLastError() != ERROR_IO_PENDING) {
            std::cerr << "Failed to write to file: " << get_last_error_as_string() << std::endl;
            CloseHandle(overlapped.hEvent);
            return false;
        }

        if (!GetOverlappedResult(*fd, &overlapped, &written, TRUE)) {
            std::cerr << "Failed to get overlapped result: " << get_last_error_as_string() << std::endl;
            CloseHandle(overlapped.hEvent);
            return false;
        }
        retries += (written == 0) ? 1 : 0;
        offset += written;
        cur += written;
    }

    CloseHandle(overlapped.hEvent);
    return (retries < max_retries);
}

bool
flush_file(void **handle) {
    if (handle == nullptr) {
        throw std::runtime_error("Expected nonnull file handle");
    }
    auto *fd = reinterpret_cast<HANDLE *>(*handle);
    if (fd && *fd != INVALID_HANDLE_VALUE) {
        return FlushFileBuffers(*fd);
    }
    return true;
}

void
destroy_handle(void **handle) {
    auto *fd = reinterpret_cast<HANDLE *>(*handle);
    if (fd) {
        if (*fd != INVALID_HANDLE_VALUE) {
            FlushFileBuffers(*fd);  // Ensure all buffers are flushed
            CloseHandle(*fd);
        }
        delete fd;
    }
}