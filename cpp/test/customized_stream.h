#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/threading/Executor.h>
// #include <aws/core/utils/threading/PooledThreadExecutor.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/logging/DefaultLogSystem.h>
#include <aws/core/utils/logging/AWSLogging.h>

#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/GetObjectResult.h>

#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/model/GetObjectRequest.h>
#include <aws/s3-crt/model/GetObjectResult.h>

#include <chrono>
#include <iostream>
#include <vector>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <fstream>
#include <memory>
#include <iomanip>
#include <future>
#include <sstream>
#include <string>

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include <streambuf>

using namespace Aws;
using namespace Aws::S3;
using namespace Aws::S3Crt;

// =================================================================================================
// Custom Streambuf for Direct I/O
// =================================================================================================

/**
 * A custom C++ streambuf designed to write data to a file using direct I/O (O_DIRECT).
 * This is a complex implementation intended to bypass the OS page cache for performance testing.
 *
 * How it works:
 * 1.  It opens a file using the low-level `open()` system call with the O_DIRECT flag.
 * 2.  It maintains a large, internal buffer that is memory-aligned to the block size (4096 bytes),
 *     which is a requirement for direct I/O.
 * 3.  The AWS SDK writes data into the small buffer provided by `std::streambuf`'s put area.
 * 4.  When this small buffer is full, `overflow()` is called. We copy the data into our large, aligned buffer.
 * 5.  Once our large buffer contains at least one full block of data, we write the block(s) to the file
 *     using the `write()` system call. Any partial block remains in the buffer for the next write.
 * 6.  `sync()` is called to flush data. The most critical part is in the destructor, which ensures
 *     the final, partial block (the remainder) is written.
 * 7.  To write the remainder, use truncate.
 */

class DirectIOStreambuf : public std::streambuf {
public:
    explicit DirectIOStreambuf(const std::string& filename, size_t buffer_size = 4096 * 16)
        : d_filename(filename),
          d_fd(-1),
          d_buffer_size(buffer_size),
          d_buffer(nullptr),
          d_total_bytes_written(0) {
        
        if (d_buffer_size % d_alignment != 0) {
            d_buffer_size = ((d_buffer_size / d_alignment) + 1) * d_alignment;
        }

        d_fd = open(d_filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT, 0666);
        if (d_fd == -1) {
            perror("DirectIOStreambuf: Failed to open file with O_DIRECT");
            throw std::runtime_error("Failed to open file with O_DIRECT for " + d_filename);
        }

        if (posix_memalign(reinterpret_cast<void**>(&d_buffer), d_alignment, d_buffer_size) != 0) {
            close(d_fd);
            throw std::runtime_error("Failed to allocate aligned memory for DirectIOStreambuf");
        }

        setp(d_buffer, d_buffer + d_buffer_size);
    }

    ~DirectIOStreambuf() override {
        sync();
        if (d_fd != -1) {
            close(d_fd);
            d_fd = -1;
        }
        if (d_buffer) {
            free(d_buffer);
            d_buffer = nullptr;
        }
    }

protected:
    int_type overflow(int_type ch = traits_type::eof()) override {
        if (d_fd == -1) {
            return traits_type::eof();
        }

        if (!flush_aligned_blocks()) {
            return traits_type::eof();
        }

        if (ch != traits_type::eof()) {
            if (pptr() < epptr()) {
                *pptr() = static_cast<char>(ch);
                pbump(1);
            } else {
                return traits_type::eof();
            }
        }

        return traits_type::not_eof(ch);
    }

    int sync() override {
        if (d_fd == -1) return -1;
        
        if (!flush_aligned_blocks()) return -1;

        size_t remainder_size = pptr() - pbase();
        if (remainder_size > 0) {
            size_t padded_size = ((remainder_size + d_alignment - 1) / d_alignment) * d_alignment;

            memset(pbase() + remainder_size, 0, padded_size - remainder_size);

            ssize_t written = write(d_fd, pbase(), padded_size);
            if (written == -1 || static_cast<size_t>(written) != padded_size) {
                perror("DirectIOStreambuf: Final padded write failed");
                // Continue to attempt truncation
            }

            d_total_bytes_written += remainder_size;
            
            // Clear the buffer since it has been flushed
            setp(pbase(), epptr());

            if (ftruncate(d_fd, d_total_bytes_written) != 0) {
                perror("DirectIOStreambuf: ftruncate failed");
                return -1;
            }
        }
        
        return 0;
    }

private:
    bool flush_aligned_blocks() {
        size_t current_size = pptr() - pbase();
        if (current_size == 0 || d_fd == -1) {
            return true;
        }
        
        size_t write_size = (current_size / d_alignment) * d_alignment;
        if (write_size > 0) {
            ssize_t written = write(d_fd, pbase(), write_size);
            if (written == -1) {
                perror("DirectIOStreambuf: write failed");
                return false;
            }
            if (static_cast<size_t>(written) != write_size) {
                return false; // Short write is an error
            }
            
            d_total_bytes_written += written;
            size_t remaining_size = current_size - written;
            memmove(pbase(), pbase() + written, remaining_size);
            setp(pbase(), epptr());
            pbump(static_cast<int>(remaining_size));
        }
        return true;
    }

    std::string d_filename;
    int         d_fd;
    size_t      d_total_bytes_written;

    const size_t      d_alignment = 4096;
    size_t            d_buffer_size;
    char*             d_buffer;
};

/**
 * A simple Aws::IOStream wrapper around the DirectIOStreambuf.
 */
class DirectIOStream : public Aws::IOStream {
public:
    explicit DirectIOStream(const std::string& filename)
        : Aws::IOStream(&streambuf), streambuf(filename) {}
private:
    DirectIOStreambuf streambuf;
};



// Custom stream buffer that discards all data (like /dev/null)
class NullStreamBuf : public std::streambuf {
  public:
  NullStreamBuf() = default;
  
  protected:
  // Override overflow to discard all data (output)
  int_type overflow(int_type c) override {
    // Just return the character to indicate success
    return c;
  }
  
  // Override xsputn for bulk writes - discard all data (output)
  std::streamsize xsputn(const char* s, std::streamsize n) override {
    // Pretend we wrote all the data by returning n
    return n;
  }
  
  // Override underflow for input operations - always EOF
  int_type underflow() override {
    return traits_type::eof();
  }
  
  // Override xsgetn for bulk reads - always return 0 (no data available)
  std::streamsize xsgetn(char* s, std::streamsize n) override {
    return 0;
  }
  
  // Override seekoff for seeking operations
  pos_type seekoff(off_type off, std::ios_base::seekdir way, std::ios_base::openmode which) override {
    // Always return success for seeking
    return pos_type(off);
  }
  
  // Override seekpos for absolute positioning
  pos_type seekpos(pos_type pos, std::ios_base::openmode which) override {
    // Always return the requested position
    return pos;
  }
};

// Custom stream that discards all data (bidirectional iostream)
class NullStream : public std::iostream {
  private:
  NullStreamBuf null_buf_;
  
  public:
  NullStream() : std::iostream(&null_buf_) {}
  
  // Make it movable
  NullStream(NullStream&& other) noexcept : std::iostream(std::move(other)), null_buf_(std::move(other.null_buf_)) {
    set_rdbuf(&null_buf_);
  }
  
  NullStream& operator=(NullStream&& other) noexcept {
    if (this != &other) {
      std::iostream::operator=(std::move(other));
      null_buf_ = std::move(other.null_buf_);
      set_rdbuf(&null_buf_);
    }
    return *this;
  }
  
  // Delete copy operations
  NullStream(const NullStream&) = delete;
  NullStream& operator=(const NullStream&) = delete;
};


inline std::string FormatRange(int64_t start, int64_t length) {
  // Format a HTTP range header value
  std::stringstream ss;
  ss << "bytes=" << start << "-" << start + length - 1;
  return ss.str();
}

inline Aws::String ToAwsString(const std::string& s) {
  // Direct construction of Aws::String from std::string doesn't work because
  // it uses a specific Allocator class.
  return Aws::String(s.begin(), s.end());
}
