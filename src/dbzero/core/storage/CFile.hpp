// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <string>
#include <vector>
#include <atomic>
#include <memory>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <dbzero/core/utils/InterProcessLock.hpp>
#include <dbzero/workspace/LockFlags.hpp>

namespace db0

{

    // CFile is a wrapper around std::FILE
    class CFile
    {
    public:
        /**
         * Open existing binary file for read/write
         */
        CFile(const std::string &file_name, AccessType access_type);
        CFile(const std::string &file_name, AccessType access_type, LockFlags lock_flags);
        ~CFile();

        /**
         * Create a new file
         * throws if file already exists
        */
        static void create(const std::string &file_name, const std::vector<char> &data, bool create_directories = true);
        
        /**
         * Check if specific file exists
        */
        static bool exists(const std::string &file_name);
        // Remove existing file
        static void remove(const std::string &file_name);
        
        void write(std::uint64_t address, std::size_t size, const void *buffer);
        
        void read(std::uint64_t address, std::size_t size, void *buffer) const;

        std::string getName() const {
            return m_path;
        }
        
        /**
         * Refresh file size (if in read-only mode)
         * the operation has no effect in read-write mode         
         * @return true if file size has changed
        */
        bool refresh();

        void flush() const;
        
        // Make changes durable and accessible to other processes as well
        void fsync() const;

        void close();
        
        inline std::uint64_t size() const {
            return m_file_size;
        }
        
        bool operator()() const {
            return m_file != nullptr;
        }

        AccessType getAccessType() const {
            return m_access_type;
        }

        /**
         * Get last modification timestamp
        */
        std::uint64_t getLastModifiedTime() const;

        // Get the number of random read / write operations
        // NOTE: that sequential reads / writes are not counted
        std::pair<std::uint64_t, std::uint64_t> getRandOps() const;
        
        // Get the total number of bytes read / written
        std::pair<std::uint64_t, std::uint64_t> getIOBytes() const;
        
#ifndef NDEBUG
        // Protect a specific file range from being modified (debugging only feature)
        void setProtectedRange(std::uint64_t begin, std::size_t size);
#endif

    private:
        const std::string m_path;
        const AccessType m_access_type;
        FILE *m_file = nullptr;
        mutable std::uint64_t m_file_pos = 0;
        mutable std::uint64_t m_file_size = 0;
        mutable std::uint64_t m_rand_read_ops = 0;
        mutable std::uint64_t m_rand_write_ops = 0;
        // total bytes read / written
        mutable std::uint64_t m_bytes_read = 0;
        mutable std::uint64_t m_bytes_written = 0;
        std::unique_ptr<InterProcessLock> m_lock;        
        mutable std::mutex m_mutex;
        mutable bool m_dirty = false;
#ifndef NDEBUG
        std::pair<std::uint64_t, std::size_t> m_protected = { 0, 0 };        
#endif        
        
        void flush(std::unique_lock<std::mutex> &) const;
        void setFilePos(std::uint64_t address, std::unique_lock<std::mutex> &) const;
    };
    
    std::uint64_t getLastModifiedTime(const char *file_name);

}
