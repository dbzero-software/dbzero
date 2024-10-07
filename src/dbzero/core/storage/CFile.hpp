#pragma once

#include <string>
#include <vector>
#include <atomic>
#include <dbzero/core/memory/AccessOptions.hpp>

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
        ~CFile();

        /**
         * Create a new file
         * throws if file already exists
        */
        static void create(const std::string &file_name, const std::vector<char> &data, 
            bool create_directories = true);
        
        /**
         * Check if specific file exists
        */
        static bool exists(const std::string &file_name);

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

        /**
         * @param update_last_modified if true, the last modified timestamp is updated
        */
        void flush();

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
    };

    std::uint64_t getLastModifiedTime(const char *file_name);

}
