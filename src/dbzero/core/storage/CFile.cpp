// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "CFile.hpp"
#include <cassert>
#include <sys/stat.h>
#include <chrono>
#include <filesystem>
#include <algorithm>
#ifdef _WIN32
#  include <io.h>
#  include <direct.h>
#else
#  include <unistd.h>
#endif


#ifdef _WIN32
#define FSEEK _fseeki64
#else
#define FSEEK fseek
#endif

#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{   

    namespace fs = std::filesystem;
    
    std::uint64_t getFileSize(FILE *file, std::uint64_t file_pos)
    {
        if (FSEEK(file, 0L, SEEK_END)) {
            THROWF(db0::IOException) << "CFile::getFileSize: fseek failed";
        }
        auto result = ftell(file);
        // return to original position
        if (FSEEK(file, file_pos, SEEK_SET)) {
            THROWF(db0::IOException) << "CFile::getFileSize: fseek failed";
        }
        return result;
    }

    FILE *openFile(const char *file_name, AccessType access_type)
    {
        auto file = fopen(file_name, (access_type == AccessType::READ_ONLY)?"rb":"r+b");
        if (!file) {
            THROWF(db0::IOException) << "Unable to open file: " << file_name;
        }

        return file;
    }
    
    std::uint64_t getLastModifiedTime(const char *file_name)
    {
        #ifdef _WIN32
            auto tp = fs::last_write_time(fs::path(file_name));
            auto duration = tp.time_since_epoch();
            return std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
        #elif defined(__APPLE__)
            auto tp = fs::last_write_time(fs::path(file_name));
            auto duration = tp.time_since_epoch();
            return std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
        #else
            struct stat st;
            if (stat(file_name, &st)) {
                THROWF(db0::IOException) << "CFile::getLastModifiedTime: stat failed";
            };
            return st.st_mtim.tv_sec * 1000000000 + st.st_mtim.tv_nsec;
        #endif
    }

    CFile::CFile(const std::string &file_name, AccessType access_type)
        : CFile(file_name, access_type, LockFlags(true))
    {
    }
    
    CFile::CFile(const std::string &file_name, AccessType access_type, LockFlags lock_flags)
        : m_path(file_name)
        , m_access_type(access_type)
        , m_file(openFile(m_path.c_str(), access_type))
        , m_file_size(getFileSize(m_file, m_file_pos))
    {
        if (access_type == AccessType::READ_WRITE && lock_flags.m_no_lock == false) {
            std::string lock_path = m_path + ".lock";
            m_lock = std::make_unique<InterProcessLock>(lock_path.c_str(), lock_flags);
        }
    }
    
    CFile::~CFile()
    {
        if (m_file) {
            if (m_dirty) {                
                flush();
            }         
            fclose(m_file);
        }
        assert(!m_dirty);
    }
    
    void CFile::flush(std::unique_lock<std::mutex> &) const
    {
        if (fflush(m_file)) {
            THROWF(db0::IOException) << "CFile::flush: failed to flush file " << m_path;
        }
        m_dirty = false;
    }

    void CFile::fsync() const
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        flush(lock);
        if (m_access_type == AccessType::READ_ONLY) {
            THROWF(db0::IOException) << "Commit failed! errno=" << errno
                  << " (" << strerror(errno) << ")\n";
        }
#ifdef _WIN32
        if (_commit(fileno(m_file)) == -1) {
            THROWF(db0::IOException) << "CFile::fsync: failed to sync file " << m_path;
        }
#else
        if (::fsync(fileno(m_file)) == -1) {
            THROWF(db0::IOException) << "CFile::fsync: failed to sync file " << m_path;
        }
#endif
    }

    void CFile::flush() const
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        flush(lock);
    }
    
    void CFile::close()
    {
        if (m_file) {
            if (fclose(m_file)) {
                THROWF(db0::IOException) << "CFile::close: failed to close file " << m_path;
            }
            m_file = nullptr;
        }
        //release the lock
        m_lock.reset();
    }
    
    bool CFile::refresh()
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        if (m_access_type == AccessType::READ_ONLY && m_file) {
            auto file_size = getFileSize(m_file, m_file_pos);
            if (file_size != m_file_size) {
                m_file_size = file_size;
                return true;
            }
        }
        return false;
    }
    
    void CFile::create(const std::string &file_name, const std::vector<char> &data, bool create_directories)
    {
        if (create_directories) {
            auto parent_path = fs::path(file_name).parent_path();
            if (!parent_path.empty()) {
                fs::create_directories(parent_path);
            }
        }
        
        // create a new empty file
        FILE *file = fopen(file_name.c_str(), "ab+");
        try {
            // check file size
            FSEEK(file, 0, SEEK_END);
            auto file_size = ftell(file);
            if (file_size != 0) {
                THROWF(db0::IOException) << "File already exists: " << file_name;
            }
            if (data.size() > 0) {
                if (fwrite(data.data(), data.size(), 1, file) != 1) {
                    THROWF(db0::IOException) << "File write failed: " << file_name;
                }
            }
            fclose(file);
        } catch (...) {
            fclose(file);
            throw;
        }
    }
    
    void CFile::setFilePos(std::uint64_t address, std::unique_lock<std::mutex> &lock) const
    {
        if (address != m_file_pos) {
            if (m_dirty) {
                flush(lock);
            }
            auto err_code = FSEEK(m_file, address, SEEK_SET);
            if (err_code != 0) {
                int err = errno;
                THROWF(db0::IOException) << "CFile::write: fseek failed with error code " << strerror(err);
            }
            m_file_pos = address;
        }
    }
    
    void CFile::write(std::uint64_t address, std::size_t size, const void *buffer)
    {        
        std::unique_lock<std::mutex> lock(m_mutex);
        assert(m_access_type != AccessType::READ_ONLY);
        if (address != m_file_pos) {
            setFilePos(address, lock);
            ++m_rand_write_ops;
        }
        assert(m_file_pos == (std::uint64_t)ftell(m_file));
        assert(!overlap(m_protected, { address, size }));
        if (fwrite(buffer, size, 1, m_file) != 1) {
            THROWF(db0::IOException) << "CFile::write: fwrite failed";
        }
        m_file_pos += size;
        m_file_size = std::max(m_file_size, m_file_pos);
        m_bytes_written += size;
        if (!m_dirty) {
            m_dirty = true;
        }
    }
    
    void CFile::read(std::uint64_t address, std::size_t size, void *buffer) const
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        // need to flush data from buffer before reading
        if (m_dirty) {
            flush(lock);
        }
        if (address != m_file_pos) {
            setFilePos(address, lock);
            ++m_rand_read_ops;
        }
        assert(m_file_pos == (std::uint64_t)ftell(m_file));
        if (fread(buffer, size, 1, m_file) != 1) {
            THROWF(db0::IOException) << "CFile::read: fread failed";
        }
        m_file_pos += size;
        m_bytes_read += size;
    }
    
    std::uint64_t CFile::getLastModifiedTime() const {
        return db0::getLastModifiedTime(m_path.c_str());
    }
    
    bool CFile::exists(const std::string &file_name)
    {
        struct stat st;
        return stat(file_name.c_str(), &st) == 0;
    }

    void CFile::remove(const std::string &file_name)
    {
        if (std::remove(file_name.c_str()) != 0) {
            THROWF(db0::IOException) << "CFile::remove: unable to remove file " << file_name;
        }
    }

    std::pair<std::uint64_t, std::uint64_t> CFile::getRandOps() const {
        return { m_rand_read_ops, m_rand_write_ops };
    }

    std::pair<std::uint64_t, std::uint64_t> CFile::getIOBytes() const {
        return { m_bytes_read, m_bytes_written };
    }
    
#ifndef NDEBUG    
    void CFile::setProtectedRange(std::uint64_t begin, std::size_t size) 
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_protected = { begin, size };
    }
#endif

}