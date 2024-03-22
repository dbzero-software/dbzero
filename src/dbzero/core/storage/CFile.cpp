#include "CFile.hpp"
#include <dbzero/core/exception/Exceptions.hpp>
#include <cassert>
#include <sys/stat.h>
#include <chrono>
#include <filesystem>

namespace db0

{   

    namespace fs = std::filesystem;
    
    std::uint64_t getFileSize(FILE *file, std::uint64_t file_pos)
    {
        if (fseek(file, 0L, SEEK_END)) {
            THROWF(db0::IOException) << "CFile::getFileSize: fseek failed";
        }
        auto result = ftell(file);
        // return to original position
        if (fseek(file, file_pos, SEEK_SET)) {
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
        struct stat st;
        if (stat(file_name, &st)) {
            THROWF(db0::IOException) << "CFile::getLastModifiedTime: stat failed";
        }
        return st.st_mtim.tv_sec * 1000000000 + st.st_mtim.tv_nsec;
    }

    CFile::CFile(const std::string &file_name, AccessType access_type)
        : m_path(file_name)
        , m_access_type(access_type)
        , m_file(openFile(m_path.c_str(), access_type))
        , m_file_size(getFileSize(m_file, m_file_pos))
    {
    }

    CFile::~CFile() 
    {
        if (m_file) {
            fclose(m_file);
        }        
    }
    
    void CFile::flush()
    {
        if (fflush(m_file)) {
            THROWF(db0::IOException) << "CFile::flush: failed to flush file " << m_path;
        }
    }
    
    void CFile::close()
    {
        if (m_file) {
            if (fclose(m_file)) {
                THROWF(db0::IOException) << "CFile::close: failed to close file " << m_path;
            }
            m_file = nullptr;
        }
    }
    
    bool CFile::refresh()
    {
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
            fseek(file, 0, SEEK_END);
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

    void CFile::write(std::uint64_t address, std::size_t size, const void *buffer)
    {
        assert(m_access_type != AccessType::READ_ONLY);
        if (address != m_file_pos) {
            if (fseek(m_file, address, SEEK_SET)) {
                THROWF(db0::IOException) << "CFile::write: fseek failed";
            }
            m_file_pos = address;
        }
        if (fwrite(buffer, size, 1, m_file) != 1) {
            THROWF(db0::IOException) << "CFile::write: fwrite failed";
        }
        m_file_pos += size;
        m_file_size = std::max(m_file_size, m_file_pos);
    }
    
    void CFile::read(std::uint64_t address, std::size_t size, void *buffer) const 
    {
        if (address != m_file_pos) {
            if (fseek(m_file, address, SEEK_SET)) {
                THROWF(db0::IOException) << "CFile::read: fseek failed";
            }
            m_file_pos = address;
        }
        if (fread(buffer, size, 1, m_file) != 1) {
            THROWF(db0::IOException) << "CFile::read: fread failed";
        }
        m_file_pos += size;
    }

    std::uint64_t CFile::getLastModifiedTime() const
    {
        return db0::getLastModifiedTime(m_path.c_str());
    }
    
    bool CFile::exists(const std::string &file_name)
    {
        struct stat st;
        return stat(file_name.c_str(), &st) == 0;
    }
    
}