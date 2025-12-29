// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PrefixCatalog.hpp"
#include <algorithm>
#include "Fixture.hpp"
#include "PrefixName.hpp"
#include <dbzero/core/storage/CFile.hpp>
#include <dbzero/core/storage/BDevStorage.hpp>
#include <dbzero/core/memory/PrefixImpl.hpp>
#include <dbzero/core/memory/MetaAllocator.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>

namespace db0

{

    namespace fs = std::filesystem;
    
    std::string preparePrefixName(const std::string& input) 
    {
        std::size_t pos = input.find_first_not_of("/\\");
        if (pos == std::string::npos) {
            return "";
        }                
        return input.substr(pos);
    }

    bool empty(const std::string &input)
    {
        return !std::any_of(input.begin(), input.end(), [](char c) {
            return !std::isspace(static_cast<unsigned char>(c));
        });
    }

    std::string removeSuffix(const std::string& str, const std::string& suffix)
    {
        if (str.length() >= suffix.length() && str.compare(str.length() - suffix.length(), suffix.length(), suffix) == 0) {
            // The suffix exists, remove it
            return str.substr(0, str.length() - suffix.length());
        }
        // The suffix doesn't exist, return the original string
        return str;
    }   

    PrefixCatalog::PrefixCatalog(const std::string &root_path)
        : m_root_path(empty(root_path) ? fs::current_path() : fs::path(root_path))
    {        
    }

    bool PrefixCatalog::drop(const std::string &prefix_name, bool if_exists)
    {
        auto file_name = getFileName(prefix_name);
        bool file_exists = CFile::exists(file_name.string());
        if (!if_exists && !file_exists) {
            THROWF(db0::PrefixNotFoundException) << "Prefix does not exist: " << prefix_name;
        }        
        if (file_exists) {
            std::remove(file_name.string().c_str());
            return true;
        }
        return false;
    }

    fs::path PrefixCatalog::getFileName(const std::string &prefix_name) const {
        return (m_root_path / (preparePrefixName(prefix_name) + ".db0"));
    }
    
    bool PrefixCatalog::exists(const std::string &prefix_name) const {
        return CFile::exists(getFileName(prefix_name).string());
    }

    void PrefixCatalog::refresh(std::function<void(const std::string &)> callback) const {
        refresh("", callback);
    }
    
    void PrefixCatalog::refresh(
        const std::string &path, std::function<void(const std::string &)> callback) const
    {
        // combine root path with the provided path
        fs::path full_path = m_root_path / path;
        for (auto &entry : fs::directory_iterator(full_path)) {
            // visit sub-directories
            if (entry.is_directory()) {
                // append directory to path
                refresh((fs::path(path) / entry.path().filename()).string(), callback);
            } if (entry.is_regular_file()) {
                auto file_name = removeSuffix(entry.path().filename().string(), ".db0");
                auto full_name = (fs::path(path) / file_name).string();
                #ifdef _WIN32
                // normalize to forward slashes for cross-platform compatibility
                std::replace(full_name.begin(), full_name.end(), '\\', '/');
                #endif
                if (m_prefix_names.find(full_name) == m_prefix_names.end()) {
                    if (callback) {
                        callback(full_name);
                    }
                    // full prefix name
                    m_prefix_names.insert(full_name);
                }
            }
        }
    }

    void PrefixCatalog::refresh() const
    {        
        refresh([&](const std::string &) {
            // do nothing
        });
    }

    void PrefixCatalog::forAll(std::function<void(const std::string &prefix_name)> callback) const
    {
        for (auto &prefix_name: m_prefix_names) {
            callback(prefix_name);
        }
    }

    FixtureCatalog::FixtureCatalog(PrefixCatalog &prefix_catalog)
        : m_prefix_catalog(prefix_catalog)
    {
        m_prefix_catalog.forAll([this](const std::string &prefix_name) {
            tryAdd(prefix_name);
        });
    } 

    bool FixtureCatalog::drop(const PrefixName &prefix_name, bool if_exists)
    {
        auto uuid = getFixtureUUID(prefix_name);                
        if (m_name_uuids.find(prefix_name) != m_name_uuids.end()) {
            m_name_uuids.erase(prefix_name);
        }
        if (uuid.has_value() && m_uuid_names.find(uuid.value()) != m_uuid_names.end()) {
            m_uuid_names.erase(uuid.value());
        }
        return m_prefix_catalog.drop(prefix_name, if_exists);
    }

    std::optional<std::uint64_t> FixtureCatalog::getFixtureUUID(const PrefixName &prefix_name) const
    {
        auto it = m_name_uuids.find(prefix_name);
        if (it != m_name_uuids.end()) {
            return it->second;
        }
        return std::nullopt;
    }
    
    void FixtureCatalog::add(const PrefixName &prefix_name, const Fixture &fixture)
    {
        m_name_uuids[prefix_name] = fixture.getUUID();
        m_uuid_names[fixture.getUUID()] = prefix_name.get();
    }
    
    void FixtureCatalog::tryAdd(const std::string &maybe_prefix_name) const
    {
        if (m_name_uuids.find(maybe_prefix_name) != m_name_uuids.end()) {
            return;
        }
        std::string file_name = m_prefix_catalog.getFileName(maybe_prefix_name).string();
        if (!CFile::exists(file_name)) {
            return;
        }

        try {
            // try opening as fixture file (for validation)
            std::atomic<std::size_t> null_meter;            
            auto prefix = std::make_shared<PrefixImpl>(
                maybe_prefix_name, null_meter, nullptr, std::make_shared<BDevStorage>(file_name, AccessType::READ_ONLY)
            );
            // state_num < 1 suggest invalid / corrupted prefix file
            if (!prefix->getStateNum()) {
                return;
            }
            auto allocator = std::make_shared<MetaAllocator>(prefix);
            auto uuid = Fixture::getUUID(prefix, *allocator);
            m_name_uuids[maybe_prefix_name] = uuid;
            m_uuid_names[uuid] = maybe_prefix_name;
        } catch (const std::exception &) {
            // likely not a fixture file or a file is corrupted
        }
    }
    
    std::optional<std::string> FixtureCatalog::getPrefixName(std::uint64_t fixture_UUID) const
    {
        auto it = m_uuid_names.find(fixture_UUID);
        if (it != m_uuid_names.end()) {
            return it->second;
        }
        return std::nullopt;
    }

    void FixtureCatalog::refresh() const
    {
        m_prefix_catalog.refresh([this](const std::string &prefix_name) {
            tryAdd(prefix_name);
        });
    }
    
    std::unordered_map<std::string, std::uint64_t> FixtureCatalog::getData() const {
        return m_name_uuids;
    }
    
    fs::path FixtureCatalog::getPrefixFileName(const PrefixName &prefix_name) const 
    {
        if (!m_prefix_catalog.exists(prefix_name)) {
            THROWF(db0::PrefixNotFoundException) << "Prefix does not exist: " << prefix_name;
        }
        return m_prefix_catalog.getFileName(prefix_name);
    }

}