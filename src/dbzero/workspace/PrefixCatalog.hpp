// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <filesystem>
#include <unordered_map>
#include <string>
#include <optional>
#include <unordered_set>
#include <unordered_map>
#include <functional>

namespace db0

{

    namespace fs = std::filesystem;
    class Fixture;
    class PrefixName;
        
    class PrefixCatalog
    {
    public:
        PrefixCatalog(const std::string &root_path);

        /**
         * Construct fully qualified file name from prefix name (for prefix creation / open by name)
        */
        fs::path getFileName(const std::string &prefix_name) const;
        
        /**
         * Drop specific prefix file
        */
        bool drop(const std::string &prefix_name, bool if_exists = true);
        
        bool exists(const std::string &prefix_name) const;

        /**
         * Refresh contents to locate newly created prefixes
        */
        void refresh() const;

        // callback only notified when new prefix is detected
        void refresh(std::function<void(const std::string &)> callback) const;
        
        void forAll(std::function<void(const std::string &prefix_name)> callback) const;

    protected:
        fs::path m_root_path;
        // cached canonical prefix names
        mutable std::unordered_set<std::string> m_prefix_names;

        void refresh(const std::string &path, std::function<void(const std::string &)>) const;
    };
    
    class FixtureCatalog
    {
    public:
        FixtureCatalog(PrefixCatalog &);

        /**
         * Refresh contents to locate newly created fixtures
        */
        void refresh() const;

        bool drop(const PrefixName &, bool if_exists = true);
        
        /**
         * Add fixture to catalog
        */
        void add(const PrefixName &, const Fixture &);
        
        /**
         * Try identifying prefix name by fixture UUID
        */
        std::optional<std::string> getPrefixName(std::uint64_t fixture_UUID) const;
        
        std::optional<std::uint64_t> getFixtureUUID(const PrefixName &) const;
        
        // Get all known prefix names and their UUIDs
        std::unordered_map<std::string, std::uint64_t> getData() const;
        
        // Locate the prefix associated existing file name
        fs::path getPrefixFileName(const PrefixName &prefix_name) const;

    private:
        PrefixCatalog &m_prefix_catalog;
        // name to UUID mapping
        mutable std::unordered_map<std::string, std::uint64_t> m_name_uuids;
        // UUID to name mapping
        mutable std::unordered_map<std::uint64_t, std::string> m_uuid_names;

        void tryAdd(const std::string &maybe_prefix_name) const;
    };
    
}