// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <string>
#include <optional>
#include <dbzero/core/memory/swine_ptr.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <mutex>
#include <memory>

namespace db0

{

    class Fixture;
    class LangCache;
    class PrefixName;
    class ProcessTimer;
    
    /**
     * Snapshot is a common interface for Workspace and WorkspaceView
    */
    class Snapshot: public std::enable_shared_from_this<Snapshot>
    {
    public:
        virtual ~Snapshot()= default;
        
        // Check if a prefix with the given name exists
        virtual bool hasFixture(const PrefixName &) const = 0;

        virtual db0::swine_ptr<Fixture> tryGetFixture(const PrefixName &, std::optional<AccessType> = {}) = 0;

        virtual db0::swine_ptr<Fixture> tryGetFixture(std::uint64_t uuid, std::optional<AccessType> = {}) = 0;
        
        virtual db0::swine_ptr<Fixture> getCurrentFixture() = 0;
        
        /**
         * Find existing (opened) fixture or throw
        */
        virtual db0::swine_ptr<Fixture> tryFindFixture(const PrefixName &) const = 0;
        
        virtual bool close(const PrefixName &) = 0;
        
        /**
         * Close all prefixes, commit all data from read/write prefixes
         * @param as_defunct if true (i.e. Python interpreter is already shut down) then close without releasing any
         * language specific resources
        */
        virtual void close(bool as_defunct = false, ProcessTimer * = nullptr) = 0;
        
        virtual std::shared_ptr<LangCache> getLangCache() const = 0;
        
        virtual bool isMutable() const = 0;
        
        db0::swine_ptr<Fixture> findFixture(const PrefixName &) const;
        
        db0::swine_ptr<Fixture> getFixture(
            const PrefixName &, std::optional<AccessType> = AccessType::READ_WRITE);
        
        db0::swine_ptr<Fixture> getFixture(std::uint64_t uuid, std::optional<AccessType> = {});
        
        // Get the corresponding "head" snapshot / workspace
        // the default implementation returns itself
        virtual Snapshot &getHeadWorkspace() const;        
        
        // The implementation returns snapshot-level access type where it has been defined (e.g. read-only snapshots)
        // by default, the std::nullopt is returned
        virtual std::optional<AccessType> tryGetAccessType() const;
        
        // @return the number of currently open prefixes in the snapshot / workspace
        virtual std::size_t size() const = 0;
    };
    
    bool checkAccessType(const Fixture &fixture, AccessType);
    bool checkAccessType(const Fixture &fixture, std::optional<AccessType> requested);
    // throws if the requested access type is not allowed
    void assureAccessType(const Fixture &fixture, std::optional<AccessType> requested);
    
}