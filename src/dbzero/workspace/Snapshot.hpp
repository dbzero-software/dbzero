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

    /**
     * Snapshot is a common interface for Workspace and WorkspaceView
    */
    class Snapshot
    {
    public:
        virtual ~Snapshot()= default;
        
        // Check if a prefix with the given name exists
        virtual bool hasFixture(const PrefixName &prefix_name) const = 0;

        virtual db0::swine_ptr<Fixture> getFixture(
            const PrefixName &prefix_name, std::optional<AccessType> = AccessType::READ_WRITE) = 0;
        
        virtual db0::swine_ptr<Fixture> getFixture(std::uint64_t uuid, std::optional<AccessType> = {}) = 0;
        
        virtual db0::swine_ptr<Fixture> getCurrentFixture() = 0;
        
        /**
         * Find existing (opened) fixture or throw
        */
        virtual db0::swine_ptr<Fixture> tryFindFixture(const PrefixName &) const = 0;

        virtual bool close(const PrefixName &prefix_name) = 0;
        
        virtual void close() = 0;

        virtual std::shared_ptr<LangCache> getLangCache() const = 0;
        
        virtual bool isMutable() const = 0;
        
        db0::swine_ptr<Fixture> findFixture(const PrefixName &) const;
    };
    
}