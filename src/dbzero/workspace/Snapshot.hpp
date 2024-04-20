#pragma once

#include <string>
#include <optional>
#include <dbzero/core/memory/swine_ptr.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>

namespace db0

{

    class Fixture;

    /**
     * Snapshot is a common interface for Workspace and WorkspaceView
    */
    class Snapshot
    {
    public:
        virtual ~Snapshot()= default;
        
        virtual db0::swine_ptr<Fixture> getFixture(
            const std::string &prefix_name, std::optional<AccessType> = AccessType::READ_WRITE) = 0;
        
        virtual db0::swine_ptr<Fixture> getFixture(std::uint64_t uuid, std::optional<AccessType> = {}) = 0;
        
        virtual db0::swine_ptr<Fixture> getCurrentFixture(std::optional<AccessType> = AccessType::READ_ONLY) = 0;
        
        virtual bool close(const std::string &prefix_name) = 0;
        
        virtual void close() = 0;
    };

}