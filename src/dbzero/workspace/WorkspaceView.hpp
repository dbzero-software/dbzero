#pragma once

#include "Workspace.hpp"
#include "Fixture.hpp"
#include "Snapshot.hpp"
#include <unordered_map>
#include <functional>
#include <dbzero/core/memory/swine_ptr.hpp>

namespace db0

{

    // A WorkspaceView exposes a limited read-only Workspace interface bound to a specific state number
    class WorkspaceView: public Snapshot
    {
    public:
        WorkspaceView(std::shared_ptr<Workspace>);
        
        db0::swine_ptr<Fixture> getFixture(const std::string &prefix_name, std::optional<AccessType> = {}) override;
        
        db0::swine_ptr<Fixture> getFixture(std::uint64_t uuid, std::optional<AccessType> = {}) override;

        db0::swine_ptr<Fixture> getCurrentFixture(std::optional<AccessType> = {}) override;
        
        bool close(const std::string &prefix_name) override;
        
        void close() override;
        
        static WorkspaceView *makeNew(void *at_ptr, std::shared_ptr<Workspace>);
        
    private:
        bool m_closed = false;
        std::shared_ptr<Workspace> m_workspace;
        const std::uint64_t m_default_uuid;
        // fixture snapshots by UUID
        std::unordered_map<std::uint64_t, db0::swine_ptr<Fixture> > m_fixtures;
        // name to UUID mapping
        std::unordered_map<std::string, std::uint64_t> m_name_uuids;
    };
    
}