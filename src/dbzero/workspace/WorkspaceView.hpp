#pragma once

#include "Workspace.hpp"
#include "Fixture.hpp"
#include <unordered_map>
#include <functional>
#include <dbzero/core/memory/swine_ptr.hpp>

namespace db0

{

    // A WorkspaceView exposes a limited read-only Workspace interface bound to a specific state number
    class WorkspaceView
    {
    public:
        WorkspaceView(std::shared_ptr<Workspace>);
        
        db0::swine_ptr<Fixture> getFixture(const std::string &prefix_name);
        
        db0::swine_ptr<Fixture> getFixture(std::uint64_t uuid);
        
        void close(const std::string &prefix_name);
        
        void close();
        
        static WorkspaceView *makeNew(void *at_ptr, std::shared_ptr<Workspace>);
        
    private:
        bool m_closed = false;
        std::shared_ptr<Workspace> m_workspace;
        // fixture snapshots by UUID
        std::unordered_map<std::uint64_t, db0::swine_ptr<Fixture> > m_fixtures;
        // name to UUID mapping
        std::unordered_map<std::string, std::uint64_t> m_name_uuids;
    };
    
}