#pragma once

#include "Workspace.hpp"
#include "Fixture.hpp"
#include "Snapshot.hpp"
#include <unordered_map>
#include <functional>
#include <dbzero/core/memory/swine_ptr.hpp>
#include <optional>

namespace db0

{

    // A WorkspaceView exposes a limited read-only Workspace interface bound to a specific state number
    class WorkspaceView: public Snapshot
    {
    public:
        /**
         * @param state_num state number to be applied to the default fixture
         */
        WorkspaceView(std::shared_ptr<Workspace>, std::optional<std::uint64_t> state_num = {},
            const std::unordered_map<std::string, std::uint64_t> &prefix_state_nums = {});
        WorkspaceView(Workspace &, std::optional<std::uint64_t> state_num = {},
            const std::unordered_map<std::string, std::uint64_t> &prefix_state_nums = {});
        
        bool hasFixture(const std::string &prefix_name) const override;

        db0::swine_ptr<Fixture> getFixture(const std::string &prefix_name, std::optional<AccessType> = {}) override;
        
        db0::swine_ptr<Fixture> getFixture(std::uint64_t uuid, std::optional<AccessType> = {}) override;

        db0::swine_ptr<Fixture> getCurrentFixture() override;
        
        bool close(const std::string &prefix_name) override;
        
        void close() override;
                
        /**
         * @param state_num if not provided then current state is used
         */        
        static WorkspaceView *makeNew(void *at_ptr, std::shared_ptr<Workspace>, std::optional<std::uint64_t> state_num, 
            const std::unordered_map<std::string, std::uint64_t> &prefix_state_nums);
        
    private:
        bool m_closed = false;
        std::shared_ptr<Workspace> m_workspace;
        Workspace *m_workspace_ptr;        
        const std::optional<std::uint64_t> m_default_uuid;
        // user requested state numbers by prefix name
        std::unordered_map<std::string, std::uint64_t> m_prefix_state_nums;
        // fixture snapshots by UUID
        std::unordered_map<std::uint64_t, db0::swine_ptr<Fixture> > m_fixtures;
        // name to UUID mapping
        std::unordered_map<std::string, std::uint64_t> m_name_uuids;
        // state number by fixture UUID
        mutable std::unordered_map<std::uint64_t, std::uint64_t> m_state_nums;

        WorkspaceView(std::shared_ptr<Workspace>, Workspace *workspace_ptr, std::optional<std::uint64_t> state_num = {},
            const std::unordered_map<std::string, std::uint64_t> &prefix_state_nums = {});

        std::optional<std::uint64_t> getSnapshotStateNum(const Fixture &) const;
    };
    
}