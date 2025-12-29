// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

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

    class LangCache;
    class PrefixName;
    
    // A WorkspaceView exposes a limited read-only Workspace interface bound to a specific state number
    class WorkspaceView: public Snapshot
    {
    public:
        virtual ~WorkspaceView();

        bool hasFixture(const PrefixName &) const override;

        db0::swine_ptr<Fixture> tryGetFixture(const PrefixName &, std::optional<AccessType> = {}) override;
        
        db0::swine_ptr<Fixture> tryGetFixture(std::uint64_t uuid, std::optional<AccessType> = {}) override;

        db0::swine_ptr<Fixture> getCurrentFixture() override;

        db0::swine_ptr<Fixture> tryFindFixture(const PrefixName &) const override;
        
        bool close(const PrefixName &) override;
        
        void close(bool as_defunct, ProcessTimer * = nullptr) override;

        std::shared_ptr<LangCache> getLangCache() const override;
        
        bool isMutable() const override;

        Snapshot &getHeadWorkspace() const override;
        
        std::optional<AccessType> tryGetAccessType() const override;

        std::size_t size() const override;
                
    protected:
        friend class Workspace;

        /**
         * @param state_num state number to be applied to the default fixture
         */
        WorkspaceView(std::shared_ptr<Workspace>, std::optional<std::uint64_t> state_num = {},
            const std::unordered_map<std::string, std::uint64_t> &prefix_state_nums = {});
        WorkspaceView(Workspace &, std::optional<std::uint64_t> state_num = {},
            const std::unordered_map<std::string, std::uint64_t> &prefix_state_nums = {});
        
    private:
        bool m_closed = false;
        // user requested state numbers by prefix name
        std::unordered_map<std::string, std::uint64_t> m_prefix_state_nums;
        // fixture snapshots by UUID
        std::unordered_map<std::uint64_t, db0::swine_ptr<Fixture> > m_fixtures;
        // name to UUID mapping
        std::unordered_map<std::string, std::uint64_t> m_name_uuids;
        // state number by fixture UUID
        mutable std::unordered_map<std::uint64_t, std::uint64_t> m_state_nums;
        // a WorkspaceView maintains a private LangCache instance
        std::shared_ptr<LangCache> m_lang_cache;
        std::shared_ptr<Workspace> m_workspace;
        Workspace *m_workspace_ptr;
        const std::optional<std::uint64_t> m_default_uuid;
        // all fixtures (UUID) included in the snapshot
        std::unordered_set<std::uint64_t> m_snapshot_fixtures;
        
        WorkspaceView(std::shared_ptr<Workspace>, Workspace *workspace_ptr, std::optional<std::uint64_t> state_num = {},
            const std::unordered_map<std::string, std::uint64_t> &prefix_state_nums = {});

        std::optional<std::uint64_t> getSnapshotStateNum(const Fixture &) const;
        
        std::optional<std::uint64_t> tryGetFixtureUUID(const PrefixName &) const;
    };
    
}