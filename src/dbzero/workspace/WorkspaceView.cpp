// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "WorkspaceView.hpp"
#include "PrefixName.hpp"
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/object_model/LangCache.hpp>

namespace db0

{
    
    /**
     * Fixture view initializer
    */
    void initSnapshot(db0::swine_ptr<Fixture> &head, db0::swine_ptr<Fixture> &view) 
    {
        auto &class_factory = head->get<db0::object_model::ClassFactory>();
        // copy all known type mappings from the head fixture
        view->get<db0::object_model::ClassFactory>().initWith(class_factory);
    }
    
    WorkspaceView::WorkspaceView(std::shared_ptr<Workspace> workspace, std::optional<std::uint64_t> state_num,
        const std::unordered_map<std::string, std::uint64_t> &prefix_state_nums)
        : WorkspaceView(workspace, workspace.get(), state_num, prefix_state_nums)
    {
    }

    WorkspaceView::WorkspaceView(Workspace &workspace, std::optional<std::uint64_t> state_num, 
        const std::unordered_map<std::string, std::uint64_t> &prefix_state_nums)
        : WorkspaceView(nullptr, &workspace, state_num, prefix_state_nums)
    {
    }
    
    WorkspaceView::WorkspaceView(std::shared_ptr<Workspace> workspace, Workspace *workspace_ptr, std::optional<std::uint64_t> state_num,
        const std::unordered_map<std::string, std::uint64_t> &prefix_state_nums)
        : m_prefix_state_nums(prefix_state_nums)
        , m_lang_cache(std::make_shared<LangCache>())
        , m_workspace(workspace)
        , m_workspace_ptr(workspace_ptr)        
        , m_default_uuid(workspace_ptr->getDefaultUUID())
    {
        if (!state_num && m_default_uuid) {
            // freeze state number of the default fixture
            auto fixture = m_workspace_ptr->getFixture(*m_default_uuid, AccessType::READ_ONLY);
            auto it = m_prefix_state_nums.find(fixture->getPrefix().getName());
            // state number for the default fixture defined by name
            if (it != m_prefix_state_nums.end()) {
                state_num = it->second;
            } else {
                state_num = fixture->getPrefix().getStateNum();
                // take the last fully consistent state for read/write fixtures
                if (fixture->getAccessType() == AccessType::READ_WRITE) {
                    --(*state_num);
                }
            }            
        }
        
        // check for conflicting requests
        if (m_default_uuid) {
            assert(state_num);
            auto fixture = m_workspace_ptr->getFixture(*m_default_uuid, AccessType::READ_ONLY);
            auto it = m_prefix_state_nums.find(fixture->getPrefix().getName());
            if (it != m_prefix_state_nums.end() && it->second != *state_num) {
                THROWF(db0::InternalException) 
                    << "Conflicting state numbers requested for the default fixture: " 
                    << fixture->getPrefix().getName();
            }
            m_state_nums[*m_default_uuid] = *state_num;
        }
        
        // Freeze state numbers of the remaining open fixtures
        // note that for read/write fixtures only the last fully consistent state number is retained
        m_workspace_ptr->forEachFixture([this](const Fixture &fixture) {
            m_snapshot_fixtures.insert(fixture.getUUID());
            if (!m_default_uuid || *m_default_uuid != fixture.getUUID()) {
                auto it = m_prefix_state_nums.find(fixture.getPrefix().getName());
                if (it != m_prefix_state_nums.end()) {
                    m_state_nums[fixture.getUUID()] = it->second;
                } else {
                    // for snapshots we can only use the last fully consistent state (i.e. finalized = true)
                    m_state_nums[fixture.getUUID()] = fixture.getPrefix().getStateNum(true);
                }
            }
            return true;
        });
    }
    
    WorkspaceView::~WorkspaceView()
    {
    }
        
    db0::swine_ptr<Fixture> WorkspaceView::tryGetFixture(
        const PrefixName &prefix_name, std::optional<AccessType> access_type)
    {
        if (m_closed) {
            THROWF(db0::InternalException) << "WorkspaceView is closed";
        }

        if (access_type && *access_type != AccessType::READ_ONLY) {
            THROWF(db0::InternalException) << "WorkspaceView does not support read/write access";
        }

        auto it = m_name_uuids.find(prefix_name);
        if (it != m_name_uuids.end()) {
            // resolve by UUID
            return getFixture(it->second);
        }
        
        auto head_fixture = m_workspace_ptr->tryGetFixture(prefix_name, AccessType::READ_ONLY);
        if (!head_fixture) {
            return nullptr;
        }
        auto fixture_uuid = head_fixture->getUUID();
        // get snapshot of the latest state
        auto result = head_fixture->getSnapshot(*this, getSnapshotStateNum(*head_fixture));
        // initialize snapshot (use both Workspace and WorkspaceView initializers)
        auto fx_initializer = m_workspace_ptr->getFixtureInitializer();
        // initialize as read-only
        if (fx_initializer) {
            fx_initializer(result, false, true, true);
            initSnapshot(head_fixture, result);
        }
        
        m_fixtures[fixture_uuid] = result;
        m_name_uuids[prefix_name] = fixture_uuid;
        return result;
    }
    
    std::optional<std::uint64_t> WorkspaceView::tryGetFixtureUUID(const PrefixName &prefix_name) const
    {
        auto it = m_name_uuids.find(prefix_name);
        if (it != m_name_uuids.end()) {            
            return it->second;
        }
        
        auto head_fixture = m_workspace_ptr->tryGetFixture(prefix_name, AccessType::READ_ONLY);
        if (!head_fixture) {
            return {};
        }
        return head_fixture->getUUID();
    }
    
    bool WorkspaceView::hasFixture(const PrefixName &prefix_name) const
    {
        auto uuid = tryGetFixtureUUID(prefix_name);
        if (!uuid) {
            return false;
        }
        return m_snapshot_fixtures.find(*uuid) != m_snapshot_fixtures.end();
    }
    
    db0::swine_ptr<Fixture> WorkspaceView::tryGetFixture(std::uint64_t uuid, std::optional<AccessType> access_type)
    {
        if (m_closed) {
            THROWF(db0::InternalException) << "WorkspaceView is closed";
        }
        
        if (access_type && *access_type != AccessType::READ_ONLY) {
            THROWF(db0::InternalException) << "WorkspaceView does not support read/write access";
        }
        
        if (!uuid) {
            if (!m_default_uuid) {
                THROWF(db0::InternalException) << "No default fixture";
            }
            uuid = *m_default_uuid;
        }
        
        auto it = m_fixtures.find(uuid);
        if (it != m_fixtures.end()) {
            return it->second;
        }
        
        auto head_fixture = m_workspace_ptr->tryGetFixture(uuid, AccessType::READ_ONLY);
        if (!head_fixture) {
            return nullptr;
        }
        assert(head_fixture->getUUID() == uuid);
        auto result = head_fixture->getSnapshot(*this, getSnapshotStateNum(*head_fixture));
        // initialize snapshot (use both Workspace and WorkspaceView initializers)
        auto fx_initializer = m_workspace_ptr->getFixtureInitializer();
        // initialize as read-only
        if (fx_initializer) {
            fx_initializer(result, false, true, true);
            initSnapshot(head_fixture, result);
        }
        m_fixtures[uuid] = result;
        m_name_uuids[head_fixture->getPrefix().getName()] = uuid;
        return result;
    }

    bool WorkspaceView::close(const PrefixName &prefix_name)
    {
        if (m_closed) {
            return false;
        }
        auto it = m_name_uuids.find(prefix_name);
        if (it != m_name_uuids.end()) {
            auto it_fixture = m_fixtures.find(it->second);
            if (it_fixture != m_fixtures.end()) {
                it_fixture->second->close(false);
                m_fixtures.erase(it_fixture);
                return true;
            }
        }

        return false;
    }
    
    void WorkspaceView::close(bool as_defunct, ProcessTimer *timer_ptr)
    {        
        if (m_closed) {
            return;
        }
        
        std::unique_ptr<ProcessTimer> timer;
        if (timer_ptr) {
            timer = std::make_unique<ProcessTimer>("WorkspaceView::close", timer_ptr);
        }
        
        auto it = m_fixtures.begin(), end = m_fixtures.end();
        while (it != end) {
            it->second->close(as_defunct, timer.get());
            it = m_fixtures.erase(it);
        }

        if (as_defunct) {
            m_lang_cache->clearDefunct();
        }
        m_lang_cache = nullptr;
        m_closed = true;
    }
    
    db0::swine_ptr<Fixture> WorkspaceView::getCurrentFixture()
    {
        if (!m_default_uuid) {
            THROWF(db0::InternalException) << "No default fixture";
        }
        return getFixture(*m_default_uuid);
    }
    
    std::optional<std::uint64_t> WorkspaceView::getSnapshotStateNum(const Fixture &fixture) const
    {
        // look up by UUID first
        auto it = m_state_nums.find(fixture.getUUID());
        if (it != m_state_nums.end()) {
            return it->second;
        }
        
        // look up by name
        auto it_name = m_prefix_state_nums.find(fixture.getPrefix().getName());
        if (it_name != m_prefix_state_nums.end()) {
            m_state_nums[fixture.getUUID()] = it_name->second;
            return it_name->second;
        }
        
        return {};
    }
    
    std::shared_ptr<LangCache> WorkspaceView::getLangCache() const {
        return m_lang_cache;
    }
    
    bool WorkspaceView::isMutable() const {
        return false;
    }
    
    db0::swine_ptr<Fixture> WorkspaceView::tryFindFixture(const PrefixName &prefix_name) const
    {
        auto uuid = m_workspace_ptr->getUUID(prefix_name);
        if (!uuid) {
            // unknown or invalid prefix
            return {};
        }
        // try resolving from cached fixtures
        auto it = m_fixtures.find(*uuid);
        if (it != m_fixtures.end()) {
            return it->second;
        }
        return const_cast<WorkspaceView *>(this)->getFixture(*uuid);
    }

    Snapshot &WorkspaceView::getHeadWorkspace() const 
    {
        assert(m_workspace_ptr);
        return *m_workspace_ptr;
    }
    
    std::optional<AccessType> WorkspaceView::tryGetAccessType() const {
        // WorkspaceView is always read-only
        return AccessType::READ_ONLY;
    }
    
    std::size_t WorkspaceView::size() const {
        return m_state_nums.size();
    }

}