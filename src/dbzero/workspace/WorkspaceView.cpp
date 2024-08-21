#include "WorkspaceView.hpp"
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/object_model/LangCache.hpp>

namespace db0

{
    
    /**
     * Fixture view initializer (currently empty)
    */
    std::function<void(db0::swine_ptr<Fixture> &, bool, bool)> view_initializer()
    {
        return [](db0::swine_ptr<Fixture> &fixture, bool, bool)
        {
        };
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
        : m_workspace(workspace)
        , m_workspace_ptr(workspace_ptr)        
        , m_default_uuid(workspace_ptr->getDefaultUUID())
        , m_prefix_state_nums(prefix_state_nums)
        , m_lang_cache(std::make_unique<LangCache>())
    {
        if (!state_num && m_default_uuid) {
            // freeze state number of the default fixture
            auto fixture = m_workspace_ptr->getFixture(*m_default_uuid);
            auto it = m_prefix_state_nums.find(fixture->getPrefix().getName());
            // state number for the default fixture defined by name
            if (it != m_prefix_state_nums.end()) {
                state_num = it->second;
            } else {
                state_num = fixture->getPrefix().getStateNum();
            }
        }

        // check for conflicting requests
        if (m_default_uuid) {
            assert(state_num);
            auto fixture = m_workspace_ptr->getFixture(*m_default_uuid);
            auto it = m_prefix_state_nums.find(fixture->getPrefix().getName());
            if (it != m_prefix_state_nums.end() && it->second != *state_num) {
                THROWF(db0::InternalException) << "Conflicting state numbers requested for the default fixture: " << fixture->getPrefix().getName();
            }
            m_state_nums[*m_default_uuid] = *state_num;
        }

        // freeze state numbers of the remaining open fixtures
        m_workspace_ptr->forAll([this](const Fixture &fixture) {
            if (!m_default_uuid || *m_default_uuid != fixture.getUUID()) {
                auto it = m_prefix_state_nums.find(fixture.getPrefix().getName());
                if (it != m_prefix_state_nums.end()) {
                    m_state_nums[fixture.getUUID()] = it->second;
                } else {
                    m_state_nums[fixture.getUUID()] = fixture.getPrefix().getStateNum();
                }
            }
        });
    }
    
    WorkspaceView::~WorkspaceView()
    {
    }

    db0::swine_ptr<Fixture> WorkspaceView::getFixture(
        const std::string &prefix_name, std::optional<AccessType> access_type)
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

        auto fixture = m_workspace_ptr->getFixture(prefix_name);
        // get snapshot of the latest state
        auto result = fixture->getSnapshot(*this, getSnapshotStateNum(*fixture));
        // initialize snapshot (use both Workspace and WorkspaceView initializers)
        auto fx_initializer = m_workspace_ptr->getFixtureInitializer();
        // initialize as read-only
        if (fx_initializer) {
            fx_initializer(result, false, true);
            view_initializer()(result, false, true);
        }

        m_fixtures[fixture->getUUID()] = result;
        m_name_uuids[prefix_name] = fixture->getUUID();
        return result;
    }
    
    bool WorkspaceView::hasFixture(const std::string &prefix_name) const {
        return m_workspace->hasFixture(prefix_name);
    }
    
    db0::swine_ptr<Fixture> WorkspaceView::getFixture(std::uint64_t uuid, std::optional<AccessType> access_type)
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
        
        auto fixture = m_workspace_ptr->getFixture(uuid);
        auto result = fixture->getSnapshot(*this, getSnapshotStateNum(*fixture));
        // initialize snapshot (use both Workspace and WorkspaceView initializers)
        auto fx_initializer = m_workspace_ptr->getFixtureInitializer();
        // initialize as read-only
        if (fx_initializer) {
            fx_initializer(result, false, true);
            view_initializer()(result, false, true);
        }
        m_fixtures[fixture->getUUID()] = result;
        m_name_uuids[fixture->getPrefix().getName()] = fixture->getUUID();
        return result;
    }

    bool WorkspaceView::close(const std::string &prefix_name)
    {
        if (m_closed) {
            return false;
        }
        auto it = m_name_uuids.find(prefix_name);
        if (it != m_name_uuids.end()) {
            auto it_fixture = m_fixtures.find(it->second);
            if (it_fixture != m_fixtures.end()) {
                it_fixture->second->close();
                m_fixtures.erase(it_fixture);
                return true;
            }
        }

        return false;
    }
    
    void WorkspaceView::close()
    {
        if (m_closed) {
            return;
        }

        auto it = m_fixtures.begin(), end = m_fixtures.end();
        while (it != end) {
            it->second->close();
            it = m_fixtures.erase(it);
        }
        m_closed = true;
    }
    
    db0::swine_ptr<Fixture> WorkspaceView::getCurrentFixture()
    {
        if (!m_default_uuid) {
            THROWF(db0::InternalException) << "No default fixture";
        }
        return getFixture(*m_default_uuid);
    }

    WorkspaceView *WorkspaceView::makeNew(void *at_ptr, std::shared_ptr<Workspace> workspace,
        std::optional<std::uint64_t> state_num, const std::unordered_map<std::string, std::uint64_t> &prefix_state_nums) 
    {
        return new (at_ptr) WorkspaceView(workspace, state_num, prefix_state_nums);
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
    
    LangCache &WorkspaceView::getLangCache() const {
        return *m_lang_cache;
    }

}