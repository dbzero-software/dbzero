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
        : m_workspace(workspace)
        , m_workspace_ptr(workspace_ptr)        
        , m_default_uuid(workspace_ptr->getDefaultUUID())
        , m_prefix_state_nums(prefix_state_nums)
        , m_lang_cache(std::make_shared<LangCache>())
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
        
        // freeze state numbers of the remaining open fixtures
        m_workspace_ptr->forEachFixture([this](const Fixture &fixture) {
            if (!m_default_uuid || *m_default_uuid != fixture.getUUID()) {
                auto it = m_prefix_state_nums.find(fixture.getPrefix().getName());
                if (it != m_prefix_state_nums.end()) {
                    m_state_nums[fixture.getUUID()] = it->second;
                } else {
                    m_state_nums[fixture.getUUID()] = fixture.getPrefix().getStateNum();
                }
            }
            return true;
        });
    }
    
    WorkspaceView::~WorkspaceView()
    {
    }
    
    db0::swine_ptr<Fixture> WorkspaceView::getFixture(
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
        
        auto head_fixture = m_workspace_ptr->getFixture(prefix_name, AccessType::READ_ONLY);
        auto fixture_uuid = head_fixture->getUUID();
        // get snapshot of the latest state
        auto result = head_fixture->getSnapshot(*this, getSnapshotStateNum(*head_fixture));
        // initialize snapshot (use both Workspace and WorkspaceView initializers)
        auto fx_initializer = m_workspace_ptr->getFixtureInitializer();
        // initialize as read-only
        if (fx_initializer) {
            fx_initializer(result, false, true);
            initSnapshot(head_fixture, result);
        }
        
        m_fixtures[fixture_uuid] = result;
        m_name_uuids[prefix_name] = fixture_uuid;
        return result;
    }
    
    bool WorkspaceView::hasFixture(const PrefixName &prefix_name) const {
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
        
        auto head_fixture = m_workspace_ptr->getFixture(uuid, AccessType::READ_ONLY);
        assert(head_fixture->getUUID() == uuid);
        auto result = head_fixture->getSnapshot(*this, getSnapshotStateNum(*head_fixture));
        // initialize snapshot (use both Workspace and WorkspaceView initializers)
        auto fx_initializer = m_workspace_ptr->getFixtureInitializer();
        // initialize as read-only
        if (fx_initializer) {
            fx_initializer(result, false, true);
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
                it_fixture->second->close();
                m_fixtures.erase(it_fixture);
                return true;
            }
        }

        return false;
    }
    
    void WorkspaceView::close(ProcessTimer *timer_ptr)
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
            it->second->close();
            it = m_fixtures.erase(it);
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

}