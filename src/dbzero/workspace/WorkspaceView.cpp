#include "WorkspaceView.hpp"
#include <dbzero/object_model/class/ClassFactory.hpp>

namespace db0

{
    
    /**
     * Fixture view initializer (currently empty)
    */
    std::function<void(db0::swine_ptr<Fixture> &, bool is_new)> view_initializer()
    {
        using ClassFactory = db0::object_model::ClassFactory;
        return [](db0::swine_ptr<Fixture> &fixture, bool)
        {
        };
    }

    WorkspaceView::WorkspaceView(std::shared_ptr<Workspace> workspace)
        : m_workspace(workspace)
        , m_default_uuid(workspace->getDefaultUUID())
    {
        // take snapshots of all open fixtures
        m_workspace->forAll([this](const Fixture &fixture)
        {
            getFixture(fixture.getUUID());
        });
    }
    
    db0::swine_ptr<Fixture> WorkspaceView::getFixture(const std::string &prefix_name, std::optional<AccessType> access_type)
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

        auto fixture = m_workspace->getFixture(prefix_name);
        // get snapshot of the latest state
        auto result = fixture->getSnapshot();
        // initialize snapshot (use both Workspace and WorkspaceView initializers)
        auto fx_initializer = m_workspace->getFixtureInitializer();
        if (fx_initializer) {
            fx_initializer(result, false);
            view_initializer()(result, false);
        }

        m_fixtures[fixture->getUUID()] = result;
        m_name_uuids[prefix_name] = fixture->getUUID();
        return result;
    }
    
    db0::swine_ptr<Fixture> WorkspaceView::getFixture(std::uint64_t uuid, std::optional<AccessType> access_type)
    {
        if (m_closed) {
            THROWF(db0::InternalException) << "WorkspaceView is closed";
        }
        
        if (access_type && *access_type != AccessType::READ_ONLY) {
            THROWF(db0::InternalException) << "WorkspaceView does not support read/write access";
        }

        auto it = m_fixtures.find(uuid);
        if (it != m_fixtures.end()) {
            return it->second;
        }
        
        auto fixture = m_workspace->getFixture(uuid);
        // get snapshot of the latest state
        auto result = fixture->getSnapshot();
        // initialize snapshot (use both Workspace and WorkspaceView initializers)
        auto fx_initializer = m_workspace->getFixtureInitializer();
        if (fx_initializer) {
            fx_initializer(result, false);
            view_initializer()(result, false);
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
    
    db0::swine_ptr<Fixture> WorkspaceView::getCurrentFixture(std::optional<AccessType> access_type)
    {
        if (access_type && *access_type != AccessType::READ_ONLY) {
            THROWF(db0::InternalException) << "WorkspaceView does not support read/write access";
        }
        return getFixture(m_default_uuid);
    }

    WorkspaceView *WorkspaceView::makeNew(void *at_ptr, std::shared_ptr<Workspace> workspace) {
        return new (at_ptr) WorkspaceView(workspace);
    }
    
}