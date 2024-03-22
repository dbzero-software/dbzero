#include "PyWorkspace.hpp"
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/object_model/ObjectModel.hpp>
#include <dbzero/object_model/object.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include "PyToolkit.hpp"

namespace db0::python

{

    void PyWorkspace::open(const std::string &prefix_name, AccessType access_type)
    {
        if (!m_workspace) {
            // initialize DBZero with current working directory
            initWorkspace("");
        }
        m_workspace->open(prefix_name, access_type);
    }
    
    void PyWorkspace::initWorkspace(const std::string &root_path)
    {
        if (m_workspace) {
            THROWF(db0::InternalException) << "DBZero already initialized";
        }
        
        m_workspace = std::shared_ptr<db0::Workspace>(new Workspace(root_path, {}, {}, db0::object_model::initializer()));
    }
    
    db0::Workspace &PyWorkspace::getWorkspace() const
    {
        if (!m_workspace) {
            THROWF(db0::InternalException) << "DBZero not initialized";
        }
        return static_cast<db0::Workspace&>(*m_workspace);
    }
    
    std::shared_ptr<db0::Workspace> PyWorkspace::getWorkspaceSharedPtr() const
    {
        if (!m_workspace) {
            THROWF(db0::InternalException) << "DBZero not initialized";
        }
        return m_workspace;
    }
    
    void PyWorkspace::close()
    {
        if (m_workspace) {
            getWorkspace().close();
            m_workspace = nullptr;
        }
    }
    
    bool PyWorkspace::hasWorkspace() const
    {
        return m_workspace != nullptr;
    }
        
    bool PyWorkspace::refresh()
    {
        return getWorkspace().refresh();
    }

}