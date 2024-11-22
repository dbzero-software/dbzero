#include "PyWorkspace.hpp"
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/workspace/PrefixName.hpp>
#include <dbzero/workspace/Config.hpp>
#include <dbzero/object_model/ObjectModel.hpp>
#include <dbzero/object_model/object.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/object_model/class/ClassFactory.hpp>
#include "PyToolkit.hpp"

namespace db0::python

{
    
    PyWorkspace::PyWorkspace()
    {
        if (!Py_IsInitialized()) {
            Py_InitializeEx(0);
        }
    }
    
    PyWorkspace::~PyWorkspace()
    {
    }
    
    void PyWorkspace::open(const std::string &prefix_name, AccessType access_type, std::optional<bool> autocommit,
        std::optional<std::size_t> slab_size, ObjectPtr py_lock_flags)
    {
        if (!m_workspace) {
            // initialize DBZero with current working directory
            initWorkspace("");
        }
        if (py_lock_flags) {
            db0::Config lock_flags_config(py_lock_flags);
            m_workspace->open(prefix_name, access_type, autocommit, slab_size, lock_flags_config);
        } else {
            m_workspace->open(prefix_name, access_type, autocommit, slab_size);
        }
    }
    
    void PyWorkspace::initWorkspace(const std::string &root_path, ObjectPtr py_config, ObjectPtr py_lock_flags)
    {
        if (m_workspace) {
            THROWF(db0::InternalException) << "DBZero already initialized";
        }
        
        m_config = std::make_shared<db0::Config>(py_config);
        db0::Config default_lock_flags(py_lock_flags);
        m_workspace = std::shared_ptr<db0::Workspace>(
            new Workspace(root_path, {}, {}, {}, {}, db0::object_model::initializer(), m_config, default_lock_flags));

        // register a callback to register bindings between known memo types (language specific objects)
        // and the corresponding Class instances. Note that types may be prefix agnostic therefore bindings may or
        // may not exist depending on the prefix
        m_workspace->setOnOpenCallback([](db0::swine_ptr<db0::Fixture> &fixture, bool is_new) {
            if (!is_new) {
                auto &class_factory = fixture->get<db0::object_model::ClassFactory>();
                PyToolkit::getTypeManager().forAllMemoTypes([&class_factory](TypeObjectPtr memo_type) {
                    class_factory.tryGetExistingType(memo_type);
                });
            }
        });
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
            // NOTE: must unlock API because workspace destroy may trigger db0 object deletions            
            m_workspace = nullptr;            
        }
        PyToolkit::getTypeManager().close();
        m_config = nullptr;
        m_workspace = nullptr;
    }
    
    bool PyWorkspace::hasWorkspace() const {
        return m_workspace != nullptr;
    }

    bool PyWorkspace::refresh() {
        return getWorkspace().refresh();
    }
    
    void PyWorkspace::stopThreads()
    {
        if (hasWorkspace()) {
            getWorkspace().stopThreads();
        }
    }

}