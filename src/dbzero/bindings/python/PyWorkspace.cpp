// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

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
        if (m_workspace) {
            // NOTE close as defunct if python interpreter is no longer running
            m_workspace->close(!PyToolkit::isValid());
            m_workspace = nullptr;
        }
    }
    
    void PyWorkspace::open(const std::string &prefix_name, AccessType access_type, std::optional<bool> autocommit,
        std::optional<std::size_t> slab_size, ObjectPtr py_lock_flags, std::optional<std::size_t> meta_io_step_size,
        std::optional<std::size_t> page_io_step_size)
    {
        if (!m_workspace) {
            // initialize dbzero with current working directory
            initWorkspace("");
        }
        
        if (py_lock_flags) {
            db0::Config lock_flags_config(py_lock_flags);
            m_workspace->open(prefix_name, access_type, autocommit, slab_size, 
                lock_flags_config, meta_io_step_size, page_io_step_size
            );
        } else {
            m_workspace->open(prefix_name, access_type, autocommit, slab_size, 
                {}, meta_io_step_size, page_io_step_size
            );
        }
    }
    
    void PyWorkspace::initWorkspace(const std::string &root_path, ObjectPtr py_config, ObjectPtr py_lock_flags)
    {
        if (m_workspace) {
            THROWF(db0::InternalException) << "dbzero already initialized";
        }
        
        m_config = std::make_shared<db0::Config>(py_config);
        db0::Config default_lock_flags(py_lock_flags);
        // Retrieve the cache size from passed config parameters
        auto cache_size = m_config->get<unsigned long long>("cache_size");

        m_workspace = std::shared_ptr<db0::Workspace>(
            new Workspace(root_path, std::move(cache_size), {}, {}, {}, db0::object_model::initializer(), m_config, default_lock_flags));

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
            THROWF(db0::InternalException) << "dbzero not initialized";
        }
        return static_cast<db0::Workspace&>(*m_workspace);
    }
    
    std::shared_ptr<db0::Workspace> PyWorkspace::getWorkspaceSharedPtr() const
    {
        if (!m_workspace) {
            THROWF(db0::InternalException) << "dbzero not initialized";
        }
        return m_workspace;
    }
    
    void PyWorkspace::close(db0::ProcessTimer *timer_ptr)
    {
        std::unique_ptr<db0::ProcessTimer> timer;
        if (timer_ptr) {
            timer = std::make_unique<db0::ProcessTimer>("PyWorkspace::close", *timer_ptr);
        }
        if (m_workspace) {
            getWorkspace().close(false, timer.get());
            // NOTE: must unlock API because workspace destroy may trigger db0 object deletions            
            m_workspace = nullptr;            
        }
        PyToolkit::getTypeManager().close(timer.get());
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

    const std::shared_ptr<db0::Config> &PyWorkspace::getConfig() const
    {
        if (!m_workspace) {
            THROWF(db0::InternalException) << "dbzero not initialized";
        }
        return m_config;
    }
}