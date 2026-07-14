// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyWorkspace.hpp"
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/PrefixName.hpp>
#include <dbzero/workspace/Config.hpp>
#include <dbzero/core/memory/config.hpp>
#include <dbzero/object_model/ObjectModel.hpp>
#include <dbzero/object_model/object.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/object_model/tags/ObjectIteratorPool.hpp>
#include "PyRestrictedMethod.hpp"
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
        std::optional<std::size_t> page_io_step_size, std::optional<bool> restricted,
        ObjectPtr restricted_context, bool restricted_context_given, std::optional<bool> no_auto_migrate)
    {
        if (!m_workspace) {
            // initialize dbzero with current working directory
            initWorkspace("");
        }

        std::optional<PrefixFlags> prefix_flags;
        if (no_auto_migrate) {
            prefix_flags.emplace();
            prefix_flags->set(PrefixOptions::NO_AUTO_MIGRATE, *no_auto_migrate);
        }

        auto is_initial_prefix_config = !m_workspace->tryFindFixture(prefix_name);
        m_restricted_contexts.validateOpenRestricted(*m_workspace, prefix_name, restricted);
        
        if (py_lock_flags) {
            db0::Config lock_flags_config(py_lock_flags);
            m_workspace->open(prefix_name, access_type, autocommit, slab_size, 
                lock_flags_config, meta_io_step_size, page_io_step_size, restricted, prefix_flags
            );
        } else {
            m_workspace->open(prefix_name, access_type, autocommit, slab_size, 
                {}, meta_io_step_size, page_io_step_size, restricted, prefix_flags
            );
        }
        if (restricted && *restricted) {
            m_restricted_contexts.setRestricted(*m_workspace, m_config, restricted, nullptr, false, prefix_name);
        } else {
            m_restricted_contexts.applyOpenRestrictedContext(*m_workspace, prefix_name, restricted_context,
                restricted_context_given, is_initial_prefix_config);
        }
    }
    
    void PyWorkspace::initWorkspace(const std::string &root_path, ObjectPtr py_config, ObjectPtr py_lock_flags,
        bool restricted, ObjectPtr restricted_context)
    {
        if (m_workspace) {
            THROWF(db0::InternalException) << "dbzero already initialized";
        }
        
        m_config = std::make_shared<db0::Config>(py_config);
        db0::Config default_lock_flags(py_lock_flags);
        // Retrieve the cache size from passed config parameters
        auto cache_size = m_config->get<unsigned long long>("cache_size");

        auto object_model_initializer = db0::object_model::initializer();
        auto python_fixture_initializer = [this, object_model_initializer](db0::swine_ptr<db0::Fixture> &fixture,
            bool is_new, bool read_only, bool is_snapshot)
        {
            object_model_initializer(fixture, is_new, read_only, is_snapshot);
            db0::python::setFixtureRestrictedContext(fixture, getEffectiveRestrictedContext(*fixture));
            if (!is_snapshot) {
                auto &iterator_pool = fixture->addResource<db0::object_model::ObjectIteratorPool>();
                fixture->addIteratorDetachHandler([&iterator_pool](std::uint64_t generation) {
                    return iterator_pool.detach(generation);
                });
                fixture->addCloseHandler([&iterator_pool](bool commit) {
                    if (!commit) {
                        iterator_pool.close();
                    }
                });
            }
        };

        m_workspace = std::shared_ptr<db0::Workspace>(
            new Workspace(root_path, std::move(cache_size), {}, {}, {}, python_fixture_initializer, m_config, default_lock_flags));
        m_workspace->setDefaultRestricted(restricted);
        m_restricted_contexts.initDefault(*m_workspace, restricted_context);

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
        db0::object_model::InitManager::instance.close();
        PyToolkit::getTypeManager().close(timer.get());
        m_config = nullptr;
        m_restricted_contexts.clear();
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

    PyWorkspace::ObjectPtr PyWorkspace::getEffectiveRestrictedContext(const db0::Fixture &fixture) const
    {
        return m_restricted_contexts.getEffectiveContext(fixture);
    }

    void PyWorkspace::setRestricted(std::optional<bool> restricted, ObjectPtr restricted_context,
        bool restricted_context_given, const std::optional<std::string> &prefix_name)
    {
        if (!m_workspace) {
            initWorkspace("");
        }
        m_restricted_contexts.setRestricted(*m_workspace, m_config, restricted, restricted_context,
            restricted_context_given, prefix_name);
    }
}
