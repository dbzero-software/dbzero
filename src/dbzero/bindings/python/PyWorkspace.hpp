// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <memory>
#include <string>
#include <vector>
#include <functional>
#include <mutex>
#include "PyTypes.hpp"
#include "PyWrapper.hpp"
#include <dbzero/core/memory/AccessOptions.hpp>
#include <dbzero/core/memory/swine_ptr.hpp>
#include "MemoObject.hpp"

namespace db0 {
    
    class Workspace;
    class Config;
    class Fixture;
    class Memspace;
    class ProcessTimer;

}

namespace db0::object_model

{
    
    class Object; 
    
}

namespace db0::python

{

    /**
     * The class to track python module / fixture associations
    */
    class PyWorkspace
    {
    public:
        using ObjectPtr = typename PyTypes::ObjectPtr;
        using ObjectSharedPtr = typename PyTypes::ObjectSharedPtr;
        using TypeObjectPtr = typename PyTypes::TypeObjectPtr;
        
        PyWorkspace();
        ~PyWorkspace();
        
        bool hasWorkspace() const;
        
        /**
         * Initialize Python workspace
         * @param root_path use "" for current directory
         * @param py_config reference to a python dict which holds configuration, from which configuration 
         * is dynamically fetched just-in-time
        */
        void initWorkspace(const std::string &root_path, ObjectPtr py_config = nullptr, ObjectPtr py_lock_flags = nullptr);
        
        /**
         * Opens a specific prefix for read or read/write
         * a newly opened read/write prefix becomes the default one
         * @param slab_size will only have effect for a newly created prefixes
         * @param page_io_step_size parameter only respected for newly created prefixes
        */
        void open(const std::string &prefix_name, AccessType, std::optional<bool> autocommit = {},
            std::optional<std::size_t> slab_size = {}, ObjectPtr lock_flags = nullptr, 
            std::optional<std::size_t> meta_io_step_size = {}, std::optional<std::size_t> page_io_step_size = {}
        );
        
        db0::Workspace &getWorkspace() const;
        
        std::shared_ptr<db0::Workspace> getWorkspaceSharedPtr() const;
        
        void close(ProcessTimer *timer = nullptr);

        bool refresh();
        
        // stop threads as a separate function because it's not an API function
        void stopThreads();

        const std::shared_ptr<db0::Config> &getConfig() const;

    private:
        std::shared_ptr<db0::Workspace> m_workspace;
        // optional DB0 config object
        std::shared_ptr<db0::Config> m_config;        
    };
    
}