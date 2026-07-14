// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstddef>
#include <Python.h>
#include "shared_py_object.hpp"
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include "Migration.hpp"
#include <functional>
#include <atomic>
#include <optional>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <dbzero/workspace/PrefixName.hpp>
#include <dbzero/object_model/object/Options.hpp>

namespace db0::python

{
 
    using MemoOptions = db0::object_model::MemoOptions;
    using MemoFlags = db0::object_model::MemoFlags;
        
    using AccessType = db0::AccessType;

    class MemoTypeDecoration
    {   
    public:     
        MemoTypeDecoration() = default;

        MemoTypeDecoration(MemoTypeDecoration &&);

        MemoTypeDecoration(shared_py_object<PyObject*> py_module,
            const char *prefix_name, const char *type_id, 
            const char *file_name, std::vector<std::string> &&init_vars, MemoFlags flags,
            shared_py_object<PyObject*> py_dyn_prefix_callable,
            std::vector<Migration> &&migrations);
        
        ~MemoTypeDecoration();
        
        // get decoration of a given memo type
        static MemoTypeDecoration &get(PyTypeObject *);
        
        // @return nullptr if no file name is set
        inline const char *tryGetFileName() const {
            return m_file_name;
        }

        // @return nullptr if no prefix name is set
        inline const char *tryGetPrefixName() const {
            return m_prefix_name.isValid() ? m_prefix_name.c_str() : nullptr; 
        }
        
        // @return nullptr if no type id is set
        inline const char *tryGetTypeId() const {
            return m_type_id;
        }
        
        // Check if scope of this type is limited to a specific prefix
        bool isScoped() const;
        
        // NOTE: may return invalid / empty prefix name
        const db0::PrefixName &getPrefixName() const;
        // @return variables potentially asignable during the type initialization
        const std::vector<std::string> &getInitVars() const;
        
        // @param access_type to use for opening the prefix if UUID needs to be resolved by name
        // note that read-only access cannot later be upgraded to read-write
        // NOTE: if access type is not provided (std::nullopt), then READ_WRITE will be used as the default
        std::uint64_t getFixtureUUID(std::optional<AccessType> access_type = AccessType::READ_WRITE) const;
        
        // check if the dyn-prefix callable is set
        bool hasDynPrefix() const;
        
        // resolve dynamic prefix from the callable
        std::string getDynPrefix(PyObject *args, PyObject *kwargs) const;
        
        // Check if there're any migrations defined for this type
        bool hasMigrations() const;
        
        // Identify applicable migrations and invoke callbacks in member-initialization order
        void forAllMigrations(const std::unordered_set<std::string> &available_members,
            std::function<bool(Migration &)> callback) const;
        
        MemoTypeDecoration &operator=(MemoTypeDecoration &&);
        
        void close();
        
        inline MemoFlags getFlags() const {
            return m_flags;
        }
        
    private:
        // module where the type is defined
        shared_py_object<PyObject*> m_py_module;
        PrefixName m_prefix_name;
        const char *m_type_id = 0;
        const char *m_file_name = 0;
        // variables potentially asignable during the type initialization
        std::vector<std::string> m_init_vars;
        MemoFlags m_flags;
        // resolved fixture UUID (initialized by the process)
        mutable std::atomic<std::uint64_t> m_fixture_uuid = 0;
        // dynamic prefix callable
        shared_py_object<PyObject*> m_py_dyn_prefix_callable;
        std::vector<Migration> m_migrations;
        // by-name migrations' index
        std::unordered_map<std::string, Migration*> m_ix_migrations;

        void init();
    };
    
}
