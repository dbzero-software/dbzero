// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <memory>
#include <dbzero/object_model/LangConfig.hpp>
#include <vector>
#include <shared_mutex>

namespace db0

{

    class Workspace;    
    using PyToolkit = db0::python::PyToolkit;
    using PyObjectPtr = PyToolkit::ObjectPtr;

    class LockedContext
    {
    public:
        using LangToolkit = db0::object_model::LangConfig::LangToolkit;
        using ObjectPtr = LangToolkit::ObjectPtr;
        using ObjectSharedPtr = LangToolkit::ObjectSharedPtr;

        LockedContext(std::shared_ptr<Workspace> &, std::shared_lock<std::shared_mutex> &&);
        
        // pairs of: prefix name / state number
        std::vector<std::pair<std::string, std::uint64_t> > getMutationLog() const;

        void close();
                    
        static std::shared_lock<std::shared_mutex> lockShared();
        static std::unique_lock<std::shared_mutex> lockUnique();

    private:
        std::shared_ptr<Workspace> m_workspace;
        // the workspace-assigned ID for this locked section
        const unsigned int m_locked_section_id;
        std::vector<std::pair<std::string, std::uint64_t> > m_mutation_log;
        
        // mutex to prevent auto-commit operations from locked context
        static std::shared_mutex m_locked_mutex;
        std::shared_lock<std::shared_mutex> m_lock;
    };

}