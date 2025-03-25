#pragma once

#include <memory>
#include <dbzero/object_model/LangConfig.hpp>
#include <unordered_set>
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
        
        static void makeNew(void *, std::shared_ptr<Workspace> &, std::shared_lock<std::shared_mutex> &&);
        
        static std::shared_lock<std::shared_mutex> lockShared();
        static std::unique_lock<std::shared_mutex> lockUnique();

    private:
        std::shared_ptr<Workspace> m_workspace;
        
        // mutex to prevent auto-commit operations from locked context
        static std::shared_mutex m_locked_mutex;
        std::shared_lock<std::shared_mutex> m_lock;
    };

}