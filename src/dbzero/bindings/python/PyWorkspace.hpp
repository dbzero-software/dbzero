#pragma once

#include <memory>
#include <string>
#include <vector>
#include <functional>
#include "PyTypes.hpp"
#include "PyWrapper.hpp"
#include <dbzero/core/memory/AccessOptions.hpp>
#include <dbzero/core/memory/swine_ptr.hpp>

namespace db0 {

    class Workspace;
    class Fixture;
    class Memspace;

}

namespace db0::object_model 

{
    
    class Object; 
    
}

namespace db0::python

{

    using MemoObject = PyWrapper<db0::object_model::Object>;

    /**
     * The class to track python module / fixture associations
    */
    class PyWorkspace
    {
    public:
        using ObjectPtr = typename PyTypes::ObjectPtr;
        using TypeObjectPtr = typename PyTypes::TypeObjectPtr;
        
        PyWorkspace() = default;
                
        bool hasWorkspace() const;

        /**
         * Initialize Python workspace
         * @param root_path use "" for current directory
        */
        void initWorkspace(const std::string &root_path, std::optional<long> autocommit_interval_ms = {});
        
        /**
         * Opens a specific prefix for read or read/write
         * a newly opened read/write prefix becomes the default one
        */
        void open(const std::string &prefix_name, AccessType, bool autocommit);
        
        db0::Workspace &getWorkspace() const;
        
        std::shared_ptr<db0::Workspace> getWorkspaceSharedPtr() const;
        
        void close();

        bool refresh();
                    
    private:
        std::shared_ptr<db0::Workspace> m_workspace;
    };
    
}