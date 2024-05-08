#pragma once

#include <memory>
#include <dbzero/core/collections/full_text/FT_Runnable.hpp>

namespace db0::object_model

{

    // A fixed-size wrapper over FT_Runnable
    class Runnable
    {
    public:
        Runnable(std::unique_ptr<FT_Runnable> &&runnable) 
            : runnable(std::move(runnable)) 
        {            
        }
        
        FT_Runnable &operator*() const {
            return *runnable;
        }

    private:
        std::unique_ptr<FT_Runnable> runnable;
    };

}