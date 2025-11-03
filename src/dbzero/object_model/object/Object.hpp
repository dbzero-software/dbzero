#pragma once

#include "ObjectImplBase.hpp"
#include "o_object.hpp"

namespace db0

{

    class Fixture;

}

namespace db0::object_model

{

    class Class;
    using Fixture = db0::Fixture;

    class Object: public ObjectImplBase<o_object>
    {
        // GC0 specific declarations
        GC0_Declare
    public:
        static constexpr unsigned char REALM_ID = o_object::REALM_ID;
        using super_t = ObjectImplBase<o_object>;        
        
        template <typename... Args>
        Object(Args&&... args)
            : super_t(std::forward<Args>(args)...)
        {
        }
    };
    
}
