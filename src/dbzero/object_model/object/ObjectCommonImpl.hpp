#pragma once

#include "ObjectImplBase.hpp"
#include "o_object.hpp"

namespace db0::object_model

{
    
    // NOTE: ObjectCommonImpl is for reinterpret_cast purposes only
    // it allows accessing Object or ObjectImmutableImpl instances under a common base type
    class ObjectCommonImpl: public ObjectImplBase<o_object_base, ObjectCommonImpl>
    {
    public:
        static constexpr unsigned char REALM_ID = o_object_base::REALM_ID;
        using super_t = ObjectImplBase<o_object_base, ObjectCommonImpl>;
        
    protected:
        friend super_t;
    };
    
}
