#pragma once

#include <dbzero/object_model/has_fixture.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include "Value.hpp"
#include "StorageClass.hpp"

namespace db0::object_model

{
    
    class Object;
    
    struct [[gnu::packed]] o_long_weak_ref: public o_fixed<o_long_weak_ref>
    {
        std::uint64_t m_fixture_uuid;
        // the full logical address (i.e. physical address + instance ID) of a memo object
        Address m_address;
        
        o_long_weak_ref(std::uint64_t fixture_uuid, Address);
    };
    
    class LongWeakRef: public db0::has_fixture<db0::v_object<o_long_weak_ref> >
    {   
        using super_t = db0::has_fixture<db0::v_object<o_long_weak_ref> >;
    public:
        LongWeakRef(db0::swine_ptr<Fixture> &fixture, const Object &);
        LongWeakRef(db0::swine_ptr<Fixture> &fixture, Address);
    };
    
}