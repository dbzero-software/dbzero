#pragma once

#include <dbzero/object_model/has_fixture.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include "Value.hpp"
#include "StorageClass.hpp"

namespace db0::object_model

{
    
    class Object;
    
    struct [[gnu::packed]] o_weak_ref: public o_fixed<o_weak_ref>
    {
        std::uint64_t m_fixture_uuid;
        // the full logical address (i.e. physical address + instance ID) of a memo object
        std::uint64_t m_address;

        o_weak_ref(std::uint64_t fixture_uuid, std::uint64_t address);
    };
    
    class WeakRef: public db0::has_fixture<db0::v_object<o_weak_ref> >
    {   
        using super_t = db0::has_fixture<db0::v_object<o_weak_ref> >;
    public:
        WeakRef(db0::swine_ptr<Fixture> &fixture, const Object &);
        WeakRef(db0::swine_ptr<Fixture> &fixture, std::uint64_t address);
    };
    
}