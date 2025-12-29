// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/object_model/has_fixture.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/core/compiler_attributes.hpp>
#include "Value.hpp"
#include "StorageClass.hpp"

namespace db0::object_model

{

DB0_PACKED_BEGIN
    
    class Object;
    
    struct DB0_PACKED_ATTR o_long_weak_ref: public o_fixed<o_long_weak_ref>
    {
        std::uint64_t m_fixture_uuid;
        // the full logical address (i.e. physical address + instance ID) of a memo object
        UniqueAddress m_address;
        
        o_long_weak_ref(std::uint64_t fixture_uuid, UniqueAddress);
    };
    
    class LongWeakRef: public db0::has_fixture<db0::v_object<o_long_weak_ref> >
    {   
        using super_t = db0::has_fixture<db0::v_object<o_long_weak_ref> >;
    public:
        LongWeakRef(db0::swine_ptr<Fixture> &fixture, const Object &);
        LongWeakRef(db0::swine_ptr<Fixture> &fixture, Address);
    };
    
DB0_PACKED_END

}