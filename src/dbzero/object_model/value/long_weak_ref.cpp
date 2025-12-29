// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "long_weak_ref.hpp"
#include <dbzero/object_model/object/Object.hpp>

namespace db0::object_model

{
    
    o_long_weak_ref::o_long_weak_ref(std::uint64_t fixture_uuid, UniqueAddress address)
        : m_fixture_uuid(fixture_uuid)
        , m_address(address)        
    {
    }
    
    LongWeakRef::LongWeakRef(db0::swine_ptr<Fixture> &fixture, const Object &obj)
        : super_t(fixture, obj.getFixtureUUID(), obj.getUniqueAddress())
    {
    }
    
    LongWeakRef::LongWeakRef(db0::swine_ptr<Fixture> &fixture, Address address)
        : super_t(super_t::tag_from_address(), fixture, address)
    {
    }
    
}