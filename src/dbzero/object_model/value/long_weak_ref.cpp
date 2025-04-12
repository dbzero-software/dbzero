#include "long_weak_ref.hpp"
#include <dbzero/object_model/object/Object.hpp>

namespace db0::object_model

{
    
    o_long_weak_ref::o_long_weak_ref(std::uint64_t fixture_uuid, std::uint64_t address)
        : m_fixture_uuid(fixture_uuid)
        , m_address(address)        
    {
    }
    
    LongWeakRef::LongWeakRef(db0::swine_ptr<Fixture> &fixture, const Object &obj)
        : super_t(fixture, obj.getFixtureUUID(), obj.getAddress())
    {
    }
    
    LongWeakRef::LongWeakRef(db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
        : super_t(super_t::tag_from_address(), fixture, address)
    {
    }

}