#include "Block.hpp"
#include <dbzero/object_model/value.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/object_model/object.hpp>
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0::object_model::pandas

{

    GC0_Define(Block)
        
    Block::Block(db0::swine_ptr<Fixture> &fixture)
        : super_t(fixture)
    {
    }
    
    Block::Block(db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
        : super_t(super_t::tag_from_address(), fixture, address)
    {
    }

    void Block::append(FixtureLock &fixture, ObjectPtr lang_value)
    {
        using TypeId = db0::bindings::TypeId;
        
        // recognize type ID from language specific object
        auto type_id = LangToolkit::getTypeManager().getTypeId(lang_value);
        // first element defines storage class
        if (size() == 0) {
            m_storage_class = TypeUtils::m_storage_class_mapper.getStorageClass(type_id);
        } else {
            if( m_storage_class != TypeUtils::m_storage_class_mapper.getStorageClass(type_id)){
                throw std::runtime_error("Storage class shoud be same for all Block elements");
            }
        }        
        v_bvector::push_back(
            createMember<LangToolkit>(*fixture, type_id, lang_value, m_storage_class) 
        );
    }

    Block::ObjectSharedPtr Block::getStorageClass() {
        return PyLong_FromUnsignedLong((std::uint8_t )m_storage_class);
    }

    Block::ObjectSharedPtr Block::getItem(std::size_t i) const
    {
        if (i >= size()) {
            THROWF(db0::InputException) << "Index out of range: " << i;
        }
        auto  value = (*this)[i];
        return unloadMember<LangToolkit>(*this, m_storage_class, value);
    }
    
    void Block::setItem(FixtureLock &fixture, std::size_t i, ObjectPtr lang_value)
    {
        if (i >= size()) {
            THROWF(db0::InputException) << "Index out of range: " << i;
        }

        // recognize type ID from language specific object
        auto type_id = LangToolkit::getTypeManager().getTypeId(lang_value);
        auto storage_class = TypeUtils::m_storage_class_mapper.getStorageClass(type_id);
        v_bvector::setItem(i, createMember<LangToolkit>(*fixture, type_id, lang_value, storage_class));
    }
    
    Block *Block::makeNew(void *at_ptr, db0::swine_ptr<Fixture> &fixture) {
        return new (at_ptr) Block(fixture);
    }
    
    Block *Block::unload(void *at_ptr, db0::swine_ptr<Fixture> &fixture, std::uint64_t address) {
        return new (at_ptr) Block(fixture, address);
    }

}
