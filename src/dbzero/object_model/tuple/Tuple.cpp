#include "Tuple.hpp"
#include <dbzero/object_model/value.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/object.hpp>
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0::object_model

{
    
    template <typename LangToolkit> o_typed_item createTupleItem(const Tuple &tuple,
    db0::bindings::TypeId type_id, typename LangToolkit::ObjectPtr lang_value, StorageClass storage_class)
    {
        return { storage_class, createMember<LangToolkit>(tuple, type_id, lang_value, storage_class) };
    }

    GC0_Define(Tuple)
    Tuple::Tuple(std::size_t size, db0::swine_ptr<Fixture> &fixture)
        : super_t(fixture, size)
    {
    }
    
    Tuple::Tuple(db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
        : super_t(super_t::tag_from_address(), fixture, address)
    {
    }
    
    Tuple::ObjectSharedPtr Tuple::getItem(std::size_t i) const
    {
        if (i >= getData()->size()) {
            THROWF(db0::InputException) << "Index out of range: " << i;
        }
        auto [storage_class, value] = (*getData())[i];
        return unloadMember<LangToolkit>(*this, storage_class, value);
    }
    
    void Tuple::setItem(std::size_t i, ObjectPtr lang_value){
        if (i >= getData()->size()) {
            THROWF(db0::InputException) << "Index out of range: " << i;
        }
        // recognize type ID from language specific object
        auto type_id = LangToolkit::getTypeManager().getTypeId(lang_value);
        auto storage_class = TypeUtils::m_storage_class_mapper.getStorageClass(type_id);
        modify()[i] = createTupleItem<LangToolkit>(*this, type_id, lang_value, storage_class);
    }

    Tuple *Tuple::makeNew(void *at_ptr, db0::swine_ptr<Fixture> &fixture, std::size_t size)
    {
        return new (at_ptr) Tuple(size, fixture);
    }
    
    Tuple *Tuple::unload(void *at_ptr, db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
    {
        return new (at_ptr) Tuple(fixture, address);
    }

    size_t Tuple::count(ObjectPtr lang_value)
    {
        size_t count = 0;
        for(auto &elem: this->const_ref()){
            auto [elem_storage_class, elem_value] = elem;
            if(unloadMember<LangToolkit>(*this, elem_storage_class, elem_value) == lang_value) {
                count += 1;
            }
        }
        return count;
    }

    size_t Tuple::index(ObjectPtr lang_value)
    {
        size_t index = 0;
        for(auto &elem: this->const_ref()){
            auto [elem_storage_class, elem_value] = elem;
            if(unloadMember<LangToolkit>(*this, elem_storage_class, elem_value) == lang_value) {
                return index;
            }
            index += 1;
        }
        THROWF(db0::InputException) << "Item is not in a list ";
        return -1;
    }

    bool Tuple::operator==(const Tuple &tuple) const
    {
        return false;
    }

    bool Tuple::operator!=(const Tuple &tuple) const
    {
        return false;
    }
    
    void Tuple::drop()
    {
        v_object<o_tuple>::destroy();
    }
    
}
