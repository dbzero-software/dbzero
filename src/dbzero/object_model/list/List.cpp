#include "List.hpp"
#include <dbzero/object_model/value.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/object.hpp>
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0::object_model

{
    GC0_Define(List)

    template <typename LangToolkit> o_typed_item createListItem(db0::swine_ptr<Fixture> &fixture,
        db0::bindings::TypeId type_id, typename LangToolkit::ObjectPtr lang_value, StorageClass storage_class)
    {
        return { storage_class, createMember<LangToolkit>(fixture, type_id, lang_value) };
    }
    
    List::List(db0::swine_ptr<Fixture> &fixture)
        : super_t(fixture)
    {
    }
    
    List::List(db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
        : super_t(super_t::tag_from_address(), fixture, address)
    {
    }

    List::List(db0::swine_ptr<Fixture> &fixture, List& list)
        : super_t(fixture, list)
    {
    }
    
    void List::append(FixtureLock &fixture, ObjectPtr lang_value)
    {
        using TypeId = db0::bindings::TypeId;
        
        // recognize type ID from language specific object
        auto type_id = LangToolkit::getTypeManager().getTypeId(lang_value);
        auto storage_class = TypeUtils::m_storage_class_mapper.getStorageClass(type_id);
        v_bvector::push_back(
            createListItem<LangToolkit>(*fixture, type_id, lang_value, storage_class)
        );
    }
    
    List::ObjectSharedPtr List::getItem(std::size_t i) const
    {
        if (i >= size()) {
            THROWF(db0::InputException) << "Index out of range: " << i;
        }
        auto [storage_class, value] = (*this)[i];
        auto fixture = this->getFixture();
        return unloadMember<LangToolkit>(fixture, storage_class, value);
    }

    List::ObjectSharedPtr List::pop(FixtureLock &fixture, std::size_t i)
    {
        if (size() == 0) {
            THROWF(db0::InputException) << "Cannot pop from empty container ";
        }
        if (i >= size()) {
            THROWF(db0::InputException) << "Index out of range: " << i;
        }
        auto [storage_class, value] = (*this)[i];
        auto member = unloadMember<LangToolkit>(*fixture, storage_class, value);
        this->swapAndPop(fixture, {i});
        return member;
    }

    void List::setItem(FixtureLock &fixture, std::size_t i, ObjectPtr lang_value)
    {
        if (i >= size()) {
            THROWF(db0::InputException) << "Index out of range: " << i;
        }

        // recognize type ID from language specific object
        auto type_id = LangToolkit::getTypeManager().getTypeId(lang_value);
        auto storage_class = TypeUtils::m_storage_class_mapper.getStorageClass(type_id);        
        v_bvector::setItem(i, createListItem<LangToolkit>(*fixture, type_id, lang_value, storage_class));
    }
    
    List *List::makeNew(void *at_ptr, db0::swine_ptr<Fixture> &fixture) {
        return new (at_ptr) List(fixture);
    }
    
    List *List::unload(void *at_ptr, db0::swine_ptr<Fixture> &fixture, std::uint64_t address) {
        return new (at_ptr) List(fixture, address);
    }
    
    List * List::copy(void *at_ptr, db0::swine_ptr<Fixture> &fixture) {
        return new (at_ptr) List(fixture, *this);
    }
    
    std::size_t List::count(ObjectPtr lang_value)
    {
        std::size_t count = 0;
        auto fixture = this->getFixture();
        for (auto &elem: (*this)) {
            auto [elem_storage_class, elem_value] = elem;
            if (unloadMember<LangToolkit>(fixture, elem_storage_class, elem_value) == lang_value) {
                count += 1;
            }
        }
        return count;
    }

    std::size_t List::index(ObjectPtr lang_value)
    {
        std::size_t index = 0;
        auto fixture = this->getFixture();
        for (auto &elem: (*this)) {
            auto [elem_storage_class, elem_value] = elem;
            if (unloadMember<LangToolkit>(fixture, elem_storage_class, elem_value) == lang_value) {
                return index;
            }
            ++index;
        }
        THROWF(db0::InputException) << "Item is not in a list ";
        return -1;
    }

    bool List::operator==(const List &list) const
    {
        if (size() != list.size()) {
            return false;
        }
        return std::equal(begin(), end(), list.begin());
    }

    bool List::operator!=(const List &list) const
    {
        if (size() != list.size()) {
            return false;
        }
        return !(*this == list);
    }

    void List::drop() {
        v_bvector<o_typed_item>::destroy();
    }

    void List::clear(FixtureLock &) {
        // FIXME: drop items
        v_bvector<o_typed_item>::clear();
    }

    void List::swapAndPop(FixtureLock &, const std::vector<uint64_t> &element_numbers) {
        // FIXME: drop items
        v_bvector<o_typed_item>::swapAndPop(element_numbers);
    }

}
