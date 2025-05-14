#include "List.hpp"
#include <dbzero/object_model/value.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/object.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/utils/ProcessTimer.hpp>
#include "ListIterator.hpp"

namespace db0::object_model

{
    GC0_Define(List)

    template <typename LangToolkit> o_typed_item createListItem(db0::swine_ptr<Fixture> &fixture,
        db0::bindings::TypeId type_id, typename LangToolkit::ObjectPtr lang_value, StorageClass storage_class)
    {        
        return { storage_class, createMember<LangToolkit>(fixture, type_id, storage_class, lang_value) };
    }
    
    List::List(db0::swine_ptr<Fixture> &fixture)
        : super_t(fixture)
    {
    }
    
    List::List(db0::swine_ptr<Fixture> &fixture, Address address)
        : super_t(super_t::tag_from_address(), fixture, address)
    {
    }

    List::List(db0::swine_ptr<Fixture> &fixture, const List &list)
        : super_t(fixture, list)
    {
    }

    List::List(tag_no_gc, db0::swine_ptr<Fixture> &fixture, const List &list)
        : super_t(tag_no_gc(), fixture, list)
    {
    }
    
    List::~List()
    {        
        // unregister needs to be called before destruction of members
        unregister();
    }
    
    void List::append(FixtureLock &fixture, ObjectPtr lang_value)
    {
        using TypeId = db0::bindings::TypeId;
        
        // recognize type ID from language specific object
        auto type_id = LangToolkit::getTypeManager().getTypeId(lang_value);
        auto pre_storage_class = TypeUtils::m_storage_class_mapper.getPreStorageClass(type_id);
        StorageClass storage_class;
        if (pre_storage_class == PreStorageClass::OBJECT_WEAK_REF) {
            storage_class = db0::getStorageClass(pre_storage_class, *fixture, lang_value);
        } else {
            storage_class = db0::getStorageClass(pre_storage_class);
        }
        
        v_bvector::push_back(
            createListItem<LangToolkit>(*fixture, type_id, lang_value, storage_class)
        );
        restoreIterators();
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
        restoreIterators();
        return member;
    }

    void List::setItem(FixtureLock &fixture, std::size_t i, ObjectPtr lang_value)
    {
        if (i >= size()) {
            THROWF(db0::InputException) << "Index out of range: " << i;
        }

        // recognize type ID from language specific object
        auto type_id = LangToolkit::getTypeManager().getTypeId(lang_value);
        auto pre_storage_class = TypeUtils::m_storage_class_mapper.getPreStorageClass(type_id);
        StorageClass storage_class;
        if (pre_storage_class == PreStorageClass::OBJECT_WEAK_REF) {
            storage_class = db0::getStorageClass(pre_storage_class, *fixture, lang_value);
        } else {
            storage_class = db0::getStorageClass(pre_storage_class);
        }
        
        auto [storage_class_value, value] = (*this)[i];
        v_bvector::setItem(i, createListItem<LangToolkit>(*fixture, type_id, lang_value, storage_class));
        unrefMember<LangToolkit>(*fixture, storage_class_value, value);
    }
    
    List *List::makeNew(void *at_ptr, db0::swine_ptr<Fixture> &fixture) {
        return new (at_ptr) List(fixture);
    }
    
    List *List::unload(void *at_ptr, db0::swine_ptr<Fixture> &fixture, Address address) {
        return new (at_ptr) List(fixture, address);
    }
    
    List * List::copy(void *at_ptr, db0::swine_ptr<Fixture> &fixture) const {
        return new (at_ptr) List(fixture, *this);
    }
    
    std::size_t List::count(ObjectPtr lang_value) const
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

    std::size_t List::index(ObjectPtr lang_value) const
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

    void List::clear(FixtureLock &) 
    {
        clearMembers();
        v_bvector<o_typed_item>::clear();
        restoreIterators();
    }

    void List::swapAndPop(FixtureLock &, const std::vector<std::uint64_t> &element_numbers) 
    {
        // FIXME: drop items
        v_bvector<o_typed_item>::swapAndPop(element_numbers);
        restoreIterators();
    }
    
    void List::moveTo(db0::swine_ptr<Fixture> &fixture) 
    {
        assert(hasInstance());
        if (this->size() > 0) {
            THROWF(db0::InputException) << "List with items cannot be moved to another fixture";
        }
        super_t::moveTo(fixture);
    }
    
    void List::destroy() const
    {
        clearMembers();
        super_t::destroy();
    }

    void List::clearMembers() const
    {
        // FIXME: optimization possible
        auto fixture = this->getFixture();
        for (auto &elem: (*this)) {
            auto [storage_class, value] = elem;
            unrefMember<LangToolkit>(fixture, storage_class, value);
        }
    }
    
    std::shared_ptr<ListIterator> List::getIterator(ObjectPtr lang_list) const
    {
        auto iter = std::shared_ptr<ListIterator>(new ListIterator(super_t::begin(), this, lang_list));
        // NOTE: we keep weak reference to the iterator to be able to invalidate / refresh it after list changes
        m_iterators.push_back(iter);
        return iter;
    }
    
    void List::restoreIterators()
    {
        if (m_iterators.empty()) {
            return;
        }
        m_iterators.forEach([](ListIterator &iter) {
            iter.restore();
        });
    }

}
