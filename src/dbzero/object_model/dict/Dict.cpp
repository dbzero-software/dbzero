
#include "Dict.hpp"
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/object_model/value.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/object.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/object_model/value/Member.hpp>


namespace db0::object_model

{

    GC0_Define(Dict)

    template <typename LangToolkit> o_typed_item createTypedItem(db0::swine_ptr<Fixture> &fixture,
        db0::bindings::TypeId type_id, typename LangToolkit::ObjectPtr lang_value, StorageClass storage_class)
    {
        return { storage_class, createMember<LangToolkit>(fixture, type_id, lang_value) };
    }

    template <typename LangToolkit> dict_item createDictItem(Memspace & memspace, const Dict &dict, std::uint64_t hash, 
        o_typed_item key, o_typed_item value)
    {
        DictIndex bindex(memspace, o_pair_item(key, value) );
        return { hash, bindex };
    }
    
    Dict::Dict(db0::swine_ptr<Fixture> &fixture)
        : super_t(fixture)
    {
    }
    
    Dict::Dict(db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
        : super_t(super_t::tag_from_address(), fixture, address)
    {
    }

    Dict::Dict(db0::swine_ptr<Fixture> &fixture, const Dict& dict)
        : super_t(fixture)
    {
        bulkInsert(dict.begin(), dict.end());
        m_size = dict.size();
    }
    
    void Dict::setItem(std::size_t hash, ObjectPtr key, ObjectPtr value)
    {
        using TypeId = db0::bindings::TypeId;
        auto type_id = LangToolkit::getTypeManager().getTypeId(value);
        auto storage_class = TypeUtils::m_storage_class_mapper.getStorageClass(type_id);
        auto fixture = this->getFixture();
        db0::FixtureLock lock(fixture);
        auto value_item = createTypedItem<LangToolkit>(*lock, type_id, value, storage_class);
        auto it = v_bindex::find(hash);
        auto memspace = this->getMemspace();
        if (it == end()) {
            // recognize type ID from language specific object
            auto type_id = LangToolkit::getTypeManager().getTypeId(key);
            auto storage_class = TypeUtils::m_storage_class_mapper.getStorageClass(type_id);
            auto key_item = createTypedItem<LangToolkit>(*lock, type_id, key, storage_class);
            v_bindex::insert(createDictItem<LangToolkit>(memspace, *lock, hash, key_item, value_item));
            m_size++;
        } else {
            auto type_id = LangToolkit::getTypeManager().getTypeId(key);
            auto storage_class = TypeUtils::m_storage_class_mapper.getStorageClass(type_id);
            auto key_item = createTypedItem<LangToolkit>(*lock, type_id, key, storage_class);
            auto modify_id = it.modifyItem();
            auto [_, address] = modify_id;
            auto memspace = this->getMemspace();
            auto bindex = address.getIndex(memspace);
            auto it_join = bindex.beginJoin(1);
            while(!it_join.is_end()){
                auto [storage_class, value] = (*it_join).m_first;
                auto member = unloadMember<LangToolkit>(fixture, storage_class, value);
                if (LangToolkit::compare(key, member.get())) {
                    bindex.erase(*it_join);
                    m_size--;
                    break;
                }
                ++it_join;

            }
            m_size++;
            bindex.insert(o_pair_item(key_item, value_item));
            auto new_address = bindex.getAddress();
            if(new_address != address.m_index_address){
                v_bindex::erase(it);
                v_bindex::insert(createDictItem<LangToolkit>(memspace, *lock, hash, key_item, value_item));
            }
        }           
    }

    Dict::ObjectSharedPtr Dict::getItem(std::size_t key, ObjectPtr key_value) const
    {
        auto iter = find(key);
        if (iter != end() ){
            auto [key, address] = *iter;
            auto memspace = this->getMemspace();
            auto bindex = address.getIndex(memspace);
            auto it = bindex.beginJoin(1);
            auto fixture = this->getFixture(); 
            
            while(!it.is_end()){
                auto [storage_class, value] = (*it).m_first;
                auto member = unloadMember<LangToolkit>(fixture, storage_class, value);
                if (LangToolkit::compare(key_value, member.get())) {
                    auto [storage_class, value] = (*it).m_second;
                    auto result = unloadMember<LangToolkit>(fixture, storage_class, value);
                    return result;
                }
                ++it;
            }
        }
        THROWF(db0::InputException) << "Item not found" << THROWF_END;        
    }
    
    Dict *Dict::makeNew(void *at_ptr, db0::swine_ptr<Fixture> &fixture) {
        return new (at_ptr) Dict(fixture);
    }
    
    Dict *Dict::unload(void *at_ptr, db0::swine_ptr<Fixture> &fixture, std::uint64_t address) {
        return new (at_ptr) Dict(fixture, address);
    }

    bool Dict::has_item(ObjectPtr obj) const 
    {
        auto hash = PyObject_Hash(obj);
        auto iter = find(hash);
        return iter != end();
    }

    Dict *Dict::copy(void *at_ptr, db0::swine_ptr<Fixture> &fixture) const {
        return new (at_ptr) Dict(fixture, *this);
    }

    Dict::ObjectSharedPtr Dict::pop(ObjectPtr obj)
    {
        auto hash = PyObject_Hash(obj);
        auto iter = find(hash);
        if (iter != end()) {
            auto [key, address] = *iter;
            auto bindex = address.getIndex(this->getMemspace());
            auto it = bindex.beginJoin(1);
            auto [storage_class, value] = (*it).m_second;
            auto fixture = this->getFixture();
            auto member = unloadMember<LangToolkit>(fixture, storage_class, value);
            if(bindex.size() == 1) {
                v_bindex::erase(iter);
                bindex.destroy();
            } else {
                bindex.erase(*it);
            }
            m_size -= 1;
            return member;
        } else {
            return nullptr;
        }
    }

    void Dict::moveTo(db0::swine_ptr<Fixture> &) {
        throw std::runtime_error("Not implemented");
    }

    void Dict::clear()
    {
        m_size = 0;
        v_bindex::clear();
    }
}
