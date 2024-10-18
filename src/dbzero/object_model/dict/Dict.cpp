
#include "Dict.hpp"
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/bindings/python/AnyObjectAPI.hpp>
#include <dbzero/object_model/value.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/object.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/object_model/value/Member.hpp>


namespace db0::object_model

{

    namespace py = db0::python;
    GC0_Define(Dict)
    
    template <typename LangToolkit> o_typed_item createTypedItem(db0::swine_ptr<Fixture> &fixture,
        typename LangToolkit::ObjectPtr lang_value)
    {
        auto type_id = LangToolkit::getTypeManager().getTypeId(lang_value);
        return { 
            TypeUtils::m_storage_class_mapper.getStorageClass(type_id), 
            createMember<LangToolkit>(fixture, type_id, lang_value) 
        };
    }

    template <typename LangToolkit> dict_item createDictItem(db0::swine_ptr<Fixture> &fixture, std::uint64_t hash,
        o_typed_item key, o_typed_item value)
    {
        DictIndex bindex(*fixture, o_pair_item(key, value) );
        return { hash, bindex };
    }
    
    Dict::Dict(db0::swine_ptr<Fixture> &fixture)
        : super_t(fixture)
        , m_index(*fixture)
    {
        modify().m_index_ptr = m_index.getAddress();
    }
    
    Dict::Dict(db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
        : super_t(super_t::tag_from_address(), fixture, address)
        , m_index(myPtr((*this)->m_index_ptr))
    {
    }

    void Dict::operator=(Dict &&other)
    {
        clear();
        super_t::operator=(std::move(other));
        m_index = std::move(other.m_index);
        assert(!other.hasInstance());
    }
    
    Dict::Dict(db0::swine_ptr<Fixture> &fixture, const Dict &dict)
        : super_t(fixture)
        , m_index(*fixture)
    {
        initWith(dict);
    }

    Dict::Dict(tag_no_gc, db0::swine_ptr<Fixture> &fixture, const Dict &dict)
        : super_t(tag_no_gc(), fixture)
        , m_index(*fixture)
    {
        initWith(dict);
    }

    Dict::~Dict()
    {
        // unregister needs to be called before destruction of members
        unregister();
    }
    
    void Dict::initWith(const Dict &dict)
    {
        modify().m_index_ptr = m_index.getAddress();
        for (auto [hash, address] : dict) {
            auto bindex = address.getIndex(this->getMemspace());
            auto bindex_copy = DictIndex(bindex);
            m_index.insert(dict_item(hash, bindex_copy));            
        }
        modify().m_size = dict.size();
    }
    
    void Dict::setItem(FixtureLock &fixture, std::uint64_t key_hash, ObjectPtr key, ObjectPtr value)
    {
        using TypeId = db0::bindings::TypeId;

        auto key_item = createTypedItem<LangToolkit>(*fixture, key);
        auto value_item = createTypedItem<LangToolkit>(*fixture, value);

        auto it = m_index.find(key_hash);
        if (it == m_index.end()) {
            m_index.insert(createDictItem<LangToolkit>(*fixture, key_hash, key_item, value_item));
            ++modify().m_size;
        } else {
            auto address = (*it).value;
            auto bindex = address.getIndex(**fixture);
            auto it_join = bindex.beginJoin(1);
            while (!it_join.is_end()) {
                auto [storage_class, value] = (*it_join).m_first;
                auto member = unloadMember<LangToolkit>(*fixture, storage_class, value);
                if (LangToolkit::compare(key, member.get())) {
                    bindex.erase(*it_join);
                    unrefMember<LangToolkit>(*fixture, storage_class, value);
                    --modify().m_size;
                    break;
                }
                ++it_join;
            }
            ++modify().m_size;
            bindex.insert(o_pair_item(key_item, value_item));
            auto new_address = bindex.getAddress();
            if (new_address != address.m_index_address) {
                it.modifyItem().value.m_index_address = new_address;
                it.modifyItem().value.m_type = bindex.getIndexType();
            }
        }
    }
    
    Dict::ObjectSharedPtr Dict::getItem(std::uint64_t key_hash, ObjectPtr key_value) const
    {
        auto fixture = this->getFixture();
        auto iter = m_index.find(key_hash);        
        if (iter != m_index.end()) {
            auto [key, address] = *iter;            
            auto bindex = address.getIndex(*fixture);
            auto it = bindex.beginJoin(1);
            while (!it.is_end()) {
                auto [storage_class, value] = (*it).m_first;
                auto member = unloadMember<LangToolkit>(fixture, storage_class, value);                
                if (LangToolkit::compare(key_value, member.get())) {
                    auto [storage_class, value] = (*it).m_second;
                    return unloadMember<LangToolkit>(fixture, storage_class, value);
                }
                ++it;
            }
        }
        return {};
    }
    
    Dict *Dict::makeNew(void *at_ptr, db0::swine_ptr<Fixture> &fixture) {
        return new (at_ptr) Dict(fixture);
    }
    
    Dict *Dict::unload(void *at_ptr, db0::swine_ptr<Fixture> &fixture, std::uint64_t address) {
        return new (at_ptr) Dict(fixture, address);
    }
    
    bool Dict::has_item(ObjectPtr obj) const
    {   
        // FIXME: this API should NOT be used directly here
        auto item = getItem(py::AnyObject_Hash(obj), obj);
        return item != nullptr;
    }

    Dict *Dict::copy(void *at_ptr, db0::swine_ptr<Fixture> &fixture) const {
        return new (at_ptr) Dict(fixture, *this);
    }
    
    Dict::ObjectSharedPtr Dict::pop(ObjectPtr obj)
    {
        auto hash = py::AnyObject_Hash(obj);
        auto iter = m_index.find(hash);
        if (iter == m_index.end()) {
            return nullptr;
        }
        auto [key, address] = *iter;
        auto bindex = address.getIndex(this->getMemspace());
        auto it = bindex.beginJoin(1);
        auto [storage_class, value] = (*it).m_second;
        auto fixture = this->getFixture();
        auto member = unloadMember<LangToolkit>(fixture, storage_class, value);
        if (bindex.size() == 1) {
            m_index.erase(iter);
            bindex.destroy();
            auto [key_storage_class, key_value] = (*it).m_first;
            unrefMember<LangToolkit>(fixture, key_storage_class, key_value);
        } else {
            bindex.erase(*it);
        }
        --modify().m_size;
        unrefMember<LangToolkit>(fixture, storage_class, value);
        return member;
    }

    void Dict::moveTo(db0::swine_ptr<Fixture> &fixture)
    {
        if (this->size() > 0) {
            THROWF(db0::InputException) << "Dict with items cannot be moved to another fixture";
        }
        assert(hasInstance());
        super_t::moveTo(fixture);
    }

    std::size_t Dict::size() const {
        return (*this)->m_size;
    }

    void Dict::clear()
    {
        unrefMembers();
        m_index.clear();
        modify().m_size = 0; 
    }
    
    void Dict::commit() const
    {
        m_index.commit();
        super_t::commit();        
    }
    
    void Dict::detach() const
    {
        // FIXME: can be removed when GC0 calls commit-op
        commit();
        m_index.detach();
        super_t::detach();
    }
    
    Dict::const_iterator Dict::begin() const {
        return m_index.begin();
    }

    Dict::const_iterator Dict::end() const {
        return m_index.end();
    }
    
    void Dict::destroy() const
    {
        unrefMembers();
        m_index.destroy();
        super_t::destroy();
    }
    
    void Dict::unrefMembers() const
    {
        auto fixture = this->getFixture();
        for (auto [_, address] : m_index) {
            auto bindex = address.getIndex(this->getMemspace());
            auto it = bindex.beginJoin(1);
            for (; !it.is_end(); ++it) {
                auto [storage_class_1, value_1] = (*it).m_first;
                unrefMember<LangToolkit>(fixture, storage_class_1, value_1);
                auto [storage_class_2, value_2] = (*it).m_second;
                unrefMember<LangToolkit>(fixture, storage_class_2, value_2);
            }
        }
    }

}
