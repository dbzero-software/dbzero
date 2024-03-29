#include "Class.hpp"
#include <dbzero/core/utils/uuid.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include "ClassFactory.hpp"
#include <dbzero/object_model/object/Object.hpp>

DEFINE_ENUM_VALUES(db0::ClassOptions, "SINGLETON")

namespace db0::object_model

{
    
    using namespace db0;
    using namespace db0::pools;

    GC0_Define(Class)
    
    o_class::o_class(RC_LimitedStringPool &string_pool, const std::string &name, const std::string &module_name,
        const VFieldVector &members, const char *type_id, ClassFlags flags)
        : m_uuid(db0::make_UUID())
        , m_name(string_pool.add(name))
        , m_module_name(string_pool.add(module_name))
        , m_type_id(type_id ? string_pool.add(type_id) : LP_String())
        , m_members_ptr(members)        
        , m_flags(flags)
    {
    }
    
    Class::Member::Member(const char *name, StorageClass storage_class, std::shared_ptr<Class> type)
        : m_name(name)
        , m_storage_class(storage_class)
        , m_type(type)
    {
    }
    
    Class::Member::Member(const std::string &name, StorageClass storage_class, std::shared_ptr<Class> type)
        : m_name(name)
        , m_storage_class(storage_class)
        , m_type(type)
    {
    }
    
    Class::Class(db0::swine_ptr<Fixture> &fixture, const std::string &name, const std::string &module_name, TypeObjectPtr lang_type_ptr,
        const char *type_id, ClassFlags flags)
        : super_t(fixture, fixture->getLimitedStringPool(), name, module_name, VFieldVector(*fixture), type_id, flags)
        , m_members(myPtr((*this)->m_members_ptr.getAddress()))        
        , m_lang_type_ptr(lang_type_ptr)
    {
    }
    
    Class::Class(db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
        // class instances are not garbage collected (no drop function provided) since they're managed by the ClassFactory
        : super_t(super_t::tag_from_address(), fixture, address)
        , m_members(myPtr((*this)->m_members_ptr.getAddress()))
    {
        // fetch all members into cache
        refreshMemberCache();
    }
    
    std::string Class::getName() const
    {
        return getFixture()->getLimitedStringPool().fetch((*this)->m_name);
    }   
    
    std::optional<std::string> Class::getTypeId() const
    {
        if (!(*this)->m_type_id) {
            return std::nullopt;
        }
        return getFixture()->getLimitedStringPool().fetch((*this)->m_type_id);
    }
    
    std::uint32_t Class::addField(const char *name, StorageClass storage_class, std::shared_ptr<Class> type)
    {
        if (storage_class == StorageClass::OBJECT_REF && !type) {
            THROWF(db0::InternalException) << "DBZero object reference missing a type";
        }
        auto field_id = m_members.size();
        m_members.emplace_back(getFixture()->getLimitedStringPool(), name, storage_class, (type?ClassPtr(*type):ClassPtr()));
        m_member_cache.emplace_back(name, storage_class, type);
        m_index[name] = field_id;
        return field_id;
    }
    
    std::uint32_t Class::findField(const char *name) const
    {
        auto it = m_index.find(name);
        if (it == m_index.end()) {            
            // try refreshing the cache
            refreshMemberCache();
            it = m_index.find(name);
            if (it == m_index.end()) {
                // field not found
                return Class::NField;
            }
        }

        return it->second;
    }

    const Class::Member &Class::get(std::uint32_t index) const
    {
        if (index < m_member_cache.size()) {
            return m_member_cache[index];
        }

        if (index >= m_member_cache.size()) {
            // try updating cache (fields might've been added by other process)
            refreshMemberCache();            
        }
        if (index >= m_member_cache.size()) {
            THROWF(db0::InternalException) << "Member slot not found: " << index;
        }
        return m_member_cache[index];
    }

    const Class::Member &Class::get(const char *name) const
    {
        auto it = m_index.find(name);
        if (it == m_index.end()) {
            THROWF(db0::InputException) << "Field " << name << " not found in class " << getName();
        }
        return get(it->second);    
    }
    
    bool Class::unloadSingleton(void *at) const
    {
        if (!(*this)->m_singleton_address) {
            return false;
        }
        
        auto fixture = getFixture();
        auto &class_factory = fixture->get<ClassFactory>();
        auto stem = Object::unloadStem(fixture, (*this)->m_singleton_address);
        auto type = db0::object_model::unloadClass(stem->m_class_ref, class_factory);
        // unload from stem
        Object::unload(at, std::move(stem), type);
        return true;
    }
    
    bool Class::isSingleton() const
    {
        return (*this)->m_flags[ClassOptions::SINGLETON];
    }
    
    bool Class::isExistingSingleton() const
    {
        return isSingleton() && (*this)->m_singleton_address;
    }
    
    void Class::setSingletonAddress(Object &object)
    {
        assert(!(*this)->m_singleton_address);
        assert(isSingleton());
        // increment reference count in order to prevent singleton object from being destroyed
        object.incRef();
        modify().m_singleton_address = object.getAddress();
    }
    
    void Class::refreshMemberCache() const
    {
        assert(m_members.size() >= m_member_cache.size());
        if (m_members.size() == m_member_cache.size()) {
            return;
        }

        // fetch all members into cache
        unsigned int index = m_member_cache.size();
        auto &string_pool = getFixture()->getLimitedStringPool();
        for (auto it = m_members.begin(index), end = m_members.end(); it != end; ++it, ++index) {
            m_member_cache.emplace_back(string_pool.fetch(it->m_name), it->m_storage_class);
            m_index[m_member_cache.back().m_name] = index;            
        }
    }
        
    std::string Class::getTypeName() const
    {
        return getFixture()->getLimitedStringPool().fetch((*this)->m_name);
    }

    std::string Class::getModuleName() const
    {
        return getFixture()->getLimitedStringPool().fetch((*this)->m_module_name);
    }

    Class::TypeObjectSharedPtr Class::getLangClass() const
    {
        if (!m_lang_type_ptr) {
            auto &type_manager = LangToolkit::getTypeManager();
            // try finding lang class by one of 4 type name variants
            for (unsigned int i = 0; i < 4; ++i) {
                auto variant_name = getNameVariant(*this, i);
                if (variant_name) {
                    m_lang_type_ptr = type_manager.findType(*variant_name);
                    if (m_lang_type_ptr) {
                        break;
                    }
                }
            }

            if (!m_lang_type_ptr) {
                THROWF(db0::InternalException) << "Language class not found for DBZero class " << getTypeName();
            }
        }
        return m_lang_type_ptr;
    }
    
    std::optional<std::string> getNameVariant(std::optional<std::string> type_id, std::optional<std::string> type_name,
        std::optional<std::string> module_name, std::optional<std::string> type_fields_str, int variant_id)
    {
        switch (variant_id) {
            case 0: {                
                if (type_id) {
                    return type_id;
                }
                return std::nullopt;
            }
            break;

            case 1: {
                // type & module name are required
                assert(type_name && module_name);
                std::stringstream _str;
                _str << "cls:" << *type_name << ".pkg:" << *module_name;
                return _str.str();                
            }
            break;

            case 2: {
                // variant 2. name + fields
                // std::stringstream _str;
                // _str << "cls:" << _class.getTypeName() << "." << db0::python::getTypeFields(lang_class);
                // return _str.str();
            }
            break;

            case 3: {
                // variant 3. module + fields
                // std::stringstream _str;
                // _str << "pkg:" << _class.getModuleName() << "." << db0::python::getTypeFields(lang_class);
                // return _str.str();
            }
            break;

            default: {
                assert(false);
                THROWF(db0::InputException) << "Invalid type name variant id: " << variant_id;
            }
            break;
        }
        return std::nullopt;
    }

    std::optional<std::string> getNameVariant(const Class &_class, int variant_id)
    {
        // FIXME: implement get type fields
        return getNameVariant(_class.getTypeId(), _class.getTypeName(), _class.getModuleName(), std::nullopt, variant_id);
    }

    void Class::renameField(const char *from_name, const char *to_name)
    {
        assert(from_name);
        assert(to_name);
        if (to_name == from_name) {
            return;
        }

        auto field_id = findField(from_name);
        if (field_id == Class::NField) {
            // do not raise exception if the "to_name" field already exists (likely double rename attemp)
            if (findField(to_name) != Class::NField) {
                return;
            }
            THROWF(db0::InputException) << "Field " << from_name << " not found in class " << getName();
        }

        // 1. update in fields vector
        auto &string_pool = getFixture()->getLimitedStringPool();
        // FIXME: unreference old name in the string pool
        m_members.modifyItem(field_id).m_name = string_pool.add(to_name);
        // 2. update in member's cache
        refreshMemberCache();
        m_member_cache[field_id].m_name = to_name;
        // 3. update in the in-memory index
        m_index.erase(from_name);
        m_index[to_name] = field_id;
    }
    
    void Class::detach()
    {
        m_members.detach();
        super_t::detach();
    }
            
    void Class::unlinkSingleton()
    {
        modify().m_singleton_address = 0;
    }
    
    void Class::commit()
    {
        m_members.commit();        
        super_t::commit();
    }

    bool Class::operator!=(const Class &rhs) const
    {
        if (*this->getFixture() != *rhs.getFixture()) {
            return true;
        }
        return this->getAddress() != rhs.getAddress();
    }

}