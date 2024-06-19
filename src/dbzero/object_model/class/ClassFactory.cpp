#include "ClassFactory.hpp"
#include "Class.hpp"
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/core/utils/conversions.hpp>

namespace db0::object_model

{
    
    using namespace db0;
    
    std::array<VClassMap, 4> openClassMaps(const db0::db0_ptr<VClassMap> *class_map_ptrs, Memspace &memspace)
    {
        return {
            class_map_ptrs[0](memspace), 
            class_map_ptrs[1](memspace),
            class_map_ptrs[2](memspace),
            class_map_ptrs[3](memspace),
        };
    }
    
    // 4 spacializations allows constructing the 4 type name variants
    std::optional<std::string> getNameVariant(ClassFactory::TypeObjectPtr lang_type, const char *type_id, int variant_id)
    {
        using LangToolkit = ClassFactory::LangToolkit;
        switch (variant_id) {
            case 0 :
            case 1 : 
            case 2 : {
                return getNameVariant(db0::getOptionalString(type_id), LangToolkit::getTypeName(lang_type),
                    LangToolkit::getModuleName(lang_type), {}, variant_id);
            }
            break;

            case 3 : {
                // return getNameVariant({}, LangToolkit::getTypeName(lang_type), {}, 
                //     db0::python::getTypeFields(lang_class), variant_id);
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

    bool tryFindByKey(const VClassMap &class_map, const char *key, ClassPtr &result)
    {
        auto it = class_map.find(key);
        if (it != class_map.end()) {
            result = it->second();
            return true;
        }
        return false;
    }

    o_class_factory::o_class_factory(Memspace &memspace)
        : m_class_map_ptrs { VClassMap(memspace), VClassMap(memspace), VClassMap(memspace), VClassMap(memspace) }
    {
    }
    
    ClassFactory::ClassFactory(db0::swine_ptr<Fixture> &fixture)
        : super_t(fixture, *fixture)
        , m_class_maps(openClassMaps((*this)->m_class_map_ptrs, getMemspace()))
    {
    }
    
    ClassFactory::ClassFactory(db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
        : super_t(super_t::tag_from_address(), fixture, address)
        , m_class_maps(openClassMaps((*this)->m_class_map_ptrs, getMemspace()))
    {
    }
    
    std::shared_ptr<Class> ClassFactory::tryGetExistingType(TypeObjectPtr lang_type) const
    {
        auto it_cached = m_type_cache.find(lang_type);
        if (it_cached == m_type_cache.end())
        {
            // find type in the type map, use 4 variants of type identification
            auto class_ptr = tryFindClassPtr(lang_type, LangToolkit::getMemoTypeID(lang_type));
            if (!class_ptr) {
                return nullptr;
            }
            // pull existing DBZero class instance by pointer
            std::shared_ptr<Class> type = getTypeByPtr(class_ptr);
            it_cached = m_type_cache.insert({lang_type, type}).first;
            // persist type objects
            m_types.push_back(lang_type);
        }
        return it_cached->second;
    }

    std::shared_ptr<Class> ClassFactory::getExistingType(TypeObjectPtr lang_type) const
    {
        auto type = tryGetExistingType(lang_type);
        if (!type) {
            THROWF(db0::InputException) << "Class not found: " << LangToolkit::getTypeName(lang_type);
        }
        return type;
    }
    
    std::shared_ptr<Class> ClassFactory::getOrCreateType(TypeObjectPtr lang_type)
    {
        auto it_cached = m_type_cache.find(lang_type);
        if (it_cached == m_type_cache.end())
        {
            const char *type_id = LangToolkit::getMemoTypeID(lang_type);
            const char *prefix_name = LangToolkit::getPrefixName(lang_type);
            // find type in the type map, use 4 key variants of type identification
            auto class_ptr = tryFindClassPtr(lang_type, type_id);
            std::shared_ptr<Class> type;
            if (class_ptr) {
                // pull existing DBZero class instance by pointer
                type = getTypeByPtr(class_ptr);
            } else {
                // create new Class instance
                bool is_singleton = LangToolkit::isSingleton(lang_type);
                auto fixture = getFixture();
                ClassFlags flags { is_singleton ? ClassOptions::SINGLETON : 0 };
                type = std::shared_ptr<Class>(new Class(fixture, LangToolkit::getTypeName(lang_type), 
                    LangToolkit::getModuleName(lang_type), lang_type, type_id, prefix_name, flags));
                class_ptr = ClassPtr(*type);
                // inc-ref to persist the class
                type->incRef();
                // register class under all known key variants
                for (unsigned int i = 0; i < 4; ++i) {
                    auto variant_name = getNameVariant(lang_type, type_id, i);
                    if (variant_name) {
                        m_class_maps[i].insert_equal(variant_name->c_str(), class_ptr);
                    }
                }

                // registering type in the by-pointer cache (for accessing by-ClassPtr)                
                type = this->getType(class_ptr, type);
            }

            it_cached = m_type_cache.insert({lang_type, type}).first;
            // persist type objects
            m_types.push_back(lang_type);
        }
        return it_cached->second;
    }
    
    std::shared_ptr<Class> ClassFactory::getType(ClassPtr ptr, std::shared_ptr<Class> type)
    {
        auto it_cached = m_ptr_cache.find(ptr);
        if (it_cached == m_ptr_cache.end()) {
            // add to by-pointer cache
            it_cached = m_ptr_cache.insert({ptr, type}).first;
        }
        return it_cached->second;
    }
    
    ClassPtr ClassFactory::tryFindClassPtr(TypeObjectPtr lang_type, const char *type_id) const
    {
        ClassPtr result;
        for (unsigned int i = 0; i < 4; ++i) {
            auto variant_key = getNameVariant(lang_type, type_id, i);
            if (variant_key) {                
                if (tryFindByKey(m_class_maps[i], variant_key->c_str(), result)) {
                    return result;
                }
                if (i == 0) {
                    // if type_id provided, then ignore all other variants
                    break;                    
                }
            }
        }
        return result;
    }
    
    std::shared_ptr<Class> ClassFactory::getTypeByPtr(ClassPtr ptr) const
    {
        auto it_cached = m_ptr_cache.find(ptr);
        if (it_cached == m_ptr_cache.end())
        {
            // Since ptr points to existing instance, we can simply pull it from backend
            // note that Class has no associated language specific type object yet
            auto fixture = getFixture();
            auto type = std::shared_ptr<Class>(new Class(fixture, ptr.getAddress()));
            it_cached = m_ptr_cache.insert({ptr, type}).first;
        }
        return it_cached->second;
    }
       
    void ClassFactory::commit()
    {
        for (auto &[type, class_ptr]: m_type_cache) {
            class_ptr->commit();
        }
        super_t::commit();
    }
    
}