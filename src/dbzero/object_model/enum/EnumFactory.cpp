#include "EnumFactory.hpp"
#include "Enum.hpp"
#include "EnumValue.hpp"
#include <dbzero/core/utils/conversions.hpp>

namespace db0::object_model

{

    using namespace db0;
    
    std::array<VEnumMap, 4> openEnumMaps(const db0::db0_ptr<VEnumMap> *enum_map_ptrs, Memspace &memspace)
    {
        return {
            enum_map_ptrs[0](memspace), 
            enum_map_ptrs[1](memspace),
            enum_map_ptrs[2](memspace),
            enum_map_ptrs[3](memspace)
        };
    }
    
    std::optional<std::string> getEnumKeyVariant(const EnumDef &enum_def, const char *type_id, int variant_id)
    {
        using LangToolkit = EnumFactory::LangToolkit;
        switch (variant_id) {
            case 0 :
            case 1 : 
            case 2 : {
                return getEnumKeyVariant(db0::getOptionalString(type_id), enum_def.m_name,
                    enum_def.m_module_name, enum_def.m_values, variant_id);
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

    bool tryFindByKey(const VEnumMap &enum_map, const char *key, EnumPtr &result)
    {
        auto it = enum_map.find(key);
        if (it != enum_map.end()) {
            result = it->second();
            return true;
        }
        return false;
    }

    o_enum_factory::o_enum_factory(Memspace &memspace)        
        : m_enum_map_ptrs { VEnumMap(memspace), VEnumMap(memspace), VEnumMap(memspace), VEnumMap(memspace) }
    {
    }
    
    EnumFactory::EnumFactory(db0::swine_ptr<Fixture> &fixture)
        : super_t(fixture, *fixture)        
        , m_enum_maps(openEnumMaps((*this)->m_enum_map_ptrs, getMemspace()))
    {
    }
    
    EnumFactory::EnumFactory(db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
        : super_t(super_t::tag_from_address(), fixture, address)
        , m_enum_maps(openEnumMaps((*this)->m_enum_map_ptrs, getMemspace()))
    {
    }

    std::shared_ptr<Enum> EnumFactory::getEnum(EnumPtr ptr, std::shared_ptr<Enum> enum_)
    {
        auto it_cached = m_ptr_cache.find(ptr);
        if (it_cached == m_ptr_cache.end()) {
            // add to by-pointer cache
            it_cached = m_ptr_cache.insert({ptr, enum_}).first;
        }
        return it_cached->second;
    }

    EnumPtr EnumFactory::tryFindEnumPtr(const EnumDef &enum_def, const char *type_id) const
    {
        EnumPtr result;
        for (unsigned int i = 0; i < 4; ++i) {
            auto variant_key = getEnumKeyVariant(enum_def, type_id, i);
            if (variant_key) {
                if (tryFindByKey(m_enum_maps[i], variant_key->c_str(), result)) {
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

    std::shared_ptr<Enum> EnumFactory::getOrCreateEnum(const EnumDef &enum_def, const char *type_id)
    {
        auto ptr = tryFindEnumPtr(enum_def, type_id);
        if (ptr) {
            return getEnumByPtr(ptr);
        }
        
        // create new Enum instance
        auto fixture = getFixture();
        auto enum_ = std::shared_ptr<Enum>(new Enum(fixture, enum_def.m_name, enum_def.m_values, type_id));
        auto enum_ptr = EnumPtr(*enum_);
        // inc-ref to persist the Enum
        enum_->incRef();
        // register Enum under all known key variants
        for (unsigned int i = 0; i < 4; ++i) {
            auto variant_name = getEnumKeyVariant(enum_def, type_id, i);
            if (variant_name) {
                m_enum_maps[i].insert_equal(variant_name->c_str(), enum_ptr);
            }
        }

        // registering enum in the by-pointer cache (for accessing by-EnumPtr)
        return this->getEnum(enum_ptr, enum_);
    }

    std::shared_ptr<Enum> EnumFactory::getExistingEnum(const EnumDef &enum_def, const char *type_id) const
    {
        auto enum_ = tryGetExistingEnum(enum_def, type_id);
        if (!enum_) {
            THROWF(db0::InputException) << "Enum not found: " << enum_def.m_name;
        }
        return enum_;
    }

    std::shared_ptr<Enum> EnumFactory::tryGetExistingEnum(const EnumDef &enum_def, const char *type_id) const
    {
        auto ptr = tryFindEnumPtr(enum_def, type_id);
        if (!ptr) {
            return nullptr;
        }
        return getEnumByPtr(ptr);
    }
    
    std::shared_ptr<Enum> EnumFactory::getEnumByPtr(EnumPtr ptr) const
    {
        auto it_cached = m_ptr_cache.find(ptr);
        if (it_cached == m_ptr_cache.end())
        {
            auto fixture = getFixture();
            // pull existing DBZero Enum instance by pointer
            auto enum_ = std::shared_ptr<Enum>(new Enum(fixture, ptr.getAddress()));
            it_cached = m_ptr_cache.insert({ptr, enum_}).first;
        }
        return it_cached->second;
    }
    
    std::shared_ptr<Enum> EnumFactory::getEnumByUID(std::uint32_t enum_uid) const
    {
        // convert enum_uid to EnumPtr
        auto enum_ptr = db0::db0_ptr_reinterpret_cast<Enum>()(getFixture()->makeAbsolute(enum_uid, Enum::SLOT_NUM));
        return getEnumByPtr(enum_ptr);
    }
    
}