// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "EnumFactory.hpp"
#include "Enum.hpp"
#include "EnumValue.hpp"
#include <dbzero/core/utils/conversions.hpp>
#include <dbzero/workspace/Snapshot.hpp>
#include <dbzero/workspace/Workspace.hpp>

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
                    enum_def.m_module_name, enum_def.m_hash, variant_id);
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
    
    EnumFactory::EnumFactory(db0::swine_ptr<Fixture> &fixture, Address address)
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
    
    EnumPtr EnumFactory::tryFindEnumPtr(const EnumDef &enum_def) const
    {
        EnumPtr result;
        auto type_id = enum_def.tryGetTypeId();
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
    
    std::shared_ptr<Enum> EnumFactory::tryGetOrCreateEnum(const EnumTypeDef &enum_type_def) {
        return tryGetOrCreateEnum(enum_type_def.m_enum_def);
    }
    
    std::shared_ptr<Enum> EnumFactory::tryGetOrCreateEnum(const EnumFullDef &enum_def)
    {
        auto ptr = tryFindEnumPtr(enum_def);
        if (ptr) {
            return getEnumByPtr(ptr);
        }
                
        // create new Enum instance (fixture must be accessible for write)
        auto fixture = getFixture();
        if (fixture->getAccessType() != AccessType::READ_WRITE) {
            // unable to create enum due to insufficient access rights
            return nullptr;
        }
        
        auto type_id = enum_def.tryGetTypeId();
        auto enum_ = std::shared_ptr<Enum>(
            new Enum(fixture, enum_def.m_name, enum_def.m_module_name, enum_def.m_values, type_id));                
        auto enum_ptr = EnumPtr(*enum_);
        
        // inc-ref to persist the Enum
        enum_->incRef(false);
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

    std::shared_ptr<Enum> EnumFactory::getOrCreateEnum(const EnumFullDef &enum_def)
    {
        auto enum_ = tryGetOrCreateEnum(enum_def);
        if (!enum_) {
            auto type_id = enum_def.tryGetTypeId();
            auto prefix_name = this->getFixture()->getPrefix().getName();
            THROWF(db0::InputException) << "Unable to create Enum: " << enum_def << ", type_id: " 
                << (type_id != nullptr ? type_id : "null") << " in " << prefix_name;
        }
        return enum_;
    }
    
    std::shared_ptr<Enum> EnumFactory::getOrCreateEnum(const EnumTypeDef &enum_type_def)
    {        
        auto enum_ = tryGetOrCreateEnum(enum_type_def);
        if (!enum_) {
            THROWF(db0::InputException) << "Unable to create Enum: " << enum_type_def;
        }
        return enum_;
    }
    
    std::shared_ptr<Enum> EnumFactory::getExistingEnum(const EnumTypeDef &enum_type_def) const
    {        
        auto enum_ = tryGetExistingEnum(enum_type_def);
        if (!enum_) {
            THROWF(db0::InputException) << "Enum not found: " << enum_type_def;
        }
        return enum_;
    }

    std::shared_ptr<Enum> EnumFactory::getExistingEnum(const EnumDef &enum_def, const char *prefix_name) const
    {
        auto enum_ = tryGetExistingEnum(enum_def, prefix_name);
        if (!enum_) {
            THROWF(db0::InputException) << "Enum not found: " << enum_def;
        }
        return enum_;
    }
    
    std::shared_ptr<Enum> EnumFactory::tryGetExistingEnum(const EnumTypeDef &enum_type_def) const
    {                
        // FIXME: handle scoped enums (with prefix defined)
        auto ptr = tryFindEnumPtr(enum_type_def.m_enum_def);
        if (!ptr) {
            return nullptr;
        }
        return getEnumByPtr(ptr);
    }
    
    std::shared_ptr<Enum> EnumFactory::tryGetExistingEnum(const EnumDef &enum_def, const char *) const
    {
        // FIXME: handle scoped enums (with prefix defined)
        auto ptr = tryFindEnumPtr(enum_def);
        if (!ptr) {
            return nullptr;
        }
        return getEnumByPtr(ptr);
    }

    std::shared_ptr<Enum> EnumFactory::getEnumByPtr(EnumPtr ptr) const
    {
        auto it_cached = m_ptr_cache.find(ptr);
        if (it_cached == m_ptr_cache.end()) {
            auto fixture = getFixture();
            // pull existing dbzero Enum instance by pointer
            auto enum_ = std::shared_ptr<Enum>(new Enum(fixture, ptr.getAddress()));
            it_cached = m_ptr_cache.insert({ptr, enum_}).first;
        }
        return it_cached->second;
    }
    
    std::shared_ptr<Enum> EnumFactory::getEnumByUID(std::uint32_t enum_uid) const
    {
        // convert enum_uid to EnumPtr
        auto enum_ptr = db0::db0_ptr_reinterpret_cast<Enum>()(
            Address::fromOffset(getFixture()->makeAbsolute(enum_uid, Enum::SLOT_NUM))
        );
        return getEnumByPtr(enum_ptr);
    }
    
    bool EnumFactory::isMigrateRequired(const EnumValue &other) const {
        assert(other);
        return !db0::is_same(other.m_fixture, this->getFixture());
    }
    
    std::shared_ptr<Enum> EnumFactory::tryGetMigratedEnum(const EnumValue &other)
    {        
        assert(other);
        assert(!db0::is_same(other.m_fixture, this->getFixture()));
        auto &other_factory = other.m_fixture.safe_lock()->get<EnumFactory>();
        auto other_enum = other_factory.getEnumByUID(other.m_enum_uid);
        auto type_id = other_enum->getTypeID();
        // FIXME: optimization
        // getEnumDef can be avoided if definition already exists in the destination fixture
        return this->tryGetOrCreateEnum(other_enum->getEnumDef());
    }
    
    std::shared_ptr<Enum> EnumFactory::getMigratedEnum(const EnumValue &other)
    {
        auto enum_ = tryGetMigratedEnum(other);
        if (!enum_) {
            THROWF(db0::InputException) << "Unable to migrate EnumValue: " << other << " to prefix: " 
                << getFixture()->getPrefix().getName();
        }
        return enum_;
    }
    
    std::optional<EnumValue> EnumFactory::tryMigrateEnumValue(const EnumValue &other)
    {
        if (db0::is_same(other.m_fixture, this->getFixture())) {
            // no translation required
            return other;
        }

        auto enum_ = tryGetMigratedEnum(other);    
        if (!enum_) {
            return std::nullopt;
        }
        // must resolve enum value by name since UIDs dont match across prefixes
        return enum_->get(other.m_str_repr.c_str());
    }

    EnumValue EnumFactory::migrateEnumValue(const EnumValue &other)
    {
        auto enum_value = tryMigrateEnumValue(other);
        if (!enum_value) {
            THROWF(db0::InputException) << "Unable to migrate EnumValue: " << other << " to prefix: " 
                << getFixture()->getPrefix().getName();
        }
        return *enum_value;
    }
    
    EnumFactory::ObjectSharedPtr EnumFactory::tryMigrateEnumLangValue(const EnumValue &other)
    {
        assert(other);
        if (db0::is_same(other.m_fixture, getFixture())) {
            // retrieve from this factory
            return getEnumByUID(other.m_enum_uid)->getLangValue(other);
        }
                
        auto enum_ = tryGetMigratedEnum(other);
        if (!enum_) {
            return nullptr;
        }
        // must resolve enum value by name since UIDs dont match across prefixes
        return enum_->getLangValue(other.m_str_repr.c_str());
    }

    EnumFactory::ObjectSharedPtr EnumFactory::migrateEnumLangValue(const EnumValue &other)
    {
        auto lang_value = tryMigrateEnumLangValue(other);
        if (!lang_value) {
            THROWF(db0::InputException) << "Unable to migrate EnumValue: " << other << " to prefix: " 
                << getFixture()->getPrefix().getName();
        }
        return lang_value;
    }
    
    void EnumFactory::commit() const
    {
        for (auto &enum_map: m_enum_maps) {
            enum_map.commit();
        }
        for (auto &item: m_ptr_cache) {
            item.second->commit();
        }
        super_t::commit();
    }

    void EnumFactory::detach() const
    {
        for (auto &enum_map: m_enum_maps) {
            enum_map.detach();
        }
        for (auto &item: m_ptr_cache) {
            item.second->detach();
        }
        super_t::detach();
    }
    
    std::optional<EnumValue> EnumFactory::tryGetEnumValue(const EnumValueRepr &enum_value_repr)
    {
        const auto &enum_type_def = enum_value_repr.getEnumTypeDef();
        auto enum_ = this->tryGetOrCreateEnum(enum_type_def);
        if (!enum_) {
            return std::nullopt;
        }
        
        // resolve by text representation (since UIDs are not compatible across fixtures)
        return enum_->get(enum_value_repr.m_str_repr.c_str());
    }
    
    EnumValue EnumFactory::getEnumValue(const EnumValueRepr &enum_value_repr)
    {
        auto enum_value = tryGetEnumValue(enum_value_repr);
        if (!enum_value) {
            THROWF(db0::InputException) << "Unable to get EnumValue of: " << enum_value_repr.m_str_repr;            
        }
        return *enum_value;
    }
    
    EnumFactory::ObjectSharedPtr EnumFactory::tryGetEnumLangValue(const EnumValueRepr &enum_value_repr)
    {
        const auto &enum_type_def = enum_value_repr.getEnumTypeDef();
        auto enum_ = this->tryGetOrCreateEnum(enum_type_def);
        if (!enum_) {
            return nullptr;
        }
        
        // resolve by text representation (since UIDs are not compatible across fixtures)
        return enum_->getLangValue(enum_value_repr.m_str_repr.c_str());
    }

}