// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "ClassFactory.hpp"
#include "Class.hpp"
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/core/utils/conversions.hpp>
#include <dbzero/workspace/Snapshot.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/object_model/value/ObjectId.hpp>

namespace db0::object_model

{
    
    using namespace db0;

    ClassFactory &getClassFactory(Fixture &fixture) {
        return fixture.get<ClassFactory>();
    }
    
    const ClassFactory &getClassFactory(const Fixture &fixture) {
        return fixture.get<ClassFactory>();
    }

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
                    LangToolkit::tryGetModuleName(lang_type), {}, variant_id);
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
        , m_class_ptr_index(getMemspace())
        , m_type_slot_addr_range(getTypeSlotAddrRange(*fixture))
    {
        modify().m_class_ptr_index_ptr = m_class_ptr_index;
    }
    
    ClassFactory::ClassFactory(db0::swine_ptr<Fixture> &fixture, Address address)
        : super_t(super_t::tag_from_address(), fixture, address)
        , m_class_maps(openClassMaps((*this)->m_class_map_ptrs, getMemspace()))
        , m_class_ptr_index((*this)->m_class_ptr_index_ptr(getMemspace()))
        , m_type_slot_addr_range(getTypeSlotAddrRange(*fixture))
    {
    }
    
    ClassFactory::~ClassFactory()
    {
    }

    void ClassFactory::initWith(const ClassFactory &other)
    {
        assert(m_type_cache.empty());
        assert(m_ptr_cache.empty());
        auto fixture = this->getFixture();
        for (auto [lang_type, type]: other.m_type_cache) {
            // validate if type exists in the snapshot
            if (exists(*type)) {
                getTypeByPtr(ClassPtr(*type), lang_type);
            }
        }
    }
    
    std::shared_ptr<Class> ClassFactory::tryGetExistingType(TypeObjectPtr lang_type) const
    {
        auto it_cached = m_type_cache.find(lang_type);
        if (it_cached == m_type_cache.end()) {
            // find type in the type map, use 4 variants of type identification
            auto class_ptr = tryFindClassPtr(lang_type, LangToolkit::getMemoTypeID(lang_type));
            if (!class_ptr) {
                return nullptr;
            }
            // pull existing dbzero class instance by pointer
            std::shared_ptr<Class> type = getTypeByPtr(class_ptr, lang_type).m_class;
            // add to by-type cache
            it_cached = m_type_cache.insert({lang_type, type}).first;
            m_pending_types.push_back(lang_type);
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
    
    std::shared_ptr<Class> ClassFactory::tryGetOrCreateType(TypeObjectPtr lang_type)
    {
        // disallow creating MemoBase type
        if (LangToolkit::getTypeManager().isMemoBase(lang_type)) {
            THROWF(db0::InputException) << "Cannot create MemoBase type";
        }

        auto it_cached = m_type_cache.find(lang_type);
        if (it_cached == m_type_cache.end()) {
            const char *type_id = LangToolkit::getMemoTypeID(lang_type);
            const char *prefix_name = LangToolkit::getPrefixName(lang_type);
            const auto &init_vars = LangToolkit::getInitVars(lang_type);
            // find type in the type map, use 4 key variants of type identification
            auto class_ptr = tryFindClassPtr(lang_type, type_id);
            std::shared_ptr<Class> type;
            if (class_ptr) {
                // pull existing dbzero class instance by pointer
                type = getTypeByPtr(class_ptr, lang_type).m_class;
            } else {
                auto fixture = getFixture();
                if (!checkAccessType(*fixture, AccessType::READ_WRITE)) {
                    return {};
                }
                
                // create new Class instance
                bool is_singleton = LangToolkit::isSingleton(lang_type);
                ClassFlags flags;
                if (is_singleton) {
                    flags += ClassOptions::SINGLETON;
                }                
                flags.set(ClassOptions::NO_DEFAULT_TAGS, LangToolkit::isNoDefaultTags(lang_type));
                flags.set(ClassOptions::IMMUTABLE, LangToolkit::isImmutable(lang_type));
                auto memo_base = LangToolkit::getBaseMemoType(lang_type);
                std::shared_ptr<Class> base_class;                
                if (memo_base) {
                    base_class = getOrCreateType(memo_base);                    
                }
                type = std::shared_ptr<Class>(new Class(fixture, LangToolkit::getTypeName(lang_type),
                    LangToolkit::tryGetModuleName(lang_type), type_id, prefix_name, init_vars, flags, base_class)
                );
                class_ptr = ClassPtr(*type);
                // inc-ref to persist the class
                type->incRef(false);
                // register class under all known key variants
                for (unsigned int i = 0; i < 4; ++i) {
                    auto variant_name = getNameVariant(lang_type, type_id, i);
                    if (variant_name) {
                        m_class_maps[i].insert_equal(variant_name->c_str(), class_ptr);
                    }
                }
                // and register its address with the class pointer index
                m_class_ptr_index.insert(class_ptr);
                // registering type in the by-pointer cache (for accessing by-ClassPtr)                
                type = this->getType(class_ptr, type, lang_type);
                if (lang_type) {       
                    type->setRuntimeFlags(LangToolkit::getMemoFlags(lang_type));
                }
            }
            
            it_cached = m_type_cache.insert({lang_type, type}).first;
            m_pending_types.push_back(lang_type);
        }
        return it_cached->second;
    }
    
    std::shared_ptr<Class> ClassFactory::getOrCreateType(TypeObjectPtr lang_type)
    {
        auto result = tryGetOrCreateType(lang_type);
        if (!result) {
            auto fixture = getFixture();
            // this is to raise a proper exception if access is denied
            assureAccessType(*fixture, AccessType::READ_WRITE);
            // throw internal exception in other cases
            THROWF(db0::InternalException) 
                << "Cannot create class: " << LangToolkit::getTypeName(lang_type);
        }

        return result;
    }
    
    std::shared_ptr<Class> ClassFactory::getType(ClassPtr ptr, std::shared_ptr<Class> type, TypeObjectPtr lang_type) const
    {
        auto it_cached = m_ptr_cache.find(ptr);
        if (it_cached == m_ptr_cache.end()) {
            // try looking-up language specific type with the TypeManager
            if (!lang_type) {
                lang_type = tryFindLangType(*type);
            }
            // add to by-pointer cache
            it_cached = m_ptr_cache.insert({ptr, ClassItem{ type, lang_type }}).first;
            m_pending_ptrs.push_back(ptr);
        }
        if (lang_type && !it_cached->second.m_lang_type) {
            it_cached->second.m_lang_type = lang_type;
            it_cached->second.m_class->setInitVars(LangToolkit::getInitVars(lang_type));
            it_cached->second.m_class->setRuntimeFlags(LangToolkit::getMemoFlags(lang_type));
        }
        return it_cached->second.m_class;
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
    
    ClassFactory::ClassItem ClassFactory::getTypeByClassRef(std::uint32_t class_ref,
        TypeObjectPtr lang_type) const 
    {
        return getTypeByPtr(db0::db0_ptr_reinterpret_cast<Class>()(
            classRefToAddress(class_ref, m_type_slot_addr_range)), lang_type
        );
    }

    ClassFactory::ClassItem ClassFactory::getTypeByAddr(Address addr, TypeObjectPtr lang_type) const {
        return getTypeByPtr(db0::db0_ptr_reinterpret_cast<Class>()(addr), lang_type);
    }

    ClassFactory::ClassItem ClassFactory::tryGetTypeByClassRef(std::uint32_t class_ref,
        TypeObjectPtr lang_type) const 
    {
        return tryGetTypeByPtr(db0::db0_ptr_reinterpret_cast<Class>()(
            classRefToAddress(class_ref, m_type_slot_addr_range)), lang_type
        );
    }

    ClassFactory::ClassItem ClassFactory::tryGetTypeByAddr(Address addr, TypeObjectPtr lang_type) const {
        return tryGetTypeByPtr(db0::db0_ptr_reinterpret_cast<Class>()(addr), lang_type);
    }

    ClassFactory::ClassItem ClassFactory::tryGetTypeByPtr(ClassPtr ptr, TypeObjectPtr lang_type) const
    {
        auto it_cached = m_ptr_cache.find(ptr);
        if (it_cached == m_ptr_cache.end()) {
            // Since ptr points to existing instance, we can simply pull it from backend
            // note that Class has no associated language specific type object yet
            auto fixture = getFixture();
            if (!Class::checkUnload(fixture, ptr.getAddress())) {
                return {};
            }
            auto type = std::shared_ptr<Class>(new Class(fixture, ptr.getAddress()));
            // try looking-up language specific type with the TypeManager
            if (!lang_type) {
                lang_type = tryFindLangType(*type);
            }
            // initialize the language model
            if (lang_type) {
                type->setInitVars(LangToolkit::getInitVars(lang_type));
                type->setRuntimeFlags(LangToolkit::getMemoFlags(lang_type));
            }
            // register the mapping to language specific type object
            it_cached = m_ptr_cache.insert({ptr, ClassItem { type, lang_type }}).first;
            m_pending_ptrs.push_back(ptr);
        }
        // register the lang type mapping if missing
        if (lang_type && !it_cached->second.m_lang_type) {
            it_cached->second.m_lang_type = lang_type;        
            it_cached->second.m_class->setInitVars(LangToolkit::getInitVars(lang_type));
            it_cached->second.m_class->setRuntimeFlags(LangToolkit::getMemoFlags(lang_type));
        }
        return it_cached->second;
    }
    
    ClassFactory::ClassItem ClassFactory::getTypeByPtr(ClassPtr ptr, TypeObjectPtr lang_type) const
    {
        auto result = tryGetTypeByPtr(ptr, lang_type);
        if (!result) {
            THROWF(db0::InputException) << "Class not found: " << ptr.getAddress();
        }
        return result;
    }

    void ClassFactory::flush() const
    {
        m_pending_types.clear();
        m_pending_ptrs.clear();

        // flush from class specific schema builders
        for (auto &item: m_ptr_cache) {
            item.second.m_class->flush();
        }
    }
    
    void ClassFactory::rollback()
    {
        // rollback from class specific schema builders
        for (auto &item: m_ptr_cache) {
            item.second.m_class->rollback();
        }
        // rollback all pending types and pointers from local cache
        for (auto &lang_type: m_pending_types) {
            m_type_cache.erase(lang_type.get());
        }
        for (auto &ptr: m_pending_ptrs) {
            m_ptr_cache.erase(ptr);
        }

        m_pending_types.clear();
        m_pending_ptrs.clear();
    }
    
    void ClassFactory::commit() const
    {
        for (auto &item: m_ptr_cache) {
            item.second.m_class->commit();
        }
        for (auto &class_map: m_class_maps) {
            class_map.commit();
        }
        m_class_ptr_index.commit();
        super_t::commit();
    }
    
    void ClassFactory::detach() const
    {
        for (auto &class_map: m_class_maps) {
            class_map.detach();
        }
        m_class_ptr_index.detach();
        // detach class objects only, without removing them from the cache
        for (auto &item: m_ptr_cache) {
            item.second.m_class->detach();
        }
        super_t::detach();
    }
    
    void ClassFactory::forAll(std::function<void(const Class &)> f) const
    {
        for (auto it = m_class_maps[1].begin(), end = m_class_maps[1].end(); it != end; ++it) {
            f(*getTypeByPtr(it->second()).m_class);
        }
    }
    
    bool ClassFactory::exists(const Class &class_obj) const {
        return m_class_ptr_index.find(ClassPtr(class_obj)) != m_class_ptr_index.end();
    }

    ClassFactory::TypeObjectSharedPtr ClassFactory::getLangType(const Class &type) const
    {
        auto it_cached = m_ptr_cache.find(ClassPtr(type));
        if (it_cached == m_ptr_cache.end()) {
            THROWF(db0::InternalException) << "Class not found: " << type.getName();
        }
        return it_cached->second.m_lang_type;
    }
    
    ClassFactory::TypeObjectSharedPtr ClassFactory::getLangType(const ClassItem &class_item) const
    {
        if (class_item.m_lang_type.get()) {
            return class_item.m_lang_type;
        }
        if (!class_item.m_class) {
            THROWF(db0::InputException) << "Class not found";
        }
        return getLangType(*class_item.m_class);
    }
    
    bool ClassFactory::hasLangType(const Class &type) const
    {
        auto it_cached = m_ptr_cache.find(ClassPtr(type));
        return it_cached != m_ptr_cache.end() && it_cached->second.m_lang_type.get();
    }
    
    ClassFactory::TypeObjectPtr ClassFactory::tryFindLangType(const Class &_class) const
    {
        auto &type_manager = LangToolkit::getTypeManager();
        // look-up all name variants
        for (unsigned int i = 0; i < 4; ++i) {
            auto variant_key = getNameVariant(_class, i);
            if (variant_key) {
                auto lang_type = type_manager.findType(*variant_key);
                if (lang_type) {
                    return lang_type;
                }
            }
        }
        // type not found
        return nullptr;
    }
    
    std::uint32_t ClassFactory::getClassRef(Address class_addr) const {
        return classRef(class_addr, m_type_slot_addr_range);
    }
    
}