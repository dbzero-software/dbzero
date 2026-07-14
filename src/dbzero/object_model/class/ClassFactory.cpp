// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "ClassFactory.hpp"
#include "Class.hpp"
#include <dbzero/bindings/python/Memo.hpp>
#include <dbzero/bindings/python/MigrateError.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/core/memory/SieveCache.hpp>
#include <dbzero/core/utils/conversions.hpp>
#include <dbzero/workspace/Snapshot.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/object_model/value/ObjectId.hpp>
#include <dbzero/object_model/tags/ObjectIterator.hpp>
#include <dbzero/object_model/tags/TagIndex.hpp>
#include <algorithm>
#include <unordered_set>

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

    void applyAccessControlFlag(Class &type, ClassFactory::TypeObjectPtr lang_type)
    {
        // A direct @memo(access_control=True) decoration is durable metadata for this
        // class, so persist it when the language type is first attached or loaded.
        if (lang_type && ClassFactory::LangToolkit::isAccessControl(lang_type) && !type.hasOwnAccessControl()) {
            type.setOwnAccessControl();
        // Classes reopened from storage may already own the durable flag. Re-apply the
        // runtime state so access control is dynamically propagated to loaded bases
        // without writing that inherited state into the base classes themselves.
        } else if (type.hasOwnAccessControl()) {
            type.setAccessControl();
        }
    }

    bool sameFieldIds(const std::vector<FieldID> &lhs, const std::vector<FieldID> &rhs)
    {
        if (lhs.size() != rhs.size()) {
            return false;
        }
        for (std::size_t i = 0; i < lhs.size(); ++i) {
            if (!(lhs[i] == rhs[i])) {
                return false;
            }
        }
        return true;
    }

    std::vector<FieldID> resolveDeclaredTagFields(const Class &type, const std::vector<std::string> &tag_fields)
    {
        std::vector<FieldID> result;
        std::unordered_set<std::uint32_t> seen;
        for (const auto &field_name: tag_fields) {
            auto member = type.tryGetMember(field_name.c_str());
            if (member) {
                auto long_index = member->m_field_id.getLongIndex();
                if (seen.insert(long_index).second) {
                    result.push_back(member->m_field_id);
                }
            }
        }
        return result;
    }

    std::unordered_set<std::uint32_t> fieldIdSet(const std::vector<FieldID> &field_ids)
    {
        std::unordered_set<std::uint32_t> result;
        for (const auto &field_id: field_ids) {
            result.insert(field_id.getLongIndex());
        }
        return result;
    }

    std::vector<FieldID> differenceById(const std::vector<FieldID> &lhs, const std::vector<FieldID> &rhs)
    {
        auto rhs_set = fieldIdSet(rhs);
        std::vector<FieldID> result;
        for (const auto &field_id: lhs) {
            if (rhs_set.find(field_id.getLongIndex()) == rhs_set.end()) {
                result.push_back(field_id);
            }
        }
        return result;
    }

    struct TagFieldEdit
    {
        std::vector<FieldID> remove;
        std::vector<FieldID> add;
    };

    TagFieldEdit makeOrderedTagFieldEdit(const std::vector<FieldID> &old_tag_field_ids,
        const std::vector<FieldID> &new_tag_field_ids)
    {
        std::size_t common_prefix_size = 0;
        while (common_prefix_size < old_tag_field_ids.size()
            && common_prefix_size < new_tag_field_ids.size()
            && old_tag_field_ids[common_prefix_size] == new_tag_field_ids[common_prefix_size])
        {
            ++common_prefix_size;
        }

        TagFieldEdit edit;
        edit.remove.insert(edit.remove.end(), old_tag_field_ids.begin() + common_prefix_size,
            old_tag_field_ids.end());
        edit.add.insert(edit.add.end(), new_tag_field_ids.begin() + common_prefix_size,
            new_tag_field_ids.end());
        return edit;
    }

    std::optional<MemberLoc> tryMemberLoc(const Class &type, FieldID field_id)
    {
        auto member = type.tryGetMember(field_id);
        if (!member) {
            return {};
        }
        return type.findField(member->m_name.c_str());
    }

    template <typename MemoT>
    typename TagIndex::ObjectSharedPtr tryGetMemoField(MemoT *memo_obj, const MemberLoc &member_loc)
    {
        return memo_obj->ext().tryGet(member_loc);
    }

    TagIndex::ObjectSharedPtr tryGetMemoField(TagIndex::ObjectPtr py_obj, const MemberLoc &member_loc)
    {
        if (db0::python::PyMemo_Check<db0::python::MemoObject>(py_obj)) {
            return tryGetMemoField(reinterpret_cast<db0::python::MemoObject *>(py_obj), member_loc);
        }
        if (db0::python::PyMemo_Check<db0::python::MemoImmutableObject>(py_obj)) {
            return tryGetMemoField(reinterpret_cast<db0::python::MemoImmutableObject *>(py_obj), member_loc);
        }
        return {};
    }

    template <typename MemoT>
    const Class &getMemoType(MemoT *memo_obj)
    {
        return memo_obj->ext().getType();
    }

    const Class *tryGetMemoType(TagIndex::ObjectPtr py_obj)
    {
        if (db0::python::PyMemo_Check<db0::python::MemoObject>(py_obj)) {
            return &getMemoType(reinterpret_cast<db0::python::MemoObject *>(py_obj));
        }
        if (db0::python::PyMemo_Check<db0::python::MemoImmutableObject>(py_obj)) {
            return &getMemoType(reinterpret_cast<db0::python::MemoImmutableObject *>(py_obj));
        }
        return nullptr;
    }

    struct TagFieldMigrateMemberLocs
    {
        std::vector<std::optional<MemberLoc> > removed;
        std::vector<std::optional<MemberLoc> > added;
    };

    class TagFieldMigrateCache:
        public db0::SieveCache<const Class *, TagFieldMigrateMemberLocs, 4>
    {
    public:
        using MemberLocs = TagFieldMigrateMemberLocs;
        using Super = db0::SieveCache<const Class *, TagFieldMigrateMemberLocs, 4>;

        TagFieldMigrateCache(const std::vector<FieldID> &removed_ids, const std::vector<FieldID> &added_ids)
            : m_removed_ids(removed_ids)
            , m_added_ids(added_ids)
        {
        }

        const MemberLocs &get(const Class &type)
        {
            return Super::getOrCreate(&type, [&] {
                return resolve(type);
            });
        }

    private:
        MemberLocs resolve(const Class &type) const
        {
            MemberLocs member_locs;
            member_locs.removed.reserve(m_removed_ids.size());
            for (const auto &field_id: m_removed_ids) {
                member_locs.removed.push_back(tryMemberLoc(type, field_id));
            }
            member_locs.added.reserve(m_added_ids.size());
            for (const auto &field_id: m_added_ids) {
                member_locs.added.push_back(tryMemberLoc(type, field_id));
            }
            return member_locs;
        }

        const std::vector<FieldID> &m_removed_ids;
        const std::vector<FieldID> &m_added_ids;
    };

    void migrateTagFields(Class &type, const std::vector<FieldID> &passive_removed_ids,
        const std::vector<FieldID> &passive_added_ids, const TagFieldEdit &tag_field_edit)
    {
        auto fixture = type.getFixture();
        if (fixture->getAccessType() != AccessType::READ_WRITE) {
            return;
        }

        auto &tag_index = fixture->get<TagIndex>();
        db0::FixtureLock lock(fixture);
        tag_index.flush();
        try {
            auto query = tag_index.makeIterator(type);
            if (query) {
                TagFieldMigrateCache migrate_cache(passive_removed_ids, passive_added_ids);
                ObjectIterator iterator(fixture, std::move(query), type.shared_from_this());
                for (;;) {
                    auto obj = iterator.next();
                    if (!obj.get()) {
                        break;
                    }
                    auto obj_type = tryGetMemoType(obj.get());
                    if (!obj_type || !(*obj_type == type)) {
                        continue;
                    }
                    const auto &member_locs = migrate_cache.get(*obj_type);
                    for (const auto &member_loc: member_locs.removed) {
                        if (!member_loc) {
                            continue;
                        }
                        auto value = tryGetMemoField(obj.get(), *member_loc);
                        if (!!value && value.get() != Py_None) {
                            auto tag = tag_index.preparePassiveTag(value.get());
                            tag_index.remove(obj.get(), tag);
                        }
                    }
                    for (const auto &member_loc: member_locs.added) {
                        if (!member_loc) {
                            continue;
                        }
                        auto value = tryGetMemoField(obj.get(), *member_loc);
                        if (!!value && value.get() != Py_None) {
                            tag_index.validatePassiveScalar(value.get());
                            auto tag = tag_index.preparePassiveTag(value.get());
                            tag_index.add(obj.get(), tag);
                        }
                    }
                }
            }
        } catch (...) {
            tag_index.rollback();
            throw;
        }

        for (const auto &field_id: tag_field_edit.remove) {
            type.removeTagField(field_id);
        }
        for (const auto &field_id: tag_field_edit.add) {
            type.addTagField(field_id);
        }
    }

    void applyTagFieldDeclarations(Class &type, const std::vector<std::string> &tag_fields,
        bool migrate, bool raise_on_mismatch)
    {
        auto new_tag_field_ids = resolveDeclaredTagFields(type, tag_fields);
        const auto &old_tag_field_ids = type.getTagFieldIds();
        auto tag_field_edit = makeOrderedTagFieldEdit(old_tag_field_ids, new_tag_field_ids);
        if (sameFieldIds(old_tag_field_ids, new_tag_field_ids)) {
            type.setDeclaredTagFields(tag_fields);
            return;
        }

        if (migrate) {
            auto passive_removed_ids = differenceById(old_tag_field_ids, new_tag_field_ids);
            auto passive_added_ids = differenceById(new_tag_field_ids, old_tag_field_ids);
            migrateTagFields(type, passive_removed_ids, passive_added_ids, tag_field_edit);
            type.setDeclaredTagFields(tag_fields);
            return;
        }

        type.setDeclaredTagFields(type.getTagFieldNames());
        if (raise_on_mismatch) {
            throw db0::python::MigrateException("Tag-field declaration migration is required for class "
                + type.getName());
        }
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
            bool no_auto_migrate = LangToolkit::isNoAutoMigrate(*getFixture());
            bool can_migrate = getFixture()->getAccessType() == AccessType::READ_WRITE && !no_auto_migrate;
            applyTagFieldDeclarations(*type, LangToolkit::getTagFields(lang_type), can_migrate, false);
            // add to by-type cache
            it_cached = m_type_cache.insert({lang_type, type}).first;
            m_pending_types.push_back(lang_type);
        } else if (lang_type) {
            bool no_auto_migrate = LangToolkit::isNoAutoMigrate(*getFixture());
            bool can_migrate = getFixture()->getAccessType() == AccessType::READ_WRITE && !no_auto_migrate;
            applyTagFieldDeclarations(*it_cached->second, LangToolkit::getTagFields(lang_type), can_migrate, false);
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
                auto memo_base = LangToolkit::getBaseMemoType(lang_type);
                if (memo_base) {
                    getOrCreateType(memo_base);
                }
                applyAccessControlFlag(*type, lang_type);
                bool no_auto_migrate = LangToolkit::isNoAutoMigrate(*getFixture());
                bool can_migrate = getFixture()->getAccessType() == AccessType::READ_WRITE && !no_auto_migrate;
                applyTagFieldDeclarations(*type, LangToolkit::getTagFields(lang_type), can_migrate,
                    no_auto_migrate);
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
                flags.set(ClassOptions::ACCESS_CONTROL, LangToolkit::isAccessControl(lang_type));
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
                    type->setDeclaredTagFields(LangToolkit::getTagFields(lang_type));
                    for (const auto &field_id: resolveDeclaredTagFields(*type, LangToolkit::getTagFields(lang_type))) {
                        type->addTagField(field_id);
                    }
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

    void ClassFactory::migrateTagFields(TypeObjectPtr lang_type)
    {
        if (!lang_type || !LangToolkit::isAnyMemoType(lang_type)) {
            THROWF(db0::InputException) << "migrate requires a dbzero memo type";
        }
        auto fixture = getFixture();
        assureAccessType(*fixture, AccessType::READ_WRITE);

        auto memo_base = LangToolkit::getBaseMemoType(lang_type);
        if (memo_base) {
            migrateTagFields(memo_base);
        }

        auto it_cached = m_type_cache.find(lang_type);
        std::shared_ptr<Class> type;
        if (it_cached != m_type_cache.end()) {
            type = it_cached->second;
        } else {
            auto class_ptr = tryFindClassPtr(lang_type, LangToolkit::getMemoTypeID(lang_type));
            if (!class_ptr) {
                type = getOrCreateType(lang_type);
            } else {
                type = getTypeByPtr(class_ptr, lang_type).m_class;
                it_cached = m_type_cache.insert({lang_type, type}).first;
                m_pending_types.push_back(lang_type);
            }
        }

        applyTagFieldDeclarations(*type, LangToolkit::getTagFields(lang_type), true, false);
    }
    
    std::shared_ptr<Class> ClassFactory::getType(ClassPtr ptr, std::shared_ptr<Class> type, TypeObjectPtr lang_type) const
    {
        auto it_cached = m_ptr_cache.find(ptr);
        bool apply_lang_metadata = false;
        if (it_cached == m_ptr_cache.end()) {
            // try looking-up language specific type with the TypeManager
            if (!lang_type) {
                lang_type = tryFindLangType(*type);
            }
            // add to by-pointer cache
            it_cached = m_ptr_cache.insert({ptr, ClassItem{ type, lang_type }}).first;
            m_pending_ptrs.push_back(ptr);
            apply_lang_metadata = !!lang_type;
        }
        if (lang_type && !it_cached->second.m_lang_type) {
            it_cached->second.m_lang_type = lang_type;
            it_cached->second.m_class->setInitVars(LangToolkit::getInitVars(lang_type));
            it_cached->second.m_class->setRuntimeFlags(LangToolkit::getMemoFlags(lang_type));
            apply_lang_metadata = true;
        }
        if (apply_lang_metadata) {
            applyAccessControlFlag(*it_cached->second.m_class, lang_type);
            bool no_auto_migrate = LangToolkit::isNoAutoMigrate(*getFixture());
            bool can_migrate = getFixture()->getAccessType() == AccessType::READ_WRITE && !no_auto_migrate;
            applyTagFieldDeclarations(*it_cached->second.m_class, LangToolkit::getTagFields(lang_type), can_migrate,
                no_auto_migrate);
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
        bool apply_lang_metadata = false;
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
                applyAccessControlFlag(*type, lang_type);
                bool no_auto_migrate = LangToolkit::isNoAutoMigrate(*getFixture());
                bool can_migrate = getFixture()->getAccessType() == AccessType::READ_WRITE && !no_auto_migrate;
                applyTagFieldDeclarations(*type, LangToolkit::getTagFields(lang_type), can_migrate, false);
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
            apply_lang_metadata = true;
        }
        if (apply_lang_metadata) {
            applyAccessControlFlag(*it_cached->second.m_class, lang_type);
            bool no_auto_migrate = LangToolkit::isNoAutoMigrate(*getFixture());
            bool can_migrate = getFixture()->getAccessType() == AccessType::READ_WRITE && !no_auto_migrate;
            applyTagFieldDeclarations(*it_cached->second.m_class, LangToolkit::getTagFields(lang_type), can_migrate,
                no_auto_migrate);
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
