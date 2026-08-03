// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#include "Class.hpp"
#include <dbzero/core/utils/uuid.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include "ClassFactory.hpp"
#include <dbzero/workspace/Snapshot.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/value/ObjectId.hpp>
#include <dbzero/object_model/value/StorageClass.hpp>
#include <dbzero/object_model/index/Index.hpp>
#include "Schema.hpp"

DEFINE_ENUM_VALUES(db0::ClassOptions, "SINGLETON", "NO_DEFAULT_TAGS", "IMMUTABLE", "RESERVED_0008", "RESERVED_0010", "ACCESS_CONTROL")
DEFINE_ENUM_VALUES(db0::object_model::FieldOptions, "TAG_FIELD", "INDEXED_FIELD")

namespace db0::object_model

{
    
    using namespace db0;
    using namespace db0::pools;

    template <typename KeyT>
    void setFieldOption(std::unordered_map<KeyT, FieldFlags> &options_by_field, const KeyT &key,
        FieldOptions option, bool enabled)
    {
        auto it = options_by_field.find(key);
        if (enabled) {
            if (it == options_by_field.end()) {
                options_by_field.emplace(key, FieldFlags({ option }));
            } else {
                it->second.set(option);
            }
        } else if (it != options_by_field.end()) {
            it->second.clear(option);
            if (it->second.value() == 0) {
                options_by_field.erase(it);
            }
        }
    }
    
    GC0_Define(Class)
    
    std::uint32_t classRef(Address addr, std::pair<std::uint64_t, std::uint64_t> type_slot_addr_range)
    {
        auto addr_offset = addr.getOffset();
        if (addr_offset < type_slot_addr_range.first || addr_offset >= type_slot_addr_range.second) {
            THROWF(db0::BadAddressException) << "Invalid address accessed";
        }
        // calculate class ref as a relative offset from the type slot begin address
        addr_offset -= type_slot_addr_range.first;
        assert(addr_offset < std::numeric_limits<std::uint32_t>::max());
        // NOTE: +1 to avoid 0 as a class ref
        return static_cast<std::uint32_t>(addr_offset) + 1;
    }
    
    std::uint32_t classRef(const Class &type, std::pair<std::uint64_t, std::uint64_t> type_slot_addr_range) {
        return classRef(type.getAddress(), type_slot_addr_range);
    }
    
    Address classRefToAddress(std::uint32_t class_ref, std::pair<std::uint64_t, std::uint64_t> type_slot_addr_range)
    {
        // calculate the absolute address
        assert((static_cast<std::uint64_t>(class_ref) - 1 + type_slot_addr_range.first) < type_slot_addr_range.second);
        return Address::fromOffset(static_cast<std::uint64_t>(class_ref) - 1 + type_slot_addr_range.first);
    }
    
    std::pair<std::uint64_t, std::uint64_t> getTypeSlotAddrRange(const Fixture &fixture)
    {        
        auto range = fixture.getAllocator().getRange(Class::SLOT_NUM);
        // type slot has a bounded range
        assert(range.second);
        return { range.first.getOffset(), range.second->getOffset() };
    }
    
    o_class::o_class(RC_LimitedStringPool &string_pool, const std::string &name, std::optional<std::string> module_name,
        const VFieldMatrix &members, const VFidelityVector &fidelities, const Schema &schema, const char *type_id, 
        const char *prefix_name, ClassFlags flags, std::uint32_t base_class_ref, std::uint32_t num_bases)
        : m_uuid(db0::make_UUID())
        , m_name(string_pool.addRef(name))
        , m_type_id(type_id ? string_pool.addRef(type_id) : LP_String())
        , m_prefix_name(prefix_name ? string_pool.addRef(prefix_name) : LP_String())
        , m_members_ptr(members)
        , m_fidelity_ptr(fidelities)
        , m_schema_ptr(schema)
        , m_flags(flags)
        , m_base_class_ref(base_class_ref)
        , m_num_bases(num_bases)
    {
        if (module_name) {
            m_module_name = string_pool.addRef(*module_name);
        }
    }
    
    Class::Member::Member(FieldID field_id, unsigned int fidelity, const char *name, FieldFlags field_options)
        : m_field_id(field_id)
        , m_fidelity(fidelity)
        , m_name(name)
        , m_field_options(field_options)
    {
    }
    
    Class::Member::Member(FieldID field_id, unsigned int fidelity, const std::string &name, FieldFlags field_options)
        : m_field_id(field_id)
        , m_fidelity(fidelity)
        , m_name(name)
        , m_field_options(field_options)
    {
    }
    
    Class::Class(db0::swine_ptr<Fixture> &fixture, const std::string &name, std::optional<std::string> module_name,
        const char *type_id, const char *prefix_name, const std::vector<std::string> &init_vars, ClassFlags flags, 
        std::shared_ptr<Class> base_class)
        : super_t(
            fixture, 
            fixture->getLimitedStringPool(), 
            name, 
            module_name,
            VFieldMatrix(*fixture),
            VFidelityVector(*fixture),
            Schema(*fixture),
            type_id, 
            prefix_name, 
            flags, 
            base_class ? classRef(*base_class, getTypeSlotAddrRange(*fixture)) : 0,
            base_class ? (1u + base_class->getNumBases()) : 0)
        , m_type_slot_addr_range(getTypeSlotAddrRange(*fixture))
        , m_members((*this)->m_members_ptr(*fixture))
        , m_fidelities((*this)->m_fidelity_ptr(*fixture))
        , m_schema((*this)->m_schema_ptr(*fixture))
        , m_base_class_ptr(base_class)
        , m_init_vars(this->makeInitVars(init_vars))
        , m_uid(this->fetchUID())
        , m_member_cache(m_members, *this, this->getRefreshCallback())        
    {
        m_tag_fields.setKeyChangeCallback([this](const FieldID &field_id, bool added) {
            onTagFieldKeyChange(field_id, added);
        });
        m_indexed_fields.setKeyChangeCallback([this](const IndexedField &indexed_field, bool added) {
            onIndexedFieldKeyChange(indexed_field, added);
        });
        openTagFields();
        if (hasOwnAccessControl()) {
            setAccessControl();
        }
        m_schema.postInit(getTotalFunc());
    }
    
    Class::Class(db0::swine_ptr<Fixture> &fixture, Address address)
        : super_t(super_t::tag_from_address(), fixture, address)
        , m_type_slot_addr_range(getTypeSlotAddrRange(*fixture))
        , m_members((*this)->m_members_ptr(*fixture))
        , m_fidelities((*this)->m_fidelity_ptr(*fixture))
        , m_schema((*this)->m_schema_ptr(*fixture))
        , m_uid(this->fetchUID())
        , m_member_cache(m_members, *this, this->getRefreshCallback())
    {
        m_tag_fields.setKeyChangeCallback([this](const FieldID &field_id, bool added) {
            onTagFieldKeyChange(field_id, added);
        });
        m_indexed_fields.setKeyChangeCallback([this](const IndexedField &indexed_field, bool added) {
            onIndexedFieldKeyChange(indexed_field, added);
        });
        openTagFields();
        m_schema.postInit(getTotalFunc());
        // initialize base class if such exists
        if ((*this)->m_base_class_ref) {
            auto fixture = this->getFixture();
            m_base_class_ptr = getClassFactory(*fixture).getTypeByClassRef((*this)->m_base_class_ref).m_class;
        }
        if (hasOwnAccessControl()) {
            setAccessControl();
        }
    }
    
    Class::~Class()
    {
        // unregister needs to be called before the destruction of members
        unregister();        
    }

    std::unordered_set<std::string> Class::makeInitVars(const std::vector<std::string> &init_vars) const
    {
        std::unordered_set<std::string> result;
        // copy all init vars from base class
        if (m_base_class_ptr) {        
            const auto &base_init_vars = m_base_class_ptr->getInitVars();
            std::copy(base_init_vars.begin(), base_init_vars.end(), std::inserter(result, result.end()));
        }
        std::copy(init_vars.begin(), init_vars.end(), std::inserter(result, result.end()));
        return result;
    }
    
    std::string Class::getName() const {
        return getFixture()->getLimitedStringPool().fetch((*this)->m_name);
    }
    
    std::optional<std::string> Class::getTypeId() const
    {
        if (!(*this)->m_type_id) {
            return std::nullopt;
        }
        return getFixture()->getLimitedStringPool().fetch((*this)->m_type_id);
    }
    
    MemberID Class::addField(const char *name, unsigned int fidelity, bool declared_tag_field)
    {
        auto member_id = addFieldInternal(name, fidelity, true, declared_tag_field);
        if (isDeclaredIndexedField(name)) {
            if (m_base_class_ptr
                && (m_base_class_ptr->isDeclaredIndexedField(name) || m_base_class_ptr->tryGetFieldIndex(name))) {
                THROWF(db0::InputException)
                    << "Indexed field declaration overlaps an ancestor indexed field in class "
                    << getName();
            }
            addIndexedField(member_id.primary().first);
        }
        return member_id;
    }

    MemberID Class::addFieldInternal(const char *name, unsigned int fidelity, bool, bool declared_tag_field)
    {
        assert(fidelity < std::numeric_limits<std::uint8_t>::max());

        // NOTE: before creating with fidelity = 0 we'll always pre-register
        // a slot for fidelity = 2 which will be used as the PRIMARY identifier
        if (fidelity == 0 && !hasSlot(name, PRIMARY_FIDELITY)) {
            addFieldInternal(name, PRIMARY_FIDELITY, false);
        }
        
        auto pos = assignSlot(fidelity);
        // reserve the slot
        m_members.set(pos, o_field { getFixture()->getLimitedStringPool(), name });
        m_member_cache.reload(pos);
        auto member_id = m_index[name].first;
        if (declared_tag_field) {
            addTagField(member_id.primary().first);
        }
        return member_id;
    }
    
    bool Class::hasSlot(const char *name, unsigned int fidelity) const
    {
        auto it = m_index.find(name);
        if (it == m_index.end()) {
            // try again after refreshing the cache
            if (m_member_cache.refresh()) {
                it = m_index.find(name);
            }
        }

        if (it != m_index.end()) {
            return it->second.first.hasFidelity(fidelity);
        }
        
        return false;
    }
        
    MemberLoc Class::findField(const char *name) const
    {
        // NOTE: refresh is a lightweght operation if there were no changes (no detach)
        m_member_cache.fastRefresh();
        auto it = m_index.find(name);
        if (it == m_index.end()) {
            // field ID not found, check for possible initialization variable
            bool is_init_var = m_init_vars.find(name) != m_init_vars.end();
            return { MemberID(), is_init_var };            
        }
        
        return it->second;
    }
    
    std::optional<Class::Member> Class::tryGetMember(FieldID field_id) const
    {
        // NOTE: cache might be refreshed if not found at first attempt
        auto member_ptr = m_member_cache.tryGet(field_id.getIndexAndOffset());
        if (!member_ptr) {
            return {};
        }
        return *member_ptr;
    }
    
    std::optional<Class::Member> Class::tryGetMember(std::pair<std::uint32_t, std::uint32_t> loc) const
    {
        // NOTE: cache might be refreshed if not found at first attempt
        auto member_ptr = m_member_cache.tryGet(loc);
        if (!member_ptr) {
            return {};
        }
        return *member_ptr;
    }

    Class::Member Class::getMember(FieldID field_id) const
    {
        auto maybe_member = tryGetMember(field_id.getIndexAndOffset());
        if (!maybe_member) {
            THROWF(db0::InputException) << "Member slot not found: " << field_id.getIndex();
        }
        return *maybe_member;
    }
    
    Class::Member Class::getMember(std::pair<std::uint32_t, std::uint32_t> loc) const
    {
        auto maybe_member = tryGetMember(loc);
        if (!maybe_member) {
            THROWF(db0::InputException) << "Member slot not found: " << loc.first << "@" << loc.second;
        }
        return *maybe_member;
    }

    std::optional<Class::Member> Class::tryGetMember(const char *name) const
    {
        auto it = m_index.find(name);
        if (it == m_index.end()) {
            return {};
        }
        return getMember(std::get<0>(it->second).primary().first);
    }
    
    Class::Member Class::getMember(const char *name) const
    {
        auto maybe_member = tryGetMember(name);
        if (!maybe_member) {
            THROWF(db0::InputException) << "Field " << name << " not found in class " << getName();
        }
        return *maybe_member;
    }
    
    bool Class::unloadSingleton(void *at) const
    {
        if (!(*this)->m_singleton_address.isValid()) {
            return false;
        }
        
        auto fixture = getFixture();
        auto &class_factory = getClassFactory(*fixture);
        auto stem = Object::unloadStem(fixture, (*this)->m_singleton_address);
        auto type = class_factory.getTypeByPtr(
            db0::db0_ptr_reinterpret_cast<Class>()(classRefToAddress(stem->getClassRef(), m_type_slot_addr_range))).m_class;
        // unload from stem
        new (at) Object(fixture, std::move(stem), type, Object::with_type_hint{});
        return true;
    }
    
    bool Class::isSingleton() const {
        return (*this)->m_flags[ClassOptions::SINGLETON];
    }

    bool Class::isNoDefaultTags() const {
        return (*this)->m_flags[ClassOptions::NO_DEFAULT_TAGS];
    }

    bool Class::isImmutable() const {
        return (*this)->m_flags[ClassOptions::IMMUTABLE];
    }

    bool Class::hasOwnAccessControl() const {
        return (*this)->m_flags[ClassOptions::ACCESS_CONTROL];
    }

    bool Class::isAccessControl() const {
        return m_access_control;
    }

    void Class::setAccessControl() {
        m_access_control = true;
        if (m_base_class_ptr) {
            m_base_class_ptr->setAccessControl();
        }
    }

    void Class::setOwnAccessControl() {
        modify().m_flags.set(ClassOptions::ACCESS_CONTROL, true);
        setAccessControl();
    }

    bool Class::isExistingSingleton() const {
        return isSingleton() && (*this)->m_singleton_address.isValid();
    }
    
    void Class::onMemberIDUpdated(const MemberID &member_id) const
    {
        if (member_id.hasFidelity(PRIMARY_FIDELITY) && member_id.size() > 1) {
            // ensure unique keys vector is large enough
            auto index = member_id.secondary().first.getIndex();
            if (m_unique_keys.size() <= index) {
                m_unique_keys.resize(index + 1);
            }
            // register the unique key
            m_unique_keys[index] = member_id.primary().first;
        }
    }

    std::function<void(const Class::Member &)> Class::getRefreshCallback() const
    {
        return [this](const Member &member) {
            // this is required before accessing members to prevent segfaults on a defunct object
            auto fixture = getFixture();
            auto it = m_index.find(member.m_name);
            if (it == m_index.end()) {
                bool is_init_var = m_init_vars.find(member.m_name) != m_init_vars.end();
                auto member_id = MemberID(member.m_field_id, member.m_fidelity);
                m_index[member.m_name] = { member_id, is_init_var };
                onMemberIDUpdated(member_id);
            } else {
                // extend existing member ID
                // possibly another fidelity was added
                if (it->second.first.tryGet(member.m_fidelity) == member.m_field_id) {
                    return;
                }
                it->second.first.assign(member.m_field_id, member.m_fidelity);
                onMemberIDUpdated(it->second.first);
            }
        };
    }

    void Class::openTagFields() const
    {
        if ((*this)->m_tag_fields_ptr && m_tag_fields.isNull()) {
            m_tag_fields.init(getFixture()->myPtr((*this)->m_tag_fields_ptr.getAddress()));
        }
    }

    void Class::openIndexedFields() const
    {
        if ((*this)->getObjVer() >= 1
            && (*this)->m_flags[ClassOptions::RESERVED_0010]
            && (*this)->m_indexed_fields_ptr
            && m_indexed_fields.isNull()) {
            m_indexed_fields.init(getFixture()->myPtr((*this)->m_indexed_fields_ptr.getAddress()));
            for (const auto &indexed_field: m_indexed_fields.cached()) {
                onIndexedFieldKeyChange(indexed_field, true);
            }
        }
    }

    void Class::onTagFieldKeyChange(const FieldID &field_id, bool added) const
    {
        setFieldOption(m_field_options, field_id.getLongIndex(), FieldOptions::TAG_FIELD, added);
        reloadMemberOptions(field_id);
    }

    void Class::onIndexedFieldKeyChange(const IndexedField &indexed_field, bool added) const
    {
        auto field_id = indexed_field.getFieldId();
        if (added) {
            auto fixture = getFixture();
            auto address = indexed_field.m_index_ptr.getAddress();
            m_index_cache[indexed_field.m_field_id] = fixture->getVObjectCache().findOrPull<Index>(
                address, true, fixture, address
            );
            setFieldOption(m_field_options, field_id.getLongIndex(), FieldOptions::INDEXED_FIELD, true);
        } else {
            m_index_cache.erase(indexed_field.m_field_id);
            setFieldOption(m_field_options, field_id.getLongIndex(), FieldOptions::INDEXED_FIELD, false);
        }
        reloadMemberOptions(field_id);
    }

    VTagFields &Class::ensureTagFields()
    {
        if (!m_tag_fields.isNull()) {
            return m_tag_fields;
        }
        openTagFields();
        if (m_tag_fields.isNull()) {
            m_tag_fields.init(*getFixture());
            modify().m_tag_fields_ptr = m_tag_fields;
        }
        return m_tag_fields;
    }

    VIndexedFields &Class::ensureIndexedFields()
    {
        if (!m_indexed_fields.isNull()) {
            return m_indexed_fields;
        }
        if (hasDeclaredIndexedFields()) {
            openIndexedFields();
        }
        if (m_indexed_fields.isNull()) {
            m_indexed_fields.init(*getFixture());
            modify().m_indexed_fields_ptr = m_indexed_fields;
            modify().m_flags.set(ClassOptions::RESERVED_0010);
        }
        return m_indexed_fields;
    }

    void Class::addTagField(FieldID field_id)
    {
        auto &tag_fields = ensureTagFields();
        for (const auto &tag_field_id: tag_fields.cached()) {
            if (tag_field_id == field_id) {
                return;
            }
        }
        tag_fields.push_back(field_id);
    }

    void Class::removeTagField(FieldID field_id)
    {
        auto &tag_fields = ensureTagFields();
        const auto &cached_tag_fields = tag_fields.cached();
        for (std::size_t index = 0; index < cached_tag_fields.size(); ++index) {
            if (cached_tag_fields[index] == field_id) {
                tag_fields.erase(index);
                return;
            }
        }
    }

    const std::vector<FieldID> &Class::getTagFieldIds() const
    {
        static const std::vector<FieldID> empty_tag_field_ids;
        openTagFields();
        if (m_tag_fields.isNull()) {
            return empty_tag_field_ids;
        }
        return m_tag_fields.cached();
    }

    bool Class::isTagField(FieldID field_id) const
    {
        return getOwnFieldOptions(field_id)[FieldOptions::TAG_FIELD];
    }

    void Class::addIndexedField(FieldID field_id)
    {
        auto &indexed_fields = ensureIndexedFields();
        for (const auto &indexed_field: indexed_fields.cached()) {
            if (indexed_field.getFieldId() == field_id) {
                onIndexedFieldKeyChange(indexed_field, true);
                return;
            }
        }

        auto fixture = getFixture();
        Index index(fixture, true);
        index.setManaged();
        index.incRef(false);
        IndexedField indexed_field(field_id, db0_ptr<Index>(index));
        indexed_fields.push_back(indexed_field);
        onIndexedFieldKeyChange(indexed_field, true);
    }

    void Class::addIndexedField(FieldID field_id, const db0_ptr<Index> &index_ptr)
    {
        auto &indexed_fields = ensureIndexedFields();
        for (const auto &indexed_field: indexed_fields.cached()) {
            if (indexed_field.getFieldId() == field_id) {
                onIndexedFieldKeyChange(indexed_field, true);
                return;
            }
        }
        IndexedField indexed_field(field_id, index_ptr);
        indexed_fields.push_back(indexed_field);
        onIndexedFieldKeyChange(indexed_field, true);
    }

    void Class::removeIndexedField(FieldID field_id)
    {
        auto &indexed_fields = ensureIndexedFields();
        const auto &cached_indexed_fields = indexed_fields.cached();
        for (std::size_t index = 0; index < cached_indexed_fields.size(); ++index) {
            if (cached_indexed_fields[index].getFieldId() == field_id) {
                auto indexed_field = cached_indexed_fields[index];
                auto fixture = getFixture();
                auto index_address = indexed_field.m_index_ptr.getAddress();
                auto managed_index_ptr = fixture->getVObjectCache().findOrPull<Index>(
                    index_address, true, fixture, index_address
                );
                indexed_fields.erase(index);
                fixture->getLangCache().erase(index_address);
                if (managed_index_ptr->decRef(false)) {
                    managed_index_ptr->destroy();
                    fixture->getVObjectCache().erase(index_address);
                }
                onIndexedFieldKeyChange(indexed_field, false);
                return;
            }
        }
    }

    const std::vector<IndexedField> &Class::getIndexedFieldRecords() const
    {
        static const std::vector<IndexedField> empty_indexed_fields;
        openIndexedFields();
        if (m_indexed_fields.isNull()) {
            return empty_indexed_fields;
        }
        return m_indexed_fields.cached();
    }

    std::vector<FieldID> Class::getIndexedFieldIds() const
    {
        std::vector<FieldID> result;
        const auto &records = getIndexedFieldRecords();
        result.reserve(records.size());
        for (const auto &record: records) {
            result.push_back(record.getFieldId());
        }
        return result;
    }

    std::shared_ptr<Index> Class::tryGetOwnIndexedFieldIndex(FieldID field_id) const
    {
        openIndexedFields();
        auto long_index = field_id.getLongIndex();
        auto it = m_index_cache.find(long_index);
        if (it != m_index_cache.end()) {
            if (auto index = it->second.lock()) {
                return index;
            }
            m_index_cache.erase(it);
        }

        if (!m_indexed_fields.isNull()) {
            for (const auto &indexed_field: m_indexed_fields.cached()) {
                if (indexed_field.m_field_id == long_index) {
                    auto fixture = getFixture();
                    auto address = indexed_field.m_index_ptr.getAddress();
                    auto index = fixture->getVObjectCache().findOrPull<Index>(address, true, fixture, address);
                    m_index_cache[long_index] = index;
                    return index;
                }
            }
        }
        return nullptr;
    }

    std::shared_ptr<Index> Class::tryGetFieldIndex(FieldID field_id) const
    {
        auto index = tryGetOwnIndexedFieldIndex(field_id);
        if (index) {
            return index;
        }
        if (m_base_class_ptr) {
            return m_base_class_ptr->tryGetFieldIndex(field_id);
        }
        return nullptr;
    }

    std::shared_ptr<Index> Class::tryGetFieldIndex(const char *field_name) const
    {
        auto member_loc = findField(field_name);
        if (!!member_loc.first) {
            auto index = tryGetOwnIndexedFieldIndex(member_loc.first.primary().first);
            if (index) {
                return index;
            }
        }
        if (m_base_class_ptr) {
            return m_base_class_ptr->tryGetFieldIndex(field_name);
        }
        return nullptr;
    }

    std::shared_ptr<Index> Class::getExistingFieldIndex(FieldID field_id) const
    {
        auto index = tryGetFieldIndex(field_id);
        if (!index) {
            auto member = tryGetMember(field_id);
            if (member && isDeclaredIndexedField(member->m_name.c_str())) {
                const_cast<Class *>(this)->addIndexedField(field_id);
                index = tryGetFieldIndex(field_id);
            }
        }
        if (!index) {
            THROWF(db0::InputException) << "Field is not an indexed field";
        }
        return index;
    }
    
    std::string Class::getTypeName() const {
        return getFixture()->getLimitedStringPool().fetch((*this)->m_name);
    }
    
    std::optional<std::string> Class::tryGetModuleName() const 
    {
        if (!(*this)->m_module_name) {
            return std::nullopt;
        }
        return getFixture()->getLimitedStringPool().fetch((*this)->m_module_name);
    }

    std::string Class::getModuleName() const
    {
        auto module_name = tryGetModuleName();
        if (!module_name) {
            THROWF(db0::InternalException) << "Module name not found for class " << getTypeName();
        }
        return *module_name;
    }   
    
    std::optional<std::string> getNameVariant(std::optional<std::string> type_id, std::optional<std::string> type_name,
        std::optional<std::string> module_name, std::optional<std::string> type_fields_str, int variant_id)
    {        
        switch (variant_id) {
            case 0: {
                if (!type_id) {
                    return std::nullopt;                    
                }
                return type_id;
            }
            break;

            case 1: {
                // NOTE: module name may not be available in some contexts - e.g. 
                // dynamically loaded classes or jupyter notebook
                // FIXME: design handling missing module names
                assert(type_name);
                std::stringstream _str;
                _str << "cls:" << *type_name;
                if (module_name) {
                    _str << ".pkg:" << *module_name;
                }
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
    
    std::optional<std::string> getNameVariant(const Class &_class, int variant_id) {
        // FIXME: implement get type fields
        return getNameVariant(_class.getTypeId(), _class.getTypeName(), _class.tryGetModuleName(), std::nullopt, variant_id);
    }
    
    void Class::renameField(const char *from_name, const char *to_name)
    {   
        assert(from_name);
        assert(to_name);
        if (to_name == from_name) {
            return;
        }

        auto [member_id, was_init_var] = findField(from_name);
        if (!member_id) {
            // do not raise exception if the "to_name" field already exists (likely double rename attemp)
            if (std::get<0>(findField(to_name))) {
                return;
            }
            THROWF(db0::InputException) << "Field " << from_name << " not found in class " << getName();
        }
        
        // 1. Update in fields matrix
        auto &string_pool = getFixture()->getLimitedStringPool();
        // 2. Update in the in-memory index
        m_index.erase(from_name);
        // unreference old name in the string pool
        for (auto &field_info: member_id) {
            auto loc = field_info.first.getIndexAndOffset();
            string_pool.unRef(m_members.get(loc).m_name);
            m_members.modifyItem(loc).m_name = string_pool.addRef(to_name);
            // 3. Update in member's cache
            m_member_cache.reload(loc);
        }
    }
    
    void Class::detach() const
    {
        m_members.detach();
        m_member_cache.detach();
        m_fidelities.detach();
        m_schema.detach();
        m_tag_fields.detach();
        m_indexed_fields.detach();
        super_t::detach();
    }
    
    void Class::unlinkSingleton() {
        modify().m_singleton_address = {};
    }
    
    void Class::flush() const {
        m_schema.flush();
    }
    
    void Class::rollback() {
        m_schema.rollback();
    }

    void Class::commit() const
    {
        m_members.commit();        
        m_fidelities.commit();
        m_schema.commit();
        m_tag_fields.commit();
        m_indexed_fields.commit();
        super_t::commit();
    }
    
    bool Class::operator!=(const Class &rhs) const
    {
        if (*this->getFixture() != *rhs.getFixture()) {
            return true;
        }
        return this->getAddress() != rhs.getAddress();
    }
    
    bool Class::operator==(const Class &rhs) const
    {
        if (*this->getFixture() != *rhs.getFixture()) {
            return false;
        }
        return this->getAddress() == rhs.getAddress();
    }

    std::uint32_t Class::fetchUID() const
    {
        // return UID as relative address from the underlying SLOT
        auto result = this->getFixture()->makeRelative(this->getAddress(), SLOT_NUM);
        // relative address must not exceed SLOT size
        assert(result < std::numeric_limits<std::uint32_t>::max());
        return result;
    }
    
    bool Class::isExistingSingleton(std::uint64_t fixture_uuid) const
    {
        if (!isSingleton()) {
            return false;
        }

        auto &class_factory = getFixture()->get<ClassFactory>();
        auto lang_class = class_factory.getLangType(*this);
        auto other_fixture = getFixture()->getWorkspace().getFixture(fixture_uuid, AccessType::READ_ONLY);
        auto &other_factory = getClassFactory(*other_fixture);
        auto other_type = other_factory.tryGetExistingType(lang_class.get());
        return other_type && other_type->isExistingSingleton();
    }
    
    bool Class::unloadSingleton(void *at, std::uint64_t fixture_uuid) const
    {    
        auto &class_factory = getFixture()->get<ClassFactory>();
        auto lang_class = class_factory.getLangType(*this);
        auto other_fixture = getFixture()->getWorkspace().getFixture(fixture_uuid, AccessType::READ_ONLY);
        auto &other_factory = getClassFactory(*other_fixture);
        // locate corresponding type in the other fixture
        auto other_type = other_factory.tryGetExistingType(lang_class.get());
        if (!other_type) {
            return false;
        }
        return other_type->unloadSingleton(at);
    }
    
    ObjectId Class::getSingletonObjectId() const
    {
        if (!isSingleton() || !isExistingSingleton()) {
            THROWF(db0::InternalException) << "Singleton object not found for class " << getTypeName();
        }

        auto singleton_addr = (*this)->m_singleton_address;
        return {
            getFixture()->getUUID(),
            singleton_addr,
            StorageClass::OBJECT_REF
        };
    }
    
    ObjectId Class::getClassId() const
    {
        return {
            getFixture()->getUUID(),
            // NOTICE: no instance ID for the class-ref
            this->getUniqueAddress(),
            StorageClass::DB0_CLASS
        };
    }
    
    std::unordered_map<std::string, MemberID> Class::getMembers() const
    {              
        m_member_cache.refresh();
        std::unordered_map<std::string, MemberID> result;
        for (auto &item: m_index) {
            result[item.first] = std::get<0>(item.second);
        }

        return result;
    }
    
    std::shared_ptr<Class> Class::tryGetBaseClass() const {
        return m_base_class_ptr;
    }
    
    const Class *Class::getBaseClassPtr() const {
        return m_base_class_ptr.get();
    }

    void Class::setInitVars(const std::vector<std::string> &init_vars)
    {
        assert(m_init_vars.empty());        
        std::copy(init_vars.begin(), init_vars.end(), std::inserter(m_init_vars, m_init_vars.end()));        
        // update the field index
        for (auto &name: m_init_vars) {
            auto it = m_index.find(name);
            if (it != m_index.end()) {
                std::get<1>(it->second) = true;
            }            
        }
    }

    void Class::setDeclaredTagFields(const std::vector<std::string> &tag_fields)
    {
        for (auto it = m_declared_field_options.begin(); it != m_declared_field_options.end();) {
            it->second.clear(FieldOptions::TAG_FIELD);
            if (it->second.value() == 0) {
                it = m_declared_field_options.erase(it);
            } else {
                ++it;
            }
        }
        for (const auto &field_name: tag_fields) {
            setFieldOption(m_declared_field_options, field_name, FieldOptions::TAG_FIELD, true);
        }
    }

    void Class::setDeclaredIndexedFields(const std::vector<std::string> &indexed_fields)
    {
        for (auto it = m_declared_field_options.begin(); it != m_declared_field_options.end();) {
            it->second.clear(FieldOptions::INDEXED_FIELD);
            if (it->second.value() == 0) {
                it = m_declared_field_options.erase(it);
            } else {
                ++it;
            }
        }
        for (const auto &field_name: indexed_fields) {
            setFieldOption(m_declared_field_options, field_name, FieldOptions::INDEXED_FIELD, true);
        }
    }

    bool Class::isDeclaredTagField(const char *field_name) const
    {
        auto it = m_declared_field_options.find(field_name);
        if (it == m_declared_field_options.end()) {
            return false;
        }
        return it->second[FieldOptions::TAG_FIELD];
    }

    bool Class::isDeclaredIndexedField(const char *field_name) const
    {
        auto it = m_declared_field_options.find(field_name);
        if (it == m_declared_field_options.end()) {
            return false;
        }
        return it->second[FieldOptions::INDEXED_FIELD];
    }

    bool Class::hasDeclaredIndexedFields() const
    {
        for (const auto &item: m_declared_field_options) {
            if (item.second[FieldOptions::INDEXED_FIELD]) {
                return true;
            }
        }
        return false;
    }

    FieldFlags Class::getOwnFieldOptions(FieldID field_id) const
    {
        auto it = m_field_options.find(field_id.getLongIndex());
        if (it != m_field_options.end()) {
            return it->second;
        }
        for (const auto &entry: m_index) {
            const auto &member_id = entry.second.first;
            for (const auto &field_info: member_id) {
                if (field_info.first == field_id) {
                    it = m_field_options.find(member_id.primary().first.getLongIndex());
                    return it == m_field_options.end() ? FieldFlags() : it->second;
                }
            }
        }
        return {};
    }

    void Class::reloadMemberOptions(FieldID field_id) const
    {
        auto member = m_member_cache.tryGet(field_id.getIndexAndOffset());
        if (!member) {
            return;
        }
        auto it = m_index.find(member->m_name);
        if (it == m_index.end()) {
            m_member_cache.reload(field_id.getIndexAndOffset());
            return;
        }
        for (const auto &field_info: it->second.first) {
            m_member_cache.reload(field_info.first.getIndexAndOffset());
        }
    }

    FieldFlags Class::getFieldOptions(const char *field_name, const MemberID &member_id) const
    {
        FieldFlags result;
        if (!!member_id) {
            auto member = tryGetMember(member_id.primary().first);
            result = member ? member->m_field_options : getOwnFieldOptions(member_id.primary().first);
        } else {
            auto it = m_declared_field_options.find(field_name);
            if (it != m_declared_field_options.end()) {
                result = it->second;
            }
        }

        if (m_base_class_ptr) {
            result = result | m_base_class_ptr->getFieldOptions(field_name);
        }
        return result;
    }

    std::vector<std::string> Class::getTagFieldNames() const
    {
        openTagFields();
        if (m_tag_fields.isNull()) {
            return {};
        }

        std::vector<std::string> result;
        result.reserve(m_tag_fields.size());
        for (const auto &field_id: m_tag_fields.cached()) {
            auto member = tryGetMember(field_id);
            if (member) {
                result.push_back(member->m_name);
            }
        }
        return result;
    }

    std::vector<std::string> Class::getIndexedFieldNames() const
    {
        openIndexedFields();
        if (m_indexed_fields.isNull()) {
            return {};
        }

        std::vector<std::string> result;
        result.reserve(m_indexed_fields.size());
        for (const auto &indexed_field: m_indexed_fields.cached()) {
            auto member = tryGetMember(indexed_field.getFieldId());
            if (member) {
                result.push_back(member->m_name);
            }
        }
        return result;
    }

    Address Class::getSingletonAddress() const {
        return (*this)->m_singleton_address;
    }
    
    const std::unordered_set<std::string> &Class::getInitVars() const {
        return m_init_vars;
    }
    
    std::function<unsigned int()> Class::getTotalFunc() const
    {
        return [this]() {
            // NOTE: -1 because Class is also referenced (+1) by the ClassFactory
            return this->getRefCounts().second - 1;
        };
    }
    
    const Schema &Class::getSchema() const {
        return m_schema;
    }
    
    void Class::getSchema(std::function<void(const std::string &field_name, SchemaTypeId primary_type,
        const std::vector<SchemaTypeId> &all_types)> callback) const
    {
        m_member_cache.refresh();
        for (auto &item: m_index) {            
            try {
                auto member_id = item.second.first;
                auto primary_id = member_id.primary().first;
                callback(item.first, m_schema.getPrimaryType(primary_id), m_schema.getAllTypes(primary_id));
            } catch (const db0::InputException &) {
                // report as UNKNOWN type if the field ID is not found in the schema
                callback(item.first, SchemaTypeId::UNDEFINED, {});
            }
        }
    }
    
    void Class::updateSchema(FieldID field_id, unsigned int fidelity, SchemaTypeId old_type, SchemaTypeId new_type)
    {
        if (old_type == new_type) {
            // type not changed, nothing to do
            return;
        }
        
        // translate into a primary field ID if needed
        if (fidelity != PRIMARY_FIDELITY) {
            field_id = getPrimaryKey(field_id.getIndex());            
        }

        m_schema.remove(field_id, old_type);
        m_schema.add(field_id, new_type);
    }
    
    void Class::addToSchema(FieldID field_id, unsigned int fidelity, SchemaTypeId type_id)
    {
        if (fidelity != PRIMARY_FIDELITY) {
            field_id = getPrimaryKey(field_id.getIndex());
        }
        m_schema.add(field_id, type_id);
    }
    
    void Class::removeFromSchema(FieldID field_id, unsigned int fidelity, SchemaTypeId type_id)
    {
        if (fidelity != PRIMARY_FIDELITY) {
            field_id = getPrimaryKey(field_id.getIndex());
        }
        m_schema.remove(field_id, type_id);
    }

    void Class::addToSchema(const XValue &value) {
        addToSchema(value.getIndex(), value.m_type, value.m_value);
    }

    void Class::removeFromSchema(const XValue &value) {
        removeFromSchema(value.getIndex(), value.m_type, value.m_value);
    }
        
    std::uint32_t Class::getNumBases() const {
        return (*this)->m_num_bases;
    }
    
    UniqueAddress Class::getUniqueAddress() const {
        // NOTICE: no instance ID for the class-ref
        return { this->getAddress(), UniqueAddress::INSTANCE_ID_MAX };
    }

    bool Class::isDescendantOf(const Class &base) const
    {
        auto current_ptr = this;
        while (current_ptr && !(*current_ptr == base)) {
            current_ptr = current_ptr->getBaseClassPtr();
        }
        return current_ptr != nullptr;
    }
    
    bool Class::assignDefaultTags() const {
        return (*this)->m_flags[ClassOptions::NO_DEFAULT_TAGS] == false;
    }
    
    std::uint32_t Class::getClassRef() const {
        return classRef(*this, m_type_slot_addr_range);
    }

    const VFieldMatrix &Class::getMembersMatrix() const {
        return m_members;
    }
    
    Class::MemberAdapter::MemberAdapter(const Class &cls)
        : m_class(cls)
    {
    }
    
    Class::Member Class::MemberAdapter::operator()(std::pair<std::uint32_t, std::uint32_t> loc, const o_field &field) const
    {
        auto field_name = m_class.get().getFixture()->getLimitedStringPool().fetch(field.m_name);
        auto field_id = FieldID(loc);
        return { field_id, m_class.get().getFidelity(loc.first), field_name, m_class.get().getOwnFieldOptions(field_id) };
    }
    
    unsigned int Class::Member::getLongIndex() const {
        return m_field_id.getLongIndex();
    }
    
    unsigned int Class::getFidelity(std::uint32_t index) const
    {
        for (auto &item: m_fidelities) {
            if (item.second == index) {
                return item.first;
            }
        }
        // the default fidelity is 0
        return 0;
    }
    
    std::pair<std::uint32_t, std::uint32_t> Class::assignSlot(unsigned int fidelity)
    {
        if (fidelity == 0) {
            // just append a new member
            return { m_members.size().first, 0 };
        } else {
            // try allocating into an existing dimension first
            for (unsigned int index = 0; index < m_fidelities.size(); ++index) {
                if (m_fidelities[m_fidelities.size() - index - 1].first == fidelity) {
                    auto at = m_fidelities[m_fidelities.size() - index - 1].second;
                    auto maybe_offset = m_members.findUnassignedKey(at);
                    // unallocated slot found
                    if (maybe_offset) {
                        return { at, *maybe_offset };
                    }
                }
            }
            // allocate a new fidelity-specific column
            m_fidelities.push_back(std::make_pair<std::uint8_t, std::uint32_t>(fidelity, m_members.size().first));
            return { m_members.size().first, 0 };
        }
    }
    
    void Class::addToSchema(unsigned int index, StorageClass storage_class, Value value)
    {
        if (storage_class == StorageClass::UNDEFINED || storage_class == StorageClass::DELETED) {
            return;
        }        
        if (storage_class == StorageClass::PACK_2) {
            // iterate over all packed fields
            auto it = lofi_store<2>::fromValue(value).begin();
            for ( ; !it.isEnd(); ++it) {
                if (*it == Value::DELETED) {
                    continue;
                }
                m_schema.add(FieldID::fromIndex(index, it.getOffset()), 
                    getSchemaTypeId(storage_class, *it));
            }
        } else {
            // NOTE: we must retrieve the field's primary key
            m_schema.add(getPrimaryKey(index), getSchemaTypeId(storage_class));
        }
    }

    void Class::removeFromSchema(unsigned int index, StorageClass storage_class, Value value)
    {
        if (storage_class == StorageClass::UNDEFINED || storage_class == StorageClass::DELETED) {
            return;
        }        
        if (storage_class == StorageClass::PACK_2) {
            // iterate over all packed fields
            auto it = lofi_store<2>::fromValue(value).begin();
            for ( ; !it.isEnd(); ++it) {
                if (*it == Value::DELETED) {
                    continue;
                }
                // only remove non-deleted values
                m_schema.remove(FieldID::fromIndex(index, it.getOffset()), 
                    getSchemaTypeId(storage_class, *it));
            }
        } else {
            // NOTE: we must retrieve the field's primary key
            m_schema.remove(getPrimaryKey(index), getSchemaTypeId(storage_class));
        }
    }
    
    void Class::updateSchema(unsigned int first_id, const std::vector<StorageClass> &types,
        const std::vector<Value> &values, bool add)
    {
        assert(types.size() == values.size());
        auto type = types.begin();
        for (auto value: values) {
            if (add) {
                addToSchema(first_id, *type, value);
            } else {
                removeFromSchema(first_id, *type, value);
            }
            ++type;
            ++first_id;
        }
    }

    void Class::updateSchema(const XValue *begin, const XValue *end, bool add)
    {
        for (;begin != end; ++begin) {
            if (add) {
                addToSchema(*begin);
            } else {
                removeFromSchema(*begin);
            }            
        }
    }
    
    FieldID Class::getPrimaryKey(unsigned int index) const
    {
        auto resolvePrimaryKey = [this, index]() -> FieldID {
            for (const auto &entry: m_index) {
                const auto &memberId = entry.second.first;
                if (!memberId) {
                    continue;
                }
                if (memberId.size() > 1 && memberId.secondary().first.getIndex() == index) {
                    return memberId.primary().first;
                }
                if (memberId.primary().first.getIndex() == index) {
                    return memberId.primary().first;
                }
            }
            return {};
        };

        if (index >= m_unique_keys.size() || !m_unique_keys[index]) {
            m_member_cache.refresh();
        }
        if (index >= m_unique_keys.size() || !m_unique_keys[index]) {
            auto primaryKey = resolvePrimaryKey();
            if (!!primaryKey) {
                if (m_unique_keys.size() <= index) {
                    m_unique_keys.resize(index + 1);
                }
                m_unique_keys[index] = primaryKey;
            }
        }
        if (index >= m_unique_keys.size() || !m_unique_keys[index]) {
            // Destruction/schema cleanup must not read past the cache if the primary-key cache is stale.
            return FieldID::fromIndex(index);
        }
        return m_unique_keys[index];
    }
    
    void Class::setRuntimeFlags(FlagSet<MemoOptions> memo_options) {
        if (memo_options[MemoOptions::ACCESS_CONTROL]) {
            setAccessControl();
        }
    }

    bool Class::isBaseClass(const Class &other) const
    {
        auto current_ptr = this;
        while (current_ptr && !(*current_ptr == other)) {
            current_ptr = current_ptr->getBaseClassPtr();
        }
        return current_ptr != nullptr;
    }
    
}
