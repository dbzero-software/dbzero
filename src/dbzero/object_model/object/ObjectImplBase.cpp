// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "ObjectImplBase.hpp"
#include <random>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/serialization/string.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/class.hpp>
#include <dbzero/object_model/value.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/object/ObjectImmutableImpl.hpp>
#include <dbzero/object_model/list/List.hpp>
#include <dbzero/object_model/tags/TagIndex.hpp>
#include <dbzero/core/utils/uuid.hpp>

namespace db0::object_model

{
    
    FlagSet<AccessOptions> getAccessOptions(const Class &type) {
        return type.isNoCache() ? FlagSet<AccessOptions> { AccessOptions::no_cache } : FlagSet<AccessOptions> {};
    }
    
    template <typename IntT> IntT safeCast(unsigned int value, const char *err_msg)
    {
        if (value > std::numeric_limits<IntT>::max()) {
            THROWF(db0::InputException) << err_msg;
        }
        return static_cast<std::uint8_t>(value);
    }
    
    template <typename T, typename ImplT>
    ObjectImplBase<T, ImplT>::ObjectImplBase(tag_as_dropped, UniqueAddress addr, unsigned int ext_refs)
        : super_t(tag_as_dropped(), addr, ext_refs)
    {
    }
    
    template <typename T, typename ImplT>
    ObjectImplBase<T, ImplT>::ObjectImplBase(std::shared_ptr<Class> db0_class)    
    {
        // prepare for initialization
        InitManager::instance.addInitializer(*this, db0_class);
    }
    
    template <typename T, typename ImplT>
    ObjectImplBase<T, ImplT>::ObjectImplBase(TypeInitializer &&type_initializer)
    {
        // prepare for initialization
        InitManager::instance.addInitializer(*this, std::move(type_initializer));
    }

    template <typename T, typename ImplT>
    ObjectImplBase<T, ImplT>::ObjectImplBase(db0::swine_ptr<Fixture> &fixture, std::shared_ptr<Class> type,
        std::pair<std::uint32_t, std::uint32_t> ref_counts, const PosVT::Data &pos_vt_data, unsigned int pos_vt_offset)        
        : super_t(fixture, type->getClassRef(), ref_counts,
            safeCast<std::uint8_t>(type->getNumBases() + 1, "Too many base classes"), pos_vt_data, pos_vt_offset, nullptr, nullptr,
            getAccessOptions(*type))        
    {
        this->m_type = type;
    }

    template <typename T, typename ImplT>
    ObjectImplBase<T, ImplT>::ObjectImplBase(db0::swine_ptr<Fixture> &fixture, Address address, AccessFlags access_mode)
        : super_t(typename super_t::tag_from_address(), fixture, address, access_mode)
    {
    }
    
    template <typename T, typename ImplT>
    ObjectImplBase<T, ImplT>::ObjectImplBase(db0::swine_ptr<Fixture> &fixture, ObjectStem &&stem, std::shared_ptr<Class> type)
        : super_t(typename super_t::tag_from_stem(), fixture, std::move(stem))        
    {
        this->m_type = type;
        assert(hasValidClassRef());
    }

    template <typename T, typename ImplT>
    ObjectImplBase<T, ImplT>::ObjectImplBase(db0::swine_ptr<Fixture> &fixture, Address address, std::shared_ptr<Class> type_hint,
        with_type_hint, AccessFlags access_mode)
        : ObjectImplBase<T, ImplT>(fixture, address, access_mode)
    {   
        assert(*fixture == *type_hint->getFixture());
        setTypeWithHint(type_hint);
    }

    template <typename T, typename ImplT>
    ObjectImplBase<T, ImplT>::ObjectImplBase(db0::swine_ptr<Fixture> &fixture, ObjectStem &&stem, std::shared_ptr<Class> type_hint, with_type_hint)
        : ObjectImplBase<T, ImplT>(fixture, std::move(stem), getTypeWithHint(*fixture, stem->getClassRef(), type_hint))
    {
    }

    template <typename T, typename ImplT>
    ObjectImplBase<T, ImplT>::~ObjectImplBase()
    {
        // unregister needs to be called before destruction of members
        this->unregister();
        if (!this->hasInstance()) {
            // release initializer if it exists, object not created
            InitManager::instance.tryCloseInitializer(*this);
        }
    }
    
    template <typename T, typename ImplT>
    typename ObjectImplBase<T, ImplT>::ObjectStem ObjectImplBase<T, ImplT>::tryUnloadStem(db0::swine_ptr<Fixture> &fixture,
        Address address, std::uint16_t instance_id, AccessFlags access_mode)
    {
        std::size_t size_of;
        if (!fixture->isAddressValid(address, super_t::REALM_ID, &size_of)) {
            return {};
        }
        // Unload from a verified address
        ObjectStem stem(db0::tag_verified(), fixture->myPtr(address), size_of, access_mode);
        if (instance_id && stem->m_header.m_instance_id != instance_id) {
            // instance ID validation failed
            return {};
        }        
        return stem;
    }
    
    template <typename T, typename ImplT>
    typename ObjectImplBase<T, ImplT>::ObjectStem ObjectImplBase<T, ImplT>::unloadStem(db0::swine_ptr<Fixture> &fixture,
        Address address, std::uint16_t instance_id, AccessFlags access_mode)
    {
        auto result = tryUnloadStem(fixture, address, instance_id, access_mode);
        if (!result) {
            THROWF(db0::InputException) << "Invalid UUID or object has been deleted";
        }
        return result;
    }

    template <typename T, typename ImplT>
    void ObjectImplBase<T, ImplT>::postInit(FixtureLock &fixture)
    {
        if (!this->hasInstance()) {
            auto &initializer = InitManager::instance.getInitializer(*this);
            PosVT::Data pos_vt_data;
            unsigned int pos_vt_offset = 0;
            auto index_vt_data = initializer.getData(pos_vt_data, pos_vt_offset);
            
            // place object in the same fixture as its class
            // construct the dbzero instance & assign to self
            this->m_type = initializer.getClassPtr();
            assert(this->m_type);
            
            auto &type = *this->m_type;
            super_t::init(*fixture, type.getClassRef(), initializer.getRefCounts(),
                safeCast<std::uint8_t>(type.getNumBases() + 1, "Too many base classes"), 
                pos_vt_data, pos_vt_offset, index_vt_data.first, index_vt_data.second,
                getAccessOptions(type)
            );
            
            // reference associated class
            type.incRef(false);
            type.updateSchema(pos_vt_offset, pos_vt_data.m_types, pos_vt_data.m_values);
            type.updateSchema(index_vt_data.first, index_vt_data.second);
            
            // bind singleton address (now that instance exists)
            if (type.isSingleton()) {
                type.setSingletonAddress(*this);
            }
            initializer.close();            
        }
        
        assert(this->hasInstance());
    }
    
    template <typename T, typename ImplT>
    std::pair<db0::bindings::TypeId, StorageClass>
    ObjectImplBase<T, ImplT>::recognizeType(Fixture &fixture, ObjectPtr lang_value) const
    {
        auto type_id = LangToolkit::getTypeManager().getTypeId(lang_value);
        // NOTE: allow storage as PACK_2
        auto pre_storage_class = TypeUtils::m_storage_class_mapper.getPreStorageClass(type_id, true);
        if (type_id == TypeId::MEMO_OBJECT || type_id == TypeId::MEMO_IMMUTABLE_OBJECT) {
            // object reference must be from the same fixture
            auto &obj = LangToolkit::getTypeManager().extractAnyObject(lang_value);
            if (fixture.getUUID() != obj.getFixture()->getUUID()) {
                THROWF(db0::InputException) << "Referencing objects from foreign prefixes is not allowed. Use db0.weak_proxy instead";
            }
        }
        
        // may need to refine the storage class (i.e. long weak ref might be needed instead)
        StorageClass storage_class;
        if (pre_storage_class == PreStorageClass::OBJECT_WEAK_REF) {
            storage_class = db0::getStorageClass(pre_storage_class, fixture, lang_value);
        } else {
            storage_class = db0::getStorageClass(pre_storage_class);
        }
        
        return { type_id, storage_class };
    }    
    
    template <typename T, typename ImplT>
    void ObjectImplBase<T, ImplT>::removePreInit(const char *field_name) const
    {
        auto &initializer = InitManager::instance.getInitializer(*this);
        auto &type = initializer.getClass();
        
        // Find an already existing field index
        auto member_id = std::get<0>(type.findField(field_name));
        if (!member_id) {
            THROWF(db0::InputException) << "Attribute not found: " << field_name;
        }
        
        for (const auto &field_info: member_id) {
            assert(field_info.first);            
            auto loc = field_info.first.getIndexAndOffset();
            // mark as deleted
            if (field_info.second == 0) {
                initializer.set(loc, StorageClass::DELETED, {});
            } else {
                assert(field_info.second == 2 && "Only fidelity == 2 is supported");
                if (member_id.hasFidelity(0)) {
                    // remove any existing regular initialization
                    auto loc0 = member_id.get(0).getIndexAndOffset();
                    initializer.remove(loc0);
                }                
                initializer.set(loc, StorageClass::PACK_2, Value::DELETED,
                    lofi_store<2>::mask(loc.second));
            }
        }
    }
    
    template <typename T, typename ImplT>
    void ObjectImplBase<T, ImplT>::setPreInit(const char *field_name, ObjectPtr obj_ptr) const
    {
        assert(!this->hasInstance());
        if (!LangToolkit::isValid(obj_ptr)) {
            removePreInit(field_name);
            return;
        }

        auto &initializer = InitManager::instance.getInitializer(*this);
        auto fixture = initializer.getFixture();
        auto &type = initializer.getClass();
        auto [type_id, storage_class] = recognizeType(*fixture, obj_ptr);
        auto storage_fidelity = getStorageFidelity(storage_class);
        
        // Find an already existing field index
        auto [member_id, is_init_var] = type.findField(field_name);
        // NOTE: even if a field already exists we might need to extend its supported fidelities
        if (!member_id || !member_id.hasFidelity(storage_fidelity)) {
            // update class definition
            // use the default fidelity for the storage class
            member_id = type.addField(field_name, storage_fidelity);
        }
        
        if (storage_fidelity == 0) {
            if (member_id.hasFidelity(2)) {
                // remove any existing lo-fi initialization
                auto loc = member_id.get(2).getIndexAndOffset();
                initializer.remove(loc, lofi_store<2>::mask(loc.second));
            }
            // register a regular member with the initializer
            // NOTE: a new member receives the no-cache flag if set (at the type level)
            auto member_flags = type.isNoCache() ? AccessFlags { AccessOptions::no_cache } : AccessFlags();
            initializer.set(member_id.get(0).getIndexAndOffset(), storage_class,
                createMember<LangToolkit>(fixture, type_id, storage_class, obj_ptr, member_flags)
            );
        } else {
            if (member_id.hasFidelity(0)) {
                // remove any existing regular initialization
                auto loc = member_id.get(0).getIndexAndOffset();
                initializer.remove(loc);
            }
            // For now only fidelity == 2 is supported (lo-fi storage)
            assert(storage_fidelity == 2);
            auto loc = member_id.get(storage_fidelity).getIndexAndOffset();
            // no access flags for lo-fi members
            auto value = lofi_store<2>::create(loc.second, 
                createMember<LangToolkit>(fixture, type_id, storage_class, obj_ptr, {}).m_store);
            // register a lo-fi member with the initializer (using mask)
            initializer.set(loc, storage_class, value, lofi_store<2>::mask(loc.second));
        }
    }
    
    template <typename T, typename ImplT>
    void ObjectImplBase<T, ImplT>::unrefPosVT(FixtureLock &fixture, FieldID field_id, unsigned int pos, 
        StorageClass storage_class, unsigned int fidelity)
    {
        auto &pos_vt = this->modify().pos_vt();
        auto old_storage_class = pos_vt.types()[pos];
        if (fidelity == 0) {
            unrefMember(*fixture, old_storage_class, pos_vt.values()[pos]);
            // mark member as unreferenced by assigning storage class
            pos_vt.set(pos, storage_class, {});
            this->m_type->removeFromSchema(field_id, fidelity, getSchemaTypeId(old_storage_class));
        } else {
            assert(fidelity == 2);
            auto value = pos_vt.values()[pos];
            auto offset = field_id.getOffset();
            if (storage_class != StorageClass::DELETED && !lofi_store<2>::fromValue(value).isSet(offset)) {
                // value is already unset
                return;
            }

            auto old_type_id = getSchemaTypeId(old_storage_class, lofi_store<2>::fromValue(value).get(offset));
            // either reset or mark as deleted
            if (storage_class == StorageClass::DELETED) {
                lofi_store<2>::fromValue(value).set(offset, Value::DELETED);
            } else {
                lofi_store<2>::fromValue(value).reset(offset);
            }
            pos_vt.set(pos, old_storage_class, value);
            this->m_type->removeFromSchema(field_id, fidelity, old_type_id);
        }
    }
    
    template <typename T, typename ImplT>
    void ObjectImplBase<T, ImplT>::unrefIndexVT(FixtureLock &fixture, FieldID field_id, unsigned int index_vt_pos,
        StorageClass storage_class, unsigned int fidelity)
    {
        auto &index_vt = this->modify().index_vt();
        auto old_storage_class = index_vt.xvalues()[index_vt_pos].m_type;
        if (fidelity == 0) {
            unrefMember(*fixture, index_vt.xvalues()[index_vt_pos]);
            // mark member as unreferenced by assigning storage class
            index_vt.set(index_vt_pos, storage_class, {});
            this->m_type->removeFromSchema(field_id, fidelity, getSchemaTypeId(old_storage_class));
        } else {
            assert(fidelity == 2);
            auto value = index_vt.xvalues()[index_vt_pos].m_value;
            auto offset = field_id.getOffset();
            if (storage_class != StorageClass::DELETED && !lofi_store<2>::fromValue(value).isSet(offset)) {
                // value is already unset
                return;
            }
            auto old_type_id = getSchemaTypeId(old_storage_class, lofi_store<2>::fromValue(value).get(offset));
            if (storage_class == StorageClass::DELETED) {
                lofi_store<2>::fromValue(value).set(offset, Value::DELETED);
            } else {
                lofi_store<2>::fromValue(value).reset(offset);
            }            
            index_vt.set(index_vt_pos, old_storage_class, value);
            this->m_type->removeFromSchema(field_id, fidelity, old_type_id);
        }
    }
    
    template <typename T, typename ImplT>
    void ObjectImplBase<T, ImplT>::unrefWithLoc(FixtureLock &fixture, FieldID field_id, const void *loc_ptr, unsigned int pos,
        StorageClass storage_class, unsigned int fidelity)
    {
        // call the actual implementation
        static_cast<ImplT*>(this)->tryUnrefWithLoc(fixture, field_id, loc_ptr, pos, storage_class, fidelity);
    }
    
    template <typename T, typename ImplT>
    bool ObjectImplBase<T, ImplT>::tryUnrefWithLoc(FixtureLock &fixture, FieldID field_id, const void *loc_ptr, unsigned int pos,
        StorageClass storage_class, unsigned int fidelity)
    {
        if (loc_ptr == &(*this)->pos_vt()) {            
            unrefPosVT(fixture, field_id, pos, storage_class, fidelity);
            return true;
        } else if (loc_ptr == &(*this)->index_vt()) {
            unrefIndexVT(fixture, field_id, pos, storage_class, fidelity);
            return true;
        }
        return false;
    }
    
    template <typename T, typename ImplT>
    bool ObjectImplBase<T, ImplT>::tryFindMemberSlot(const std::pair<FieldID, unsigned int> &field_info, unsigned int &pos, 
        std::pair<FieldInfo, const void *> &result) const
    {
        auto [index, offset] = field_info.first.getIndexAndOffset();
        // pos-vt lookup
        if ((*this)->pos_vt().find(index, pos)) {
            if (field_info.second == 0 || slotExists((*this)->pos_vt().values()[pos], field_info.second, offset)) {                    
                result = { field_info, &(*this)->pos_vt() };
            }
            return true;
        }
        
        // index-vt lookup
        if ((*this)->index_vt().find(index, pos)) {
            if (field_info.second == 0 || slotExists((*this)->index_vt().xvalues()[pos].m_value, field_info.second, offset)) {
                result = { field_info, &(*this)->index_vt() };
            }
            return true;
        
        }
        // not found but the lookup may be continued in the kv-index
        return false;
    }
    
    template <typename T, typename ImplT> std::pair<FieldInfo, const void *>
    ObjectImplBase<T, ImplT>::tryGetMemberSlot(const MemberID &member_id, unsigned int &pos) const
    {
        std::pair<FieldInfo, const void *> result;
        for (auto &field_info: member_id) {
            // call the actual implementation
            if (static_cast<const ImplT*>(this)->tryFindMemberSlot(field_info, pos, result)) {
                // otherwise continue since member might've been deleted and reassigned to a different slot
                if (result.first.first) {
                    return result;
                }
            }
        }
        
        // not found or deleted
        return { {}, nullptr };
    }
    
    template <typename T, typename ImplT>
    std::pair<const void*, unsigned int> ObjectImplBase<T, ImplT>::tryGetLoc(FieldID field_id) const
    {
        auto index = field_id.getIndex();
        unsigned int pos = 0;
        // pos-vt lookup
        if ((*this)->pos_vt().find(index, pos)) {
            return { &(*this)->pos_vt(), pos };
        }
        // index-vt lookup
        if ((*this)->index_vt().find(index, pos)) {
            return { &(*this)->index_vt(), pos };
        }
        // not found or located in the kv-index
        return { nullptr, 0 };
    }
    
    template <typename T, typename ImplT>
    std::pair<MemberID, bool> ObjectImplBase<T, ImplT>::findField(const char *name) const
    {
        if (this->isDropped()) {
            // defunct objects should not be accessed
            assert(!this->isDefunct());
            THROWF(db0::InputException) << "Object does not exist";
        }
        
        auto class_ptr = this->m_type.get();
        if (!class_ptr) {
            // retrieve class from the initializer
            class_ptr = &InitManager::instance.getInitializer(*this).getClass();
        }

        assert(class_ptr);
        return class_ptr->findField(name);
    }

    template <typename T, typename ImplT>
    FieldID ObjectImplBase<T, ImplT>::tryGetMember(const char *field_name, std::pair<StorageClass, Value> &member,
        bool &is_init_var) const
    {
        MemberID member_id;
        std::tie(member_id, is_init_var) = this->findField(field_name);        
        bool exists, deleted = false;
        if (member_id) {
            std::tie(exists, deleted) = tryGetMemberAt(member_id.primary(), member);
            if (exists) {
                assert(!deleted);
                return member_id.primary().first;
            }
            
            // the primary slot was not occupied, try with the secondary
            bool secondary_deleted = false;
            std::tie(exists, secondary_deleted) = tryGetMemberAt(member_id.secondary(), member);
            if (exists) {
                assert(!secondary_deleted);
                return member_id.secondary().first;
            }

            deleted |= secondary_deleted;
        }
        
        if (is_init_var) {
            // unless explicitly deleted, 
            // report as None even if the field_id has not been assigned yet
            member = { deleted ? StorageClass::DELETED : StorageClass::NONE, Value() };
        }

        // member not found
        return {};
    }
    
    template <typename T, typename ImplT>
    std::optional<XValue> ObjectImplBase<T, ImplT>::tryGetX(const char *field_name) const
    {
        auto [member_id, is_init_var] = this->findField(field_name);
        bool exists, deleted = false;
        if (member_id) {
            assert(member_id.primary().first);
            std::pair<StorageClass, Value> member;
            std::tie(exists, deleted) = tryGetMemberAt(member_id.primary(), member);
            if (exists) {
                assert(!deleted);
                return XValue(member_id.primary().first.getIndex(), member.first, member.second);
            }
            // the primary slot was not occupied, try with the secondary
            bool secondary_deleted = false;
            std::tie(exists, secondary_deleted) = tryGetMemberAt(member_id.secondary(), member);
            if (exists) {
                assert(!secondary_deleted);
                return XValue(member_id.secondary().first.getIndex(), member.first, member.second);
            }
            deleted |= secondary_deleted;
        }

        if (!deleted && is_init_var) {
            // unless explicitly deleted,
            // report as None even if the field_id has not been assigned yet
            return XValue(member_id.primary().first.getIndex(), StorageClass::NONE, Value());
        }
        
        return std::nullopt;
    }

    template <typename T, typename ImplT>
    typename ObjectImplBase<T, ImplT>::ObjectSharedPtr ObjectImplBase<T, ImplT>::tryGet(const char *field_name) const
    {
        std::pair<StorageClass, Value> member;
        bool is_init_var = false;
        auto field_id = tryGetMember(field_name, member, is_init_var);
        // NOTE: init vars are always reported as None if not explicitly set nor explicitly deleted
        if (field_id || (is_init_var && member.first != StorageClass::DELETED)) {
            auto fixture = this->getFixture();
            // prevent accessing a deleted or undefined member
            assert(member.first != StorageClass::DELETED && member.first != StorageClass::UNDEFINED);        
            // NOTE: offset is required for lo-fi members
            return unloadMember<LangToolkit>(
                fixture, member.first, member.second, field_id.maybeOffset(), this->getMemberFlags()
            );
        }
        
        return nullptr;
    }
    
    template <typename T, typename ImplT>
    typename ObjectImplBase<T, ImplT>::ObjectSharedPtr ObjectImplBase<T, ImplT>::tryGetAs(
        const char *field_name, TypeObjectPtr lang_type) const
    {
        std::pair<StorageClass, Value> member;
        bool is_init_var = false;
        auto field_id = tryGetMember(field_name, member, is_init_var);
        if (field_id || (is_init_var && member.first != StorageClass::DELETED)) {
            // prevent accessing a deleted member
            assert(member.first != StorageClass::DELETED && member.first != StorageClass::UNDEFINED);
            auto fixture = this->getFixture();
            if (member.first == StorageClass::OBJECT_REF) {
                auto &class_factory = getClassFactory(*fixture);
                return PyToolkit::unloadObject(fixture, member.second.asAddress(), class_factory, lang_type);
            }
            
            // NOTE: offset is required for lo-fi members
            return unloadMember<LangToolkit>(
                fixture, member.first, member.second, field_id.getOffset(), this->getMemberFlags()
            );
        }

        return nullptr;
    }
    
    template <typename T, typename ImplT>
    typename ObjectImplBase<T, ImplT>::ObjectSharedPtr ObjectImplBase<T, ImplT>::get(const char *field_name) const
    {
        auto obj = tryGet(field_name);
        if (!obj) {
            if (this->isDropped()) {
                THROWF(db0::InputException) << "Object is no longer accessible";
            }
            THROWF(db0::InputException) << "Attribute not found: " << field_name;
        }
        return obj;
    }

    template <typename T, typename ImplT>
    bool ObjectImplBase<T, ImplT>::slotExists(Value value, unsigned int fidelity, unsigned int at) const
    {
        assert(fidelity != 0 && "Operation only available for lo-fi values");
        // lo-fi value
        assert(fidelity == 2);
        return lofi_store<2>::fromValue(value).isSet(at);
    }

    template <typename T, typename ImplT>
    std::pair<bool, bool> ObjectImplBase<T, ImplT>::hasValueAt(Value value, unsigned int fidelity, unsigned int at) const
    {
        assert(fidelity != 0 && "Operation only available for lo-fi values");
        // lo-fi value
        assert(fidelity == 2);
        if (lofi_store<2>::fromValue(value).isSet(at)) {
            // might be deleted
            bool deleted = (lofi_store<2>::fromValue(value).get(at) == Value::DELETED);
            return { !deleted, deleted };
        } else {
            // NOTE: unset value is assumed as empty / undefined
            return { false, false };
        }
    }
    
    template <typename T, typename ImplT>
    bool ObjectImplBase<T, ImplT>::tryFindMemberAt(std::pair<FieldID, unsigned int> field_info,
        std::pair<StorageClass, Value> &result, std::pair<bool, bool> &find_result) const
    {
        if (!field_info.first) {
            find_result = { false, false };
            return true;
        }

        auto loc = field_info.first.getIndexAndOffset();
        if (!this->hasInstance()) {
            // try retrieving from initializer
            auto initializer_ptr = InitManager::instance.findInitializer(*this);
            if (!initializer_ptr) {
                find_result = { false, false };
                return true;
            }
            find_result = { initializer_ptr->tryGetAt(loc, result), false };
            return true;
        }
        
        // retrieve from positionally encoded values
        if ((*this)->pos_vt().find(loc.first, result)) {
            // NOTE: removed field slots might be marked as DELETED            
            if (result.first == StorageClass::DELETED) {
                // report as deleted
                find_result = { false, true };
                return true;
            }
            
            if (field_info.second == 0) {
                find_result = { result.first != StorageClass::UNDEFINED, false };                
            } else {
                find_result = hasValueAt(result.second, field_info.second, loc.second);                
            }
            return true;
        }
        
        if ((*this)->index_vt().find(loc.first, result)) {
            if (result.first == StorageClass::DELETED) {
                // report as deleted
                find_result = { false, true };
                return true;
            }
            
            if (field_info.second == 0) {
                find_result = { result.first != StorageClass::UNDEFINED, false };
            } else {
                find_result = hasValueAt(result.second, field_info.second, loc.second);
            }
            return true;
        }
        
        return false;
    }

    template <typename T, typename ImplT>
    std::pair<bool, bool> ObjectImplBase<T, ImplT>::tryGetMemberAt(std::pair<FieldID, unsigned int> field_info,
        std::pair<StorageClass, Value> &result) const
    {
        std::pair<bool, bool> find_result;
        if (static_cast<const ImplT*>(this)->tryFindMemberAt(field_info, result, find_result)) {
            return find_result;
        }
        // Does not exist, not explicitly removed
        return { false, false };    
    }
    
    template <typename T, typename ImplT>
    void ObjectImplBase<T, ImplT>::setType(std::shared_ptr<Class> type)
    {
        assert(!this->m_type);
        this->m_type = type;
        assert(hasValidClassRef());
    }

    template <typename T, typename ImplT>
    void ObjectImplBase<T, ImplT>::setTypeWithHint(std::shared_ptr<Class> type_hint)
    {
        assert(!this->m_type);
        assert(type_hint);
        assert(this->hasInstance());
        if (type_hint->getClassRef() == (*this)->getClassRef()) {
            this->m_type = type_hint;
        } else {
            this->m_type = unloadType();
        }
    }

    template <typename T, typename ImplT>
    void ObjectImplBase<T, ImplT>::dropTags(Class &type) const
    {
        // only drop if any type tags are assigned
        if ((*this)->m_header.m_ref_counter.getFirst() > 0) {
            auto fixture = this->getFixture();
            assert(fixture);
            auto &tag_index = fixture->template get<TagIndex>();
            const Class *type_ptr = &type;
            auto unique_address = this->getUniqueAddress();
            while (type_ptr) {
                // remove auto-assigned type (or its base) tag
                tag_index.removeTypeTag(unique_address, type_ptr->getAddress());
                // NOTE: no need to decRef since object is being destroyed
                type_ptr = type_ptr->getBaseClassPtr();
            }
        }
    }

    template <typename T, typename ImplT>
    void ObjectImplBase<T, ImplT>::dropMembers(Class &class_ref) const
    {
        auto fixture = this->getFixture();
        assert(fixture);
        // call the actual implementation
        static_cast<const ImplT*>(this)->dropMembers(fixture, class_ref);
    }
    
    template <typename T, typename ImplT>
    void ObjectImplBase<T, ImplT>::dropMembers(db0::swine_ptr<Fixture> &fixture, Class &class_ref) const
    {
        // drop pos-vt members first
        {
            auto &types = (*this)->pos_vt().types();
            auto &values = (*this)->pos_vt().values();
            auto value = values.begin();
            unsigned int index = types.offset();
            for (auto type = types.begin(); type != types.end(); ++type, ++value, ++index) {
                if (*type == StorageClass::DELETED || *type == StorageClass::UNDEFINED) {
                    // skip undefined or deleted members
                    continue;
                }
                unrefMember(fixture, *type, *value);
                class_ref.removeFromSchema(index, *type, *value);
            }
        }
        // drop index-vt members next
        {
            auto &xvalues = (*this)->index_vt().xvalues();
            for (auto &xvalue: xvalues) {
                if (xvalue.m_type == StorageClass::DELETED || xvalue.m_type == StorageClass::UNDEFINED) {
                    // skip undefined or deleted members
                    continue;
                }
                unrefMember(fixture, xvalue);
                class_ref.removeFromSchema(xvalue);
            }
        }
    }
    
    template <typename T, typename ImplT>
    void ObjectImplBase<T, ImplT>::destroy()
    {
        if (this->hasInstance()) {
            // associated class type (may require unloading)
            auto type = this->m_type;
            if (!type) {
                // retrieve type from the initializer
                type = std::const_pointer_cast<Class>(unloadType());
            }
            
            dropTags(*type);
            dropMembers(*type);
            // dereference associated class
            type->decRef(false);
        }
        super_t::destroy();
    }
    
    template <typename T, typename ImplT>
    FieldLayout ObjectImplBase<T, ImplT>::getFieldLayout() const
    {
        FieldLayout layout;
        // call the actual implementation
        static_cast<const ImplT*>(this)->getFieldLayoutImpl(layout);
        return layout;
    }
    
    template <typename T, typename ImplT>
    void ObjectImplBase<T, ImplT>::getFieldLayoutImpl(FieldLayout &layout) const
    {        
        // collect pos-vt information                
        for (auto type: (*this)->pos_vt().types()) {
            layout.m_pos_vt_fields.push_back(type);
        }
        
        // collect index-vt information        
        for (auto &xvalue: (*this)->index_vt().xvalues()) {
            layout.m_index_vt_fields.emplace_back(xvalue.getIndex(), xvalue.m_type);
        }
    }
    
    template <typename T, typename ImplT>
    void ObjectImplBase<T, ImplT>::getMembersFrom(const Class &this_type, unsigned int index, StorageClass storage_class,
        Value value, std::unordered_set<std::string> &result) const
    {
        if (storage_class == StorageClass::DELETED || storage_class == StorageClass::UNDEFINED) {
            // skip undefined or deleted members
            return;
        }

        if (storage_class == StorageClass::PACK_2) {
            auto it = lofi_store<2>::fromValue(value).begin(), end = lofi_store<2>::fromValue(value).end();
            for (; it != end; ++it) {
                result.insert(this_type.getMember({ index, it.getOffset() }).m_name);
            }
        } else {
            result.insert(this_type.getMember(FieldID::fromIndex(index)).m_name);
        }
    }

    template <typename T, typename ImplT>
    std::unordered_set<std::string> ObjectImplBase<T, ImplT>::getMembers() const
    {
        std::unordered_set<std::string> result;
        // call the actual implementation
        static_cast<const ImplT*>(this)->getMembersImpl(result);
        return result;
    }
    
    template <typename T, typename ImplT>
    void ObjectImplBase<T, ImplT>::getMembersImpl(std::unordered_set<std::string> &result) const
    {
        // Visit pos-vt members first
        auto &obj_type = this->getType();
        {
            auto &types = (*this)->pos_vt().types();
            auto &values = (*this)->pos_vt().values();
            unsigned int index = types.offset();
            auto size = types.size();
            for (unsigned int pos = 0;pos < size; ++index, ++pos) {
                getMembersFrom(obj_type, index, types[pos], values[pos], result);
            }
        }
        
        // Visit index-vt members next
        {
            auto &xvalues = (*this)->index_vt().xvalues();
            for (auto &xvalue: xvalues) {
                auto index = xvalue.getIndex();
                getMembersFrom(obj_type, index, xvalue.m_type, xvalue.m_value, result);
            }
        }        
    }

    template <typename T, typename ImplT>
    void ObjectImplBase<T, ImplT>::forAll(std::function<bool(const std::string &, const XValue &, unsigned int)> f) const
    {
        // call the actual implementation
        static_cast<const ImplT*>(this)->forAllImpl(f);
    }   
    
    template <typename T, typename ImplT>
    bool ObjectImplBase<T, ImplT>::forAllImpl(std::function<bool(const std::string &, const XValue &, unsigned int)> f) const
    {
        // Visit pos-vt members first
        auto &obj_type = this->getType();
        {
            auto &types = (*this)->pos_vt().types();
            auto &values = (*this)->pos_vt().values();
            auto value = values.begin();
            unsigned int index = types.offset();
            for (auto type = types.begin(); type != types.end(); ++type, ++value, ++index) {
                if (*type == StorageClass::DELETED || *type == StorageClass::UNDEFINED) {
                    // skip deleted or undefined members
                    continue;
                }
                if (*type == StorageClass::PACK_2) {
                    // iterate individual lo-fi members
                    if (!forAll({index, *type, *value}, f)) {
                        return false;
                    }
                } else {
                    if (!f(obj_type.getMember(FieldID::fromIndex(index)).m_name, { index, *type, *value }, 0)) {
                        return false;
                    }
                }
            }
        }

        // Visit index-vt members next
        {
            auto &xvalues = (*this)->index_vt().xvalues();
            for (auto &xvalue: xvalues) {  
                if (xvalue.m_type == StorageClass::DELETED || xvalue.m_type == StorageClass::UNDEFINED) {
                    // skip deleted or undefined members                    
                    continue;
                }              
                if (xvalue.m_type == StorageClass::PACK_2) {
                    // iterate individual lo-fi members
                    if (!forAll(xvalue, f)) {
                        return false;
                    }
                } else {
                    // regular member
                    if (!f(obj_type.getMember(FieldID::fromIndex(xvalue.getIndex())).m_name, xvalue, 0)) {
                        return false;
                    }
                }
            }
        }
        
        // Continue with kv-index members if any
        return true;
    }

    template <typename T, typename ImplT>
    void ObjectImplBase<T, ImplT>::forAll(std::function<bool(const std::string &, ObjectSharedPtr)> f) const
    {
        auto fixture = this->getFixture();
        forAll([&](const std::string &name, const XValue &xvalue, unsigned int offset) -> bool {
            // all references convert to UUID
            auto py_member = unloadMember<LangToolkit>(
                fixture, xvalue.m_type, xvalue.m_value, offset, this->getMemberFlags()
            );
            return f(name, py_member);
        });
    }

    template <typename T, typename ImplT>
    bool ObjectImplBase<T, ImplT>::forAll(XValue xvalue, 
        std::function<bool(const std::string &, const XValue &, unsigned int offset)> f) const
    {
        assert(xvalue.m_type == StorageClass::PACK_2);
        unsigned int index = xvalue.getIndex();
        auto _value = xvalue.m_value;
        auto it = lofi_store<2>::fromValue(_value).begin(), end = lofi_store<2>::fromValue(_value).end();
        auto &obj_type = this->getType();
        for (; it != end; ++it) {
            if (!f(obj_type.getMember(FieldID::fromIndex(index, it.getOffset())).m_name,
                xvalue, it.getOffset()))
            {
                return false;
            }
        }
        return true;
    }

    template <typename T, typename ImplT>
    bool ObjectImplBase<T, ImplT>::tryEqualToImpl(const ObjectImplBase<T, ImplT> &other, bool &result) const
    {
        if (!this->hasInstance() || !other.hasInstance()) {
            THROWF(db0::InputException) << "Object not initialized";
        }
        
        if (this->isDefunct() || other.isDefunct()) {
            // defunct objects should not be compared
            assert(!this->isDefunct());
            THROWF(db0::InputException) << "Object does not exist";
        }

        if ((*this)->getClassRef() != other->getClassRef()) {
            // different types
            result = false;
            return true;
        }

        if (this->getFixture()->getUUID() == other.getFixture()->getUUID() 
            && this->getUniqueAddress() == other.getUniqueAddress()) 
        {
            // comparing 2 versions of the same object (fastest)
            if (!((*this)->pos_vt() == other->pos_vt())) {
                result = false;
                return true;
            }
            if (!((*this)->index_vt() == other->index_vt())) {
                result = false;
                return true;
            }
        }
        // unable to determine
        return false;
    }
    
    template <typename T, typename ImplT>
    bool ObjectImplBase<T, ImplT>::equalTo(const ObjectImplBase<T, ImplT> &other) const
    {
        bool result;
        if (static_cast<const ImplT*>(this)->tryEqualToImpl(other, result)) {
            return result;
        }
        
        // field-wise compare otherwise (slower)
        result = true;
        this->forAll([&](const std::string &name, const XValue &xvalue, unsigned int offset) -> bool {
            auto maybe_other_value = other.tryGetX(name.c_str());
            if (!maybe_other_value) {
                result = false;
                return false;
            }
            
            if (!xvalue.equalTo(*maybe_other_value, offset)) {
                result = false;
                return false;
            }
            return true;
        });
        return result;
    }    
    
    template <typename T, typename ImplT>
    void ObjectImplBase<T, ImplT>::moveTo(db0::swine_ptr<Fixture> &) {
        throw std::runtime_error("Not implemented");
    }
    
    template <typename T, typename ImplT>
    void ObjectImplBase<T, ImplT>::detach() const
    {
        this->m_type->detach();
        // invalidate since detach is not supported by the MorphingBIndex
        this->m_kv_index = nullptr;
        super_t::detach();
    }
    
    template <typename T, typename ImplT>
    void ObjectImplBase<T, ImplT>::commit() const
    {
        this->m_type->commit();
        if (m_kv_index) {
            m_kv_index->commit();
        }
        super_t::commit();
        // reset the silent-mutation flag
        this->m_touched = false;
    }

    template <typename T, typename ImplT>
    void ObjectImplBase<T, ImplT>::unrefMember(db0::swine_ptr<Fixture> &fixture, StorageClass type, Value value) const {
        db0::object_model::unrefMember<LangToolkit>(fixture, type, value);
    }
    
    template <typename T, typename ImplT>
    void ObjectImplBase<T, ImplT>::unrefMember(db0::swine_ptr<Fixture> &fixture, XValue value) const {
        db0::object_model::unrefMember<LangToolkit>(fixture, value.m_type, value.m_value);
    }

    template <typename T, typename ImplT>
    std::shared_ptr<Class> ObjectImplBase<T, ImplT>::unloadType() const
    {
        auto fixture = this->getFixture();
        return getClassFactory(*fixture).getTypeByClassRef((*this)->getClassRef()).m_class;
    }

    template <typename T, typename ImplT>
    bool ObjectImplBase<T, ImplT>::hasValidClassRef() const
    {
        if (this->hasInstance() && this->m_type) {
            return (*this)->getClassRef() == this->m_type->getClassRef();
        }
        return true;
    }

    template <typename T, typename ImplT>
    std::shared_ptr<Class> ObjectImplBase<T, ImplT>::getTypeWithHint(const Fixture &fixture, std::uint32_t class_ref, 
        std::shared_ptr<Class> type_hint)
    {
        assert(type_hint);
        if (type_hint->getClassRef() == class_ref) {
            return type_hint;
        }
        return getClassFactory(fixture).getTypeByClassRef(class_ref).m_class;
    }
    
    template <typename T, typename ImplT>
    bool ObjectImplBase<T, ImplT>::hasRefs() const
    {
        assert(this->hasInstance());
        return (*this)->hasRefs();
    }
    
    template class ObjectImplBase<o_object, Object>;
    template class ObjectImplBase<o_immutable_object, ObjectImmutableImpl>;    
    
}