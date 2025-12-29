// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "ObjectAnyBase.hpp"
#include <dbzero/object_model/LangConfig.hpp>
#include <dbzero/object_model/class/MemberID.hpp>
#include <dbzero/workspace/GC0.hpp>
#include <dbzero/core/compiler_attributes.hpp>
#include "o_object.hpp"
#include "o_immutable_object.hpp"

namespace db0

{

    class Fixture;

}

namespace db0::object_model

{

    class Class;
    class Object;
    class ObjectImmutableImpl;
    using Fixture = db0::Fixture;
    
    struct FieldLayout
    {
        std::vector<StorageClass> m_pos_vt_fields;
        std::vector<std::pair<unsigned int, StorageClass> > m_index_vt_fields;
        std::vector<std::pair<unsigned int, StorageClass> > m_kv_index_fields;        
    };
    
    template <typename T, typename ImplT>
    class ObjectImplBase: public ObjectAnyBase<T, ImplT>
    {
    public:
        using super_t = ObjectAnyBase<T, ImplT>;
        using LangToolkit = LangConfig::LangToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using TypeObjectPtr = typename LangToolkit::TypeObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
        using TypeManager = typename LangToolkit::TypeManager;
        using ObjectStem = ObjectVType<T>;
        using TypeInitializer = ObjectInitializer::TypeInitializer;
        using tag_as_dropped = typename super_t::tag_as_dropped;
        
        // Construct as null / dropped object
        ObjectImplBase(tag_as_dropped, UniqueAddress, unsigned int ext_refs);
        ObjectImplBase(const ObjectImplBase<T, ImplT> &) = delete;
        ObjectImplBase(ObjectImplBase<T, ImplT> &&) = delete;

        /**
         * Construct new Object (uninitialized, without corresponding dbzero instance yet)          
        */
        ObjectImplBase(std::shared_ptr<Class>);
        ObjectImplBase(TypeInitializer &&);
        
        // Unload from address with a known type (possibly a base type)
        // NOTE: unload works faster if type_hint is the exact object's type
        struct with_type_hint {};
        ObjectImplBase(db0::swine_ptr<Fixture> &, Address, std::shared_ptr<Class> type_hint, 
            with_type_hint, AccessFlags = {});
        
        // Unload from stem with a known type (possibly a base type)
        // NOTE: unload works faster if type_hint is the exact object's type
        ObjectImplBase(db0::swine_ptr<Fixture> &, ObjectStem &&, std::shared_ptr<Class> type_hint, 
            with_type_hint);
        
        ObjectImplBase(db0::swine_ptr<Fixture> &, Address, AccessFlags = {});
        ObjectImplBase(db0::swine_ptr<Fixture> &, std::shared_ptr<Class>, std::pair<std::uint32_t, 
            std::uint32_t> ref_counts, const PosVT::Data &, unsigned int pos_vt_offset);
        ObjectImplBase(db0::swine_ptr<Fixture> &, ObjectStem &&, std::shared_ptr<Class>);
        
        ~ObjectImplBase();
        
        // post-init invoked by memo type directly after __init__
        void postInit(FixtureLock &);
                
        // Unload the object stem, to retrieve its type
        static ObjectStem tryUnloadStem(db0::swine_ptr<Fixture> &, Address, 
            std::uint16_t instance_id = 0, AccessFlags = {});
        static ObjectStem unloadStem(db0::swine_ptr<Fixture> &, Address, 
            std::uint16_t instance_id = 0, AccessFlags = {});
        
        // Called to finalize adding members
        void endInit();
        
        // Assign field of an uninitialized instance (assumed as a non-mutating operation)
        // NOTE: if lang_value is nullptr then the member is removed
        void setPreInit(const char *field_name, ObjectPtr lang_value) const;
        void removePreInit(const char *field_name) const;
        
        ObjectSharedPtr tryGet(const char *field_name) const;
        ObjectSharedPtr tryGetAs(const char *field_name, TypeObjectPtr) const;
        ObjectSharedPtr get(const char *field_name) const;
                
        // Get description of the field layout
        FieldLayout getFieldLayout() const;
        
        void destroy();
        
        // execute the function for all members (until false is returned from the input lambda)
        void forAll(std::function<bool(const std::string &, const XValue &, unsigned int offset)>) const;
        void forAll(std::function<bool(const std::string &, ObjectSharedPtr)>) const;
        
        // get dbzero member / member names assigned to this object
        std::unordered_set<std::string> getMembers() const;
                
        // Binary (shallow) compare 2 objects or 2 versions of the same memo object (e.g. from different snapshots)
        // NOTE: ref-counts are not compared (only user-assigned members)
        // @return true if objects are identical
        bool equalTo(const ObjectImplBase<T, ImplT> &) const;
        
        /**
         * Move unreferenced object to a different prefix without changing the instance
         * this operations is required for auto-hardening
         */
        void moveTo(db0::swine_ptr<Fixture> &);
        
        void detach() const;
        void commit() const;
        
        // FieldID, is_init_var, fidelity
        std::pair<MemberID, bool> findField(const char *name) const;
        
        // NOTE: hasRefs is NOT available in ObjectAnyBase bacause
        // of the use of num_type_tags property
        bool hasRefs() const;
        
    protected:        
        // local kv-index instance cache (created at first use)
        mutable std::unique_ptr<KV_Index> m_kv_index;
        
        void setType(std::shared_ptr<Class>);
        // adjusts to actual type if the type hint is a base class
        void setTypeWithHint(std::shared_ptr<Class> type_hint);
        // @return exists / deleted
        std::pair<bool, bool> hasValueAt(Value, unsigned int fidelity, unsigned int at) const;
        // similar to hasValueAt but assume deleted slot as present
        bool slotExists(Value value, unsigned int fidelity, unsigned int at) const;
        
        void getFieldLayoutImpl(FieldLayout &) const;
        void getMembersImpl(std::unordered_set<std::string> &) const;
        bool tryEqualToImpl(const ObjectImplBase<T, ImplT> &, bool &result) const;
        
        // Try retrieving member either from values (initialized) or from the initialization buffer (not initialized yet)
        // @return member exists, member deleted flags
        bool tryFindMemberAt(std::pair<FieldID, unsigned int>, std::pair<StorageClass, Value> &,
            std::pair<bool, bool> &find_result) const;
        std::pair<bool, bool> tryGetMemberAt(std::pair<FieldID, unsigned int>, 
            std::pair<StorageClass, Value> &) const;
        FieldID tryGetMember(const char *field_name, std::pair<StorageClass, Value> &, bool &is_init_var) const;

        // Try resolving field ID of an existing (or deleted) member and also its storage location
        // @param pos the member's position in the containing collection
        // @return FieldID + containing collection (e.g. pos_vt())
        bool tryFindMemberSlot(const std::pair<FieldID, unsigned int> &field_info, unsigned int &pos,
            std::pair<FieldInfo, const void *> &result) const;
        
        std::pair<FieldInfo, const void *> tryGetMemberSlot(const MemberID &, unsigned int &pos) const;
        
        // Try locating a field ID associated slot
        std::pair<const void*, unsigned int> tryGetLoc(FieldID) const;
        
        inline ObjectInitializer *tryGetInitializer() const {
            return this->m_type ? static_cast<ObjectInitializer*>(nullptr) : &InitManager::instance.getInitializer(*this);
        }
        
        void dropMembers(db0::swine_ptr<Fixture> &, Class &) const;
        void dropMembers(Class &) const;
        void dropTags(Class &) const;
        
        void unrefMember(db0::swine_ptr<Fixture> &, StorageClass, Value) const;
        void unrefMember(db0::swine_ptr<Fixture> &, XValue) const;
        
        using TypeId = db0::bindings::TypeId;
        std::pair<TypeId, StorageClass> recognizeType(Fixture &, ObjectPtr lang_value) const;
        
        // Unload associated type
        std::shared_ptr<Class> unloadType() const;
        
        // Retrieve a type by class-ref with a possible match (type_hint)
        static std::shared_ptr<Class> getTypeWithHint(const Fixture &, std::uint32_t class_ref, 
            std::shared_ptr<Class> type_hint);
        
        bool hasValidClassRef() const;
        
        // try retrieving member as XValue
        std::optional<XValue> tryGetX(const char *field_name) const;        
        
        // Unreference value
        // NOTE: storage_class to be assigned can either be DELETED or UNDEFINED
        void unrefPosVT(FixtureLock &, FieldID, unsigned int pos, StorageClass, unsigned int fidelity);
        void unrefIndexVT(FixtureLock &, FieldID, unsigned int index_vt_pos, StorageClass, unsigned int fidelity);
        
        void unrefWithLoc(FixtureLock &, FieldID, const void *, unsigned int pos, StorageClass,
            unsigned int fidelity);
        bool tryUnrefWithLoc(FixtureLock &, FieldID, const void *, unsigned int pos, StorageClass,
            unsigned int fidelity);
        
        bool forAllImpl(std::function<bool(const std::string &, const XValue &, unsigned int offset)>) const;
        // lo-fi member specialized implementation
        bool forAll(XValue, std::function<bool(const std::string &, const XValue &, unsigned int offset)>) const;
        
        void getMembersFrom(const Class &this_type, unsigned int index, StorageClass, Value,
            std::unordered_set<std::string> &) const;
    };
    
    extern template class ObjectImplBase<o_object, Object>;
    extern template class ObjectImplBase<o_immutable_object, ObjectImmutableImpl>;
    
}
