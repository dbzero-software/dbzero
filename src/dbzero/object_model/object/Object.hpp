#pragma once

#include <dbzero/object_model/LangConfig.hpp>
#include <dbzero/object_model/object_header.hpp>
#include <dbzero/object_model/ObjectBase.hpp>
#include <dbzero/object_model/class/MemberID.hpp>
#include <dbzero/workspace/GC0.hpp>
#include <dbzero/core/compiler_attributes.hpp>
#include "ValueTable.hpp"
#include "ObjectInitializer.hpp"
#include <dbzero/object_model/value/StorageClass.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/serialization/packed_int.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include "KV_Index.hpp"

namespace db0

{

    class Fixture;

}

namespace db0::object_model

{

    class Class;
    using Fixture = db0::Fixture;

DB0_PACKED_BEGIN
    class DB0_PACKED_ATTR o_object: public db0::o_base<o_object, 0, false>
    {
    protected:
        using super_t = db0::o_base<o_object, 0, false>;

    public:
        static constexpr unsigned char REALM_ID = 1;
        // common object header
        o_unique_header m_header;        
        // optional address of the key-value store (to store extension fields)
        KV_Address m_kv_address;
        // kv-index type must be stored separately from the address
        bindex::type m_kv_type;
        // number of auto-assigned type tags
        std::uint8_t m_num_type_tags = 0;
        
        PosVT &pos_vt();
        const PosVT &pos_vt() const;

        const packed_int32 &classRef() const;
        std::uint32_t getClassRef() const;
        
        const IndexVT &index_vt() const;

        IndexVT &index_vt();

        // ref_counts - the initial reference counts (tags / objects) inherited from the initializer
        o_object(std::uint32_t class_ref, std::pair<std::uint32_t, std::uint32_t> ref_counts, std::uint8_t num_type_tags, 
            const PosVT::Data &pos_vt_data, unsigned int pos_vt_offset, const XValue *index_vt_begin = nullptr, 
            const XValue *index_vt_end = nullptr);
        
        static std::size_t measure(std::uint32_t, std::pair<std::uint32_t, std::uint32_t>, std::uint8_t num_type_tags,
            const PosVT::Data &pos_vt_data, unsigned int pos_vt_offset, const XValue *index_vt_begin = nullptr, 
            const XValue *index_vt_end = nullptr);
        
        template <typename BufT> static std::size_t safeSizeOf(BufT buf)
        {
            return super_t::sizeOfMembers(buf)
                (PosVT::type())
                (packed_int32::type())            
                (IndexVT::type());
        }     
        
        void incRef(bool is_tag);
        bool hasRefs() const;
        bool hasAnyRefs() const;
    };    
DB0_PACKED_END

    struct FieldLayout
    {
        std::vector<StorageClass> m_pos_vt_fields;
        std::vector<std::pair<unsigned int, StorageClass> > m_index_vt_fields;
        std::vector<std::pair<unsigned int, StorageClass> > m_kv_index_fields;        
    };
    
    enum class ObjectOptions: std::uint8_t
    {
        // the dbzero instance has been deleted
        DROPPED = 0x01,
        // object is defunct - e.g. due to exception on __init__
        DEFUNCT = 0x02
    };

    using ObjectFlags = db0::FlagSet<ObjectOptions>;
    // NOTE: Object instances are created within the realm_id = 1
    using ObjectVType = db0::v_object<o_object, 0, o_object::REALM_ID>;
    
    class Object: public db0::ObjectBase<Object, ObjectVType, StorageClass::OBJECT_REF>
    {
        // GC0 specific declarations
        GC0_Declare
    public:
        static constexpr unsigned char REALM_ID = o_object::REALM_ID;
        using super_t = db0::ObjectBase<Object, ObjectVType, StorageClass::OBJECT_REF>;
        using LangToolkit = LangConfig::LangToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using TypeObjectPtr = typename LangToolkit::TypeObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
        using TypeManager = typename LangToolkit::TypeManager;
        using ObjectStem = ObjectVType;
        using TypeInitializer = ObjectInitializer::TypeInitializer;
        
        // construct as null / dropped object
        Object(UniqueAddress, unsigned int ext_refs);
        Object(const Object &) = delete;
        Object(Object &&) = delete;

        /**
         * Construct new Object (uninitialized, without corresponding dbzero instance yet)          
        */
        Object(std::shared_ptr<Class>);
        Object(TypeInitializer &&);

        // Unload from address with a known type (possibly a base type)
        // NOTE: unload works faster if type_hint is the exact object's type
        struct with_type_hint {};
        Object(db0::swine_ptr<Fixture> &, Address, std::shared_ptr<Class> type_hint, 
            with_type_hint, AccessFlags = {});
        
        // Unload from stem with a known type (possibly a base type)
        // NOTE: unload works faster if type_hint is the exact object's type
        Object(db0::swine_ptr<Fixture> &, ObjectStem &&, std::shared_ptr<Class> type_hint, with_type_hint);
        
        Object(db0::swine_ptr<Fixture> &, Address, AccessFlags = {});
        Object(db0::swine_ptr<Fixture> &, std::shared_ptr<Class>, std::pair<std::uint32_t, std::uint32_t> ref_counts, 
            const PosVT::Data &, unsigned int pos_vt_offset);
        Object(db0::swine_ptr<Fixture> &, ObjectStem &&, std::shared_ptr<Class>);
        
        ~Object();
        
        // post-init invoked by memo type directly after __init__
        void postInit(FixtureLock &);
        
        // Destroys an existing instance and constructs a "null" placeholder 
        void dropInstance(FixtureLock &);
        
        // Unload the object stem, to retrieve its type
        static ObjectStem tryUnloadStem(db0::swine_ptr<Fixture> &, Address, 
            std::uint16_t instance_id = 0, AccessFlags = {});
        static ObjectStem unloadStem(db0::swine_ptr<Fixture> &, Address, 
            std::uint16_t instance_id = 0, AccessFlags = {});
        
        // Called to finalize adding members
        void endInit();
        
        // Assign language specific value as a field (to already initialized or uninitialized instance)
        // NOTE: if lang_value is nullptr then the member is removed
        void set(FixtureLock &, const char *field_name, ObjectPtr lang_value);
        void remove(FixtureLock &, const char *field_name);
        
        // Assign field of an uninitialized instance (assumed as a non-mutating operation)
        // NOTE: if lang_value is nullptr then the member is removed
        void setPreInit(const char *field_name, ObjectPtr lang_value) const;
        void removePreInit(const char *field_name) const;
        
        ObjectSharedPtr tryGet(const char *field_name) const;
        ObjectSharedPtr tryGetAs(const char *field_name, TypeObjectPtr) const;
        ObjectSharedPtr get(const char *field_name) const;
        
        inline std::shared_ptr<Class> getClassPtr() const {
            return m_type ? m_type : m_init_manager.getInitializer(*this).getClassPtr();
        }
        
        inline const Class &getType() const {
            return m_type ? *m_type : m_init_manager.getInitializer(*this).getClass();
        }
        
        Class &getType();
        
        db0::swine_ptr<Fixture> tryGetFixture() const;

        db0::swine_ptr<Fixture> getFixture() const;

        Memspace &getMemspace() const;
                
        /**
         * Get description of the field layout
        */
        FieldLayout getFieldLayout() const;
        
        // Convert singleton into a regular instance
        void unSingleton(FixtureLock &);

        void destroy() const;
                
        bool isSingleton() const;
        
        // execute the function for all members (until false is returned from the input lambda)
        void forAll(std::function<bool(const std::string &, const XValue &, unsigned int offset)>) const;
        void forAll(std::function<bool(const std::string &, ObjectSharedPtr)>) const;
        
        // get dbzero member / member names assigned to this object
        std::unordered_set<std::string> getMembers() const;
        
        /**
         * The overloaded incRef implementation is provided to also handle non-fully initialized objects
        */
        void incRef(bool is_tag);
        bool hasRefs() const;
        // check for any refs (including auto-assigned type tags)
        bool hasAnyRefs() const;
        
        // check if any references from tags exist (i.e. are any tags assigned)
        bool hasTagRefs() const;
        
        // @return true if reference count was decremented to zero
        bool decRef(bool is_tag);
        
        // Binary (shallow) compare 2 objects or 2 versions of the same memo object (e.g. from different snapshots)
        // NOTE: ref-counts are not compared (only user-assigned members)
        // @return true if objects are identical
        bool equalTo(const Object &) const;
        
        /**
         * Move unreferenced object to a different prefix without changing the instance
         * this operations is required for auto-hardening
         */
        void moveTo(db0::swine_ptr<Fixture> &);
        
        /**
         * Change fixture of the uninitialized object
         * Object must not have any members yet either
         */
        void setFixture(db0::swine_ptr<Fixture> &);
        
        void detach() const;

        void commit() const;
        
        Address getAddress() const;
        UniqueAddress getUniqueAddress() const;

        // NOTE: the operation is marked const because the dbzero state is not affected
        void setDefunct() const;

        inline bool isDropped() const {
            return m_flags.test(ObjectOptions::DROPPED);
        }

        inline bool isDefunct() const {
            return m_flags.test(ObjectOptions::DEFUNCT);
        }
        
        // is dropped or defunct
        inline bool isDead() const {
            return m_flags.any(
                static_cast<std::uint8_t>(ObjectOptions::DROPPED) | static_cast<std::uint8_t>(ObjectOptions::DEFUNCT)
            );
        }
        
        // FieldID, is_init_var, fidelity
        std::pair<MemberID, bool> findField(const char *name) const;
        
        // Check if the 2 memo objects are of the same type
        bool sameType(const Object &) const;

        // the member called to indicate the object mutation
        void touch();
        
        void addExtRef() const;
        void removeExtRef() const;
        
        inline std::uint32_t getExtRefs() const {
            return m_ext_refs;
        }

    private:
        // Class will only be assigned after initialization
        std::shared_ptr<Class> m_type;
        // local kv-index instance cache (created at first use)
        mutable std::unique_ptr<KV_Index> m_kv_index;
        static ObjectInitializerManager m_init_manager;
        mutable ObjectFlags m_flags;
        // reference counter for inner references from language objects
        // NOTE: inner references are held by internal dbzero buffers (e.g. TagIndex)
        // see also PyEXT_INCREF / PyEXT_DECREF
        mutable std::uint32_t m_ext_refs = 0;
        // A flag indicating that object's silent mutation has already been reflected
        // with the underlying MemLock / ResourceLock
        // NOTE: by silent mutation we mean a mutation that does not change data (e.g. +refcount(+-1) + (refcount-1))
        mutable bool m_touched = false;
        // NOTE: member assigned only to dropped objects (see replaceWithNull)
        // so that we can retrieve the address of the dropped instance after it has been destroyed
        const UniqueAddress m_unique_address;
        
        void setType(std::shared_ptr<Class>);
        // adjusts to actual type if the type hint is a base class
        void setTypeWithHint(std::shared_ptr<Class> type_hint);
        // @return exists / deleted
        std::pair<bool, bool> hasValueAt(Value, unsigned int fidelity, unsigned int at) const;
        // similar to hasValueAt but assume deleted slot as present
        bool slotExists(Value value, unsigned int fidelity, unsigned int at) const;
        
        // Try retrieving member either from values (initialized) or from the initialization buffer (not initialized yet)
        // @return member exists, member deleted flags
        std::pair<bool, bool> tryGetMemberAt(std::pair<FieldID, unsigned int>, 
            std::pair<StorageClass, Value> &) const;
        FieldID tryGetMember(const char *field_name, std::pair<StorageClass, Value> &, bool &is_init_var) const;
        
        // Try resolving field ID of an existing (or deleted) member and also its storage location
        // @param pos the member's position in the containing collection
        // @return FieldID + containing collection (e.g. pos_vt())
        std::pair<FieldInfo, const void *> tryGetMemberSlot(const MemberID &, unsigned int &pos) const;
        
        // Try locating a field ID associated slot
        std::pair<const void*, unsigned int> tryGetLoc(FieldID) const;
        
        inline ObjectInitializer *tryGetInitializer() const {
            return m_type ? static_cast<ObjectInitializer*>(nullptr) : &m_init_manager.getInitializer(*this);
        }
        
        /**
         * If the KV_Index does not exist yet, create it and add the first value
         * otherwise return instance of an existing KV_Index
        */
        KV_Index *addKV_First(const XValue &);

        KV_Index *tryGetKV_Index() const;
        
        void dropMembers(Class &) const;
        void dropTags(Class &) const;
        
        void unrefMember(db0::swine_ptr<Fixture> &, StorageClass, Value) const;
        void unrefMember(db0::swine_ptr<Fixture> &, XValue) const;
        
        using TypeId = db0::bindings::TypeId;
        std::pair<TypeId, StorageClass> recognizeType(Fixture &, ObjectPtr lang_value) const;
        
        // Unload associated type
        std::shared_ptr<Class> unloadType() const;
        
        // Retrieve a type by class-ref with a possible match (type_hint)
        static std::shared_ptr<Class> getTypeWithHint(const Fixture &, std::uint32_t class_ref, std::shared_ptr<Class> type_hint);
        
        bool hasValidClassRef() const;
        bool hasKV_Index() const;
        
        // try retrieving member as XValue
        std::optional<XValue> tryGetX(const char *field_name) const;
        void _touch();
        
        // Set or update member in a pos_vt
        void setPosVT(FixtureLock &, FieldID, unsigned int pos, unsigned int fidelity, StorageClass, Value);
        void setIndexVT(FixtureLock &, FieldID, unsigned int index_vt_pos, unsigned int fidelity,
            StorageClass, Value);        
        // Set or update member in kv-index
        void setKVIndexValue(FixtureLock &, FieldID, unsigned int fidelity, StorageClass, Value);
        
        // Set with a specific location (pos_vt, index_vt, kv-index)
        void setWithLoc(FixtureLock &, FieldID, const void *, unsigned int pos, unsigned int fidelity, 
            StorageClass, Value);
        
        // Unreference value
        // NOTE: storage_class to be assigned can either be DELETED or UNDEFINED
        void unrefPosVT(FixtureLock &, FieldID, unsigned int pos, StorageClass, unsigned int fidelity);
        void unrefIndexVT(FixtureLock &, FieldID, unsigned int index_vt_pos, StorageClass, unsigned int fidelity);
        void unrefKVIndexValue(FixtureLock &, FieldID, StorageClass, unsigned int fidelity);
        
        void unrefWithLoc(FixtureLock &, FieldID, const void *, unsigned int pos, StorageClass, 
            unsigned int fidelity);
        
        // Add a new value
        void addToPosVT(FixtureLock &, FieldID, unsigned int pos, unsigned int fidelity, StorageClass, Value);
        void addToIndexVT(FixtureLock &, FieldID, unsigned int index_vt_pos, unsigned int fidelity, StorageClass, Value);
        void addToKVIndex(FixtureLock &, FieldID, unsigned int fidelity, StorageClass, Value);
        
        void addWithLoc(FixtureLock &, FieldID, const void *, unsigned int pos, unsigned int fidelity,
            StorageClass, Value);
        
        // lo-fi member specialized implementation
        bool forAll(XValue, std::function<bool(const std::string &, const XValue &, unsigned int offset)>) const;
        
        void getMembersFrom(const Class &this_type, unsigned int index, StorageClass, Value,
            std::unordered_set<std::string> &) const;
    };
    
}

DECLARE_ENUM_VALUES(db0::object_model::ObjectOptions, 2)