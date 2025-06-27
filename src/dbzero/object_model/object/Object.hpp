#pragma once

#include <dbzero/object_model/LangConfig.hpp>
#include <dbzero/object_model/object_header.hpp>
#include <dbzero/object_model/ObjectBase.hpp>
#include <dbzero/object_model/class/FieldID.hpp>
#include <dbzero/workspace/GC0.hpp>
#include "ValueTable.hpp"
#include "ObjectInitializer.hpp"
#include <dbzero/object_model/value/StorageClass.hpp>
#include <dbzero/core/serialization/Types.hpp>
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
    
    class [[gnu::packed]] o_object: public db0::o_base<o_object, 0, false>
    {
    protected:
        using super_t = db0::o_base<o_object, 0, false>;

    public:
        // common object header
        o_unique_header m_header;
        const std::uint32_t m_class_ref;
        // optional address of the key-value store (to store extension fields)
        KV_Address m_kv_address;
        // kv-index type must be stored separately from the address
        bindex::type m_kv_type;
        // reserved for future purposes (e.g. instance flags)
        std::uint8_t m_reserved = 0;

        PosVT &pos_vt();

        const PosVT &pos_vt() const;
        
        const IndexVT &index_vt() const;

        IndexVT &index_vt();

        // ref_counts - the initial reference counts (tags / objects) inherited from the initializer
        o_object(std::uint32_t class_ref, std::pair<std::uint32_t, std::uint32_t> ref_counts, const PosVT::Data &pos_vt_data, 
            const XValue *index_vt_begin = nullptr, const XValue *index_vt_end = nullptr);
        
        static std::size_t measure(std::uint32_t, std::pair<std::uint32_t, std::uint32_t>, const PosVT::Data &pos_vt_data,
            const XValue *index_vt_begin = nullptr, const XValue *index_vt_end = nullptr);
        
        template <typename BufT> static std::size_t safeSizeOf(BufT buf)
        {
            return super_t::sizeOfMembers(buf)
                (PosVT::type())
                (IndexVT::type());
        }     
        
        void incRef(bool is_tag);
    };
    
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

    class Object: public db0::ObjectBase<Object, db0::v_object<o_object>, StorageClass::OBJECT_REF>
    {
        // GC0 specific declarations
        GC0_Declare
    public:
        using super_t = db0::ObjectBase<Object, db0::v_object<o_object>, StorageClass::OBJECT_REF>;
        using LangToolkit = LangConfig::LangToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using TypeObjectPtr = typename LangToolkit::TypeObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
        using TypeManager = typename LangToolkit::TypeManager;
        using ObjectStem = db0::v_object<o_object>;
        using TypeInitializer = ObjectInitializer::TypeInitializer;

        Object(const Object &) = delete;
        Object(Object &&) = delete;

        /**
         * Construct new Object (uninitialized, without corresponding dbzero instance yet)          
        */
        Object(std::shared_ptr<Class>);
        Object(TypeInitializer &&);
        Object(db0::swine_ptr<Fixture> &, Address);
        
        ~Object();
        
        // post-init invoked by memo type directly after __init__
        void postInit(FixtureLock &);
        
        // make null instance (e.g. after destroying the original one)
        static Object *makeNull(void *at_ptr);
        
        // Unload the object stem, to retrieve its type
        static ObjectStem tryUnloadStem(db0::swine_ptr<Fixture> &, Address, std::uint16_t instance_id = 0);
        static ObjectStem unloadStem(db0::swine_ptr<Fixture> &, Address, std::uint16_t instance_id = 0);
        
        // Unload from stem with a known type (possibly a base type)
        // NOTE: unload works faster if type_hint is the exect object's type
        static Object *unload(void *at_ptr, ObjectStem &&, std::shared_ptr<Class> type_hint);
        
        // Unload from address with a known type (possibly a base type)
        // NOTE: unload works faster if type_hint is the exact object's type
        static Object *unload(void *at_ptr, Address, std::shared_ptr<Class> type_hint);

        // Called to finalize adding members
        void endInit();
        
        // Assign language specific value as a field (to already initialized or uninitialized instance)
        void set(FixtureLock &, const char *field_name, ObjectPtr lang_value);
        // Assign field of an uninitialized instance (assumed as a non-mutating operation)
        void setPreInit(const char *field_name, ObjectPtr lang_value) const;
        
        ObjectSharedPtr tryGet(const char *field_name) const;
        ObjectSharedPtr tryGetAs(const char *field_name, TypeObjectPtr) const;
        ObjectSharedPtr get(const char *field_name) const;
        
        // bp::object get(const char *field_name) const;

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
        void unSingleton();

        void destroy() const;
                
        bool isSingleton() const;
        
        // execute the function for all members (until false is returned from the input lambda)
        void forAll(std::function<bool(const std::string &, const XValue &)>) const;
        void forAll(std::function<bool(const std::string &, ObjectSharedPtr)>) const;
        
        // get dbzero member / member names assigned to this object
        std::unordered_set<std::string> getMembers() const;
        
        /**
         * The overloaded incRef implementation is provided to also handle non-fully initialized objects
        */
        void incRef(bool is_tag);
        
        void decRef(bool is_tag);

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

        // NOTE: the operation is marked const because the dbzero state is not affected
        void setDefunct() const;

        inline bool isDropped() const {
            return m_flags.test(ObjectOptions::DROPPED);
        }

        inline bool isDefunct() const {
            return m_flags.test(ObjectOptions::DEFUNCT);
        }
        
        std::pair<FieldID, bool> findField(const char *name) const;
        
        // Check if the 2 memo objects are of the same type
        bool sameType(const Object &) const;

        // the member called to indicate the object mutation
        void touch();

    private:
        // Class will only be assigned after initialization
        std::shared_ptr<Class> m_type;
        // local kv-index instance cache (created at first use)
        mutable std::unique_ptr<KV_Index> m_kv_index;
        static ObjectInitializerManager m_init_manager;
        mutable ObjectFlags m_flags;
        // A flag indicating that object's silent mutation has already been reflected
        // with the underlying MemLock / ResourceLock
        // NOTE: by silent mutation we mean a mutation that does not change data (e.g. +refcount(+-1) + (refcount-1))
        mutable bool m_touched = false;
        
        Object();
        Object(db0::swine_ptr<Fixture> &, std::shared_ptr<Class>, std::pair<std::uint32_t, std::uint32_t> ref_counts, 
            const PosVT::Data &);
        Object(db0::swine_ptr<Fixture> &, ObjectStem &&, std::shared_ptr<Class>);
        
        void setType(std::shared_ptr<Class>);
        // adjusts to actual type if the type hint is a base class
        void setTypeWithHint(std::shared_ptr<Class> type_hint);
                
        // Try retrieving member either from DB0 values (initialized) or from the initialization buffer (not initialized yet)        
        bool tryGetMemberAt(FieldID, bool is_init_var, std::pair<StorageClass, Value> &) const;
        bool tryGetMember(const char *field_name, std::pair<StorageClass, Value> &) const;

        inline ObjectInitializer *tryGetInitializer() const {
            return m_type ? static_cast<ObjectInitializer*>(nullptr) : &m_init_manager.getInitializer(*this);
        }
        
        /**
         * If the KV_Index does not exist yet, create it and add the first value
         * otherwise return instance of an existing KV_Index
        */
        KV_Index *addKV_First(const XValue &);

        const KV_Index *tryGetKV_Index() const;   
        
        void dropMembers(Class &) const;

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
    };
    
}

DECLARE_ENUM_VALUES(db0::object_model::ObjectOptions, 2)