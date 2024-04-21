#pragma once

#include <dbzero/object_model/config.hpp>
#include <dbzero/object_model/object_header.hpp>
#include <dbzero/object_model/ObjectBase.hpp>
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
    
    std::shared_ptr<Class> unloadClass(std::uint32_t class_ref, const ClassFactory &);
    
    enum class ObjectOptions: std::uint8_t
    {
        // the flag indicating that the instance has been used as a tag
        IS_TAG = 0x01
    };

    using ObjectFlags = db0::FlagSet<ObjectOptions>;

    class [[gnu::packed]] o_object: public db0::o_base<o_object, 0, false>
    {
    protected: 
        using super_t = db0::o_base<o_object, 0, false>;

    public:
        // common object header
        o_object_header m_header;
        const std::uint32_t m_class_ref;
        const std::uint32_t m_instance_id;
        // optional address of the key-value store (to store extension fields)
        KV_Address m_kv_address;
        // kv-index type must be stored separately from the address
        bindex::type m_kv_type;
        ObjectFlags m_flags = {};
        
        PosVT &pos_vt();

        const PosVT &pos_vt() const;
        
        const IndexVT &index_vt() const;

        IndexVT &index_vt();

        // ref_count - the initial reference count inherited from the initializer
        o_object(std::uint32_t class_ref, std::uint32_t instance_id, std::uint32_t ref_count,
            const PosVT::Data &pos_vt_data, const XValue *index_vt_begin = nullptr, const XValue *index_vt_end = nullptr);

        static std::size_t measure(std::uint32_t, std::uint32_t, std::uint32_t, const PosVT::Data &pos_vt_data,
            const XValue *index_vt_begin = nullptr, const XValue *index_vt_end = nullptr);

        template <typename BufT> static std::size_t safeSizeOf(BufT buf)
        {
            auto start = buf;
            buf += super_t::baseSize();
            buf += PosVT::safeSizeOf(buf);
            buf += IndexVT::safeSizeOf(buf);
            return buf - start;
        }
        

        inline void incRef() {
            ++m_header.m_ref_count;
        }
    };
    
    struct FieldLayout
    {
        std::vector<StorageClass> m_pos_vt_fields;
        std::vector<std::pair<unsigned int, StorageClass> > m_index_vt_fields;
    };
    
    class Object: public db0::ObjectBase<Object, db0::v_object<o_object>, StorageClass::OBJECT_REF>
    {
        // GC0 specific declarations
        GC0_Declare
    public:
        using super_t = db0::ObjectBase<Object, db0::v_object<o_object>, StorageClass::OBJECT_REF>;
        using LangToolkit = Config::LangToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
        using TypeManager = typename LangToolkit::TypeManager;
        using ObjectStem = db0::v_object<o_object>;
        
        Object(const Object &) = delete;
        Object(Object &&) = delete;
        Object(db0::swine_ptr<Fixture> &, std::uint64_t address);
        
        /* FIXME
        // Express-new constructor
        DBZObject(db0::Memspace &memspace, std::shared_ptr<DBZClass> type, unsigned int size,
            const std::vector<unsigned int> &field_indices, const std::vector<std::uint64_t> &field_values);

        // Express-new using RowBuilder as the data source
        DBZObject(db0::Memspace &memspace, std::shared_ptr<DBZClass> type, unsigned int size, 
            RowBuilder &row);
        */
        
        ~Object();

        // post-init invoked by memo type directly after __init__
        void postInit(FixtureLock &);
        
        // Express-init an object from the "kwargs" arguments
        // void expressInit(const bp::dict &kwargs);

        /* FIXME
        // Assign arbitrary tags to this object
        void assignTags(const bp::object &tags);
        */
        
        /**
         * Construct new Object (uninitialized, without corresponding DBZero instance yet) 
         * at a specific memory location
        */
        static Object *makeNew(void *at_ptr, std::shared_ptr<Class>);
        // make null instance (e.g. after destroying the original one)
        static Object *makeNull(void *at_ptr);
        
        // Unload the object stem, to retrieve its type and validate UUID / instance ID
        static ObjectStem unloadStem(db0::swine_ptr<Fixture> &, std::uint64_t address, 
            std::optional<std::uint32_t> instance_id = {});
        
        // unload from stem with a known type
        static Object *unload(void *at_ptr, ObjectStem &&, std::shared_ptr<Class>);
        
        // unload from address with a known type
        static Object *unload(void *at_ptr, std::uint64_t address, std::shared_ptr<Class>);

        // Called to finalize adding members
        void endInit();
        
        // Assign language specific value as a field (to already initialized or uninitialized instance)
        void set(FixtureLock &, const char *field_name, ObjectPtr lang_value);
        // Assign field of an uninitialized instance
        void setPreInit(const char *field_name, ObjectPtr lang_value);
        
        ObjectSharedPtr get(const char *field_name) const;
        
        // bp::object get(const char *field_name) const;

        inline std::shared_ptr<Class> getClassPtr() const {
            return m_type ? m_type : m_init_manager.getInitializer(*this).getClassPtr();
        }
        
        inline const Class &getType() const {
            return m_type ? *m_type : m_init_manager.getInitializer(*this).getClass();
        }
        
        Class &getType();

        /* FIXME:
        // Obtain the instance specific tag manager
        std::shared_ptr<DBZTags> tags() const;
        */
        
        /* FIXME:
        // Get reference to the instance-specific cache (e.g. for storing transformed fields)
        std::shared_ptr<DB0Cache> getMemberCache() const;

        DB0Cache &getMemberCacheReference() const;

        static std::shared_ptr<DBZObject> __new(const bp::object &py_class);
        
        // Pull instance from DBZero and assign type
        static std::shared_ptr<DBZObject> __ref(std::uint64_t dbz_addr, std::shared_ptr<DBZClass>);

        static std::shared_ptr<DBZObject> __ref(db0::long_ptr, std::shared_ptr<DBZClass>);

        static std::shared_ptr<DBZObject> __ref(DBZObjectPtr, std::shared_ptr<DBZClass>);

        // Set instance key
        // @return true if instance already exists (in such case pull existing instance)
        bool hasKey(const char *str_key);
        */
        
        db0::swine_ptr<Fixture> tryGetFixture() const;

        db0::swine_ptr<Fixture> getFixture() const;

        Memspace &getMemspace() const;
        
        /**
         * Check if the instance is null (dropped)
        */
        inline bool isNull() const {
            return m_instance_id == 0;
        }
        
        /**
         * Get description of the field layout
        */
        FieldLayout getFieldLayout() const;
        
        // Convert singleton into a regular instance
        void unSingleton();

        void destroy();
                
        bool isSingleton() const;
        
        /**
         * Mark the object as tag if necessary and return the tag value
        */
        std::uint64_t asTag();
        
        bool isTag() const;
        
        // execute the function for all members
        void forAll(std::function<void(const std::string &, const XValue &)>) const;
        void forAll(std::function<void(const std::string &, ObjectSharedPtr)>) const;

        /**
         * The overloaded incRef implementation is provided to also handle non-fully initialized objects
        */
        void incRef();

    private:
        // Class will only be assigned after initialization
        std::shared_ptr<Class> m_type;
        // local kv-index instance cache (created at first use)
        mutable std::unique_ptr<KV_Index> m_kv_index;
        // locally cached instance ID (required for handling deleted objects)
        std::uint32_t m_instance_id = 0;
        static thread_local ObjectInitializerManager m_init_manager;

        Object() = default;
        Object(std::shared_ptr<Class>);
        Object(db0::swine_ptr<Fixture> &, std::shared_ptr<Class>, std::uint32_t ref_count ,const PosVT::Data &);
        Object(db0::swine_ptr<Fixture> &, ObjectStem &&, std::shared_ptr<Class>);
        
        void setType(std::shared_ptr<Class>);
        
        // Pull existing reference from DBZero as a specific type (schema on read)
        // DBZObject(db0::mptr ptr, std::shared_ptr<DBZClass> type);
        
        // Initialize as the same DB0 instance (only allowed for uninitialized instances)
        // void operator=(const DBZObject &);
        
        // Try retrieving member either from DB0 values (initialized) or from the initialization buffer (not initialized yet)        
        bool tryGetMemberAt(unsigned int index, std::pair<StorageClass, Value> &) const;
        
        inline ObjectInitializer *tryGetInitializer() const {
            return m_type ? static_cast<ObjectInitializer*>(nullptr) : &m_init_manager.getInitializer(*this);
        }
        
        /**
         * If the KV_Index does not exist yet, create it and add the first value
         * otherwise return instance of an existing KV_Index
        */
        KV_Index *addKV_First(const XValue &);

        const KV_Index *tryGetKV_Index() const;   

        void dropMembers();

        void dropMember(db0::swine_ptr<Fixture> &, StorageClass, Value);

        using TypeId = db0::bindings::TypeId;
        std::pair<TypeId, StorageClass> recognizeType(Fixture &, ObjectPtr lang_value) const;
    };
    
}

DECLARE_ENUM_VALUES(db0::object_model::ObjectOptions, 1)

