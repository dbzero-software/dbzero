// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "Field.hpp"
#include "MemberID.hpp"

#include <limits>
#include <array>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/serialization/FixedVersioned.hpp>
#include <dbzero/core/vspace/db0_ptr.hpp>
#include <dbzero/core/collections/pools/StringPools.hpp>
#include <dbzero/core/collections/vector/v_bvector.hpp>
#include <dbzero/core/collections/vector/LimitedMatrixCache.hpp>
#include <dbzero/core/utils/FlagSet.hpp>
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/object_model/ObjectBase.hpp>
#include <dbzero/object_model/value/Value.hpp>
#include <dbzero/object_model/value/XValue.hpp>
#include <dbzero/workspace/GC0.hpp>
#include <dbzero/core/compiler_attributes.hpp>
#include "Schema.hpp"

namespace db0

{

    class Fixture;

    enum ClassOptions: std::uint32_t
    {
        SINGLETON = 0x0001,
        // instances of this type opted out of auto-assigned type tags
        NO_DEFAULT_TAGS = 0x0002,
        IMMUTABLE = 0x0004
    };

    using ClassFlags = db0::FlagSet<ClassOptions>;

}

DECLARE_ENUM_VALUES(db0::ClassOptions, 3)

namespace db0::object_model

{

    using namespace db0;
    using namespace db0::pools;
    using Fixture = db0::Fixture;
    using ClassFlags = db0::ClassFlags;    
    class Object;
    class ObjectImmutableImpl;
    class ObjectAnyImpl;
    class Class;    
    struct ObjectId;

    // fidelity + slot index
    using VFidelityVector = db0::v_bvector<std::pair<std::uint8_t, unsigned int> >;
    
DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_class: public db0::o_fixed_versioned<o_class>
    {        
        // common object header
        db0::o_object_header m_header;
        // auto-generated class UUID
        const std::uint64_t m_uuid;
        LP_String m_name;
        LP_String m_module_name = 0;
        LP_String m_type_id;
        // optional scoped-class prefix
        LP_String m_prefix_name;
        db0_ptr<VFieldMatrix> m_members_ptr;
        // member slot fidelities
        db0_ptr<VFidelityVector> m_fidelity_ptr;
        db0_ptr<Schema> m_schema_ptr;
        ClassFlags m_flags;
        UniqueAddress m_singleton_address = {};
        const std::uint32_t m_base_class_ref;
        const std::uint32_t m_num_bases;
        
        o_class(RC_LimitedStringPool &, const std::string &name, std::optional<std::string> module_name,
            const VFieldMatrix &, const VFidelityVector &, const Schema &, const char *type_id, const char *prefix_name, ClassFlags,
            std::uint32_t base_class_ref, std::uint32_t num_bases
        );
    };    
DB0_PACKED_END
    
    // address <-> class_ref conversion functions
    // @param type_slot_addr_range the address of the types-specific slot
    std::uint32_t classRef(const Class &, std::pair<std::uint64_t, std::uint64_t> type_slot_addr_range);
    std::uint32_t classRef(Address, std::pair<std::uint64_t, std::uint64_t> type_slot_addr_range);
    Address classRefToAddress(std::uint32_t class_ref, std::pair<std::uint64_t, std::uint64_t> type_slot_addr_range);
    std::pair<std::uint64_t, std::uint64_t> getTypeSlotAddrRange(const Fixture &);
    
    using ClassVType = db0::v_object<o_class, Fixture::TYPE_SLOT_NUM>;
    
    // NOTE: Class type uses SLOT_NUM = TYPE_SLOT_NUM
    // NOTE: class allocations are NOT unique
    class Class: public db0::ObjectBase<Class, ClassVType, StorageClass::DB0_CLASS, false>,
        public std::enable_shared_from_this<Class>
    {
        GC0_Declare
        using super_t = db0::ObjectBase<Class, ClassVType, StorageClass::DB0_CLASS, false>;
    public:
        static constexpr std::uint32_t SLOT_NUM = Fixture::TYPE_SLOT_NUM;
        static constexpr unsigned int PRIMARY_FIDELITY = 2;
        
        struct Member
        {
            // primary field ID (primary key)
            FieldID m_field_id;
            unsigned int m_fidelity = 0;
            std::string m_name;
            
            Member(FieldID, unsigned int fidelity, const char *);
            Member(FieldID, unsigned int fidelity, const std::string &);
            
            // @return full index (index + offset) as a single integer
            unsigned int getLongIndex() const;
        };
        
        // Pull existing type
        Class(db0::swine_ptr<Fixture> &, Address address);
        ~Class();

        // set the model field names
        void setInitVars(const std::vector<std::string> &init_vars);

        // Get class name in the underlying language object model
        std::string getName() const;
        
        std::optional<std::string> getTypeId() const;
        
        // Add a new field to this class or a new fidelity
        // @return assigned member ID
        MemberID addField(const char *name, unsigned int fidelity);
        
        // @return member ID / init var flag assigned on initialization flag (see Schema Extensions)
        std::pair<MemberID, bool> findField(const char *name) const;
        
        // Get the total number of unique members declared in this class
        std::size_t size() const {            
            return m_index.size();
        }
        
        Member getMember(FieldID) const;
        Member getMember(std::pair<std::uint32_t, std::uint32_t> loc) const;
        Member getMember(const char *name) const;
        
        /**
         * Try unloading the associated singleton instance, possibly from a specific workspace view
        */
        bool unloadSingleton(void *at) const;
        // Try unloading singleton located on a specific fixture (identified by UUID)
        bool unloadSingleton(void *at, std::uint64_t fixture_uuid) const;
        
        /**
         * Check if this is a singleton class
        */
        bool isSingleton() const;
        bool assignDefaultTags() const;
        
        /**
         * Check if this class has an associated singleton instance
        */
        bool isExistingSingleton() const;

        // Check if singleton exists on a specific fixture (identified by UUID)
        bool isExistingSingleton(std::uint64_t fixure_uuid) const;
        
        // Construct singleton's ObjectId without unloading it
        ObjectId getSingletonObjectId() const;
        
        template <typename T> void setSingletonAddress(T &);

        Address getSingletonAddress() const;
        
        std::string getTypeName() const;

        std::optional<std::string> tryGetModuleName() const;
        std::string getModuleName() const;
        
        /**
         * Update field layout by changing field name
        */
        void renameField(const char *from_name, const char *to_name);

        void flush() const;
        void rollback();
        
        void commit() const;

        /**
         * Class must implement detach since it has v_bvector as a member
        */
        void detach() const;

        bool operator==(const Class &rhs) const;
        bool operator!=(const Class &rhs) const;

        std::uint32_t getUID() const { return m_uid; }

        // get class id (UUID) as an ObjectId type
        ObjectId getClassId() const;
        
        // @return field name / member ID map
        std::unordered_map<std::string, MemberID> getMembers() const;
        
        std::shared_ptr<Class> tryGetBaseClass() const;
        // @return base class pointer or nullptr if no base class is defined
        const Class *getBaseClassPtr() const;
        
        // Get initialization variables identified by static code analysis
        // note that the result includes also all base class init vars
        const std::unordered_set<std::string> &getInitVars() const;

        const Schema &getSchema() const;
        
        // Collect schema with a callback function
        // NOTE: type is the primary / most likely type for the field
        // NOTE: possible types (all_types) are reported from the most likely / common to least likely
        void getSchema(std::function<void(const std::string &field_name, SchemaTypeId primary_type,
            const std::vector<SchemaTypeId> &all_types)>) const;
        
        void updateSchema(unsigned int first_id, const std::vector<StorageClass> &types,
            const std::vector<Value> &values, bool add = true);
        // Add or remove from schema index-encoded field types
        void updateSchema(const XValue *begin, const XValue *end, bool add = true);
        // Update type of a single field occurrence
        void updateSchema(FieldID, unsigned int fidelity, SchemaTypeId old_type, SchemaTypeId new_type);
        // Add a single field occurrence to the schema
        void addToSchema(FieldID, unsigned int fidelity, SchemaTypeId);
        void removeFromSchema(FieldID, unsigned int fidelity, SchemaTypeId);
        void addToSchema(unsigned int index, StorageClass, Value);
        void removeFromSchema(unsigned int index, StorageClass, Value);
        void addToSchema(const XValue &);
        void removeFromSchema(const XValue &);
        
        std::uint32_t getNumBases() const;
        
        // NOTE: this is for type compatibility only, Class objects don't have instance_id
        UniqueAddress getUniqueAddress() const;
        
        std::uint32_t getClassRef() const;
        
        const VFieldMatrix &getMembersMatrix() const;
        
        // Get specific slot's fidelity (or 0 if not assigned)
        unsigned int getFidelity(std::uint32_t index) const;
        
        // Set / update only the runtime flags from the memo type decoration
        void setRuntimeFlags(FlagSet<MemoOptions>);

        inline bool isNoCache() const {
            return m_no_cache;
        }
        
        // Get instance flags to be applied to objects of this class
        inline AccessFlags getInstanceFlags() const {
            return m_no_cache ? AccessFlags { AccessOptions::no_cache } : AccessFlags {};
        }
                
    protected:
        friend class ClassFactory;        
        friend ClassPtr;
        friend class Object;
        friend class ObjectImmutableImpl;
        friend class ObjectAnyImpl;
        friend super_t;
        
        void unlinkSingleton();         

        // dbzero class instances should only be created by the ClassFactory
        // construct a new dbzero class
        // NOTE: module name may not be available in some contexts (e.g. classes defined in notebooks)
        Class(db0::swine_ptr<Fixture> &, const std::string &name, std::optional<std::string> module_name,
            const char *type_id, const char *prefix_name, const std::vector<std::string> &init_vars, ClassFlags, 
            std::shared_ptr<Class> base_class);
            
        // Get unique class identifier within its fixture
        std::uint32_t fetchUID() const;
        
        std::optional<Member> tryGetMember(FieldID) const;
        std::optional<Member> tryGetMember(std::pair<std::uint32_t, std::uint32_t> loc) const;
        std::optional<Member> tryGetMember(const char *name) const;
        
    private:
        const std::pair<std::uint64_t, std::uint64_t> m_type_slot_addr_range;
        using FieldKeyT = std::pair<std::uint32_t, std::uint32_t>;
        
        struct MemberAdapter
        {
            std::reference_wrapper<const Class> m_class;

            MemberAdapter(const Class &);
            Member operator()(std::pair<std::uint32_t, std::uint32_t> loc, const o_field &) const;
        };
        
        using MemberCacheT = LimitedMatrixCache<VFieldMatrix, Member, MemberAdapter>;
        
        // member field definitions
        VFieldMatrix m_members;
        // only holds non-default fidelities (i.e. > 0)
        VFidelityVector m_fidelities;
        Schema m_schema;
        std::shared_ptr<Class> m_base_class_ptr;
        
        // Field by-name index (cache)
        // values: member ID / assigned on initialization flag
        mutable std::unordered_map<std::string, std::pair<MemberID, bool> > m_index;
        // For fidelity = 0 this maps "index" to the unique field ID
        mutable std::vector<FieldID> m_unique_keys;
        // fields initialized on class creation (from static code analysis)
        std::unordered_set<std::string> m_init_vars;
        const std::uint32_t m_uid = 0;
        mutable MemberCacheT m_member_cache;
        // runtime flags
        bool m_no_cache = false;
        
        // A function to retrieve the total number of instances of the schema
        std::function<unsigned int()> getTotalFunc() const;
        std::function<void(const Member &)> getRefreshCallback() const;
        // callback for MemberID updates
        void onMemberIDUpdated(const MemberID &) const;
        // translate member's field ID into a unique key
        FieldID getPrimaryKey(unsigned int index) const;
        
        // Initialization function
        std::unordered_set<std::string> makeInitVars(const std::vector<std::string> &) const;
        
        // Assign a new field slot with a specified fidelity
        std::pair<std::uint32_t, std::uint32_t> assignSlot(unsigned int fidelity);
        // Check if a specific field (by name) exists and is assigned to a given fidelity (0 = default)
        bool hasSlot(const char *name, unsigned int fidelity) const;
    };
    
    // retrieve one of 4 possible type name variants
    std::optional<std::string> getNameVariant(std::optional<std::string> type_id, std::optional<std::string> type_name,
        std::optional<std::string> module_name, std::optional<std::string> type_fields_str, int variant_id);
    
    std::optional<std::string> getNameVariant(const Class &, int variant_id);
    
    template <typename T>
    void Class::setSingletonAddress(T &object)
    {
        assert(!(*this)->m_singleton_address.isValid());
        assert(isSingleton());
        // increment reference count in order to prevent singleton object from being destroyed
        object.incRef(false);
        modify().m_singleton_address = object.getUniqueAddress();
    }

}
