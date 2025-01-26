#pragma once

#include "Field.hpp"
#include "FieldID.hpp"

#include <limits>
#include <array>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/vspace/db0_ptr.hpp>
#include <dbzero/core/collections/pools/StringPools.hpp>
#include <dbzero/core/collections/vector/v_bvector.hpp>
#include <dbzero/core/utils/FlagSet.hpp>
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/object_model/ObjectBase.hpp>
#include <dbzero/workspace/GC0.hpp>

namespace db0

{

    class Fixture;

    enum ClassOptions: std::uint32_t
    {
        SINGLETON = 0x0001
    };

    using ClassFlags = db0::FlagSet<ClassOptions>;

}

DECLARE_ENUM_VALUES(db0::ClassOptions, 1)

namespace db0::object_model

{

    using namespace db0;
    using namespace db0::pools;
    using Fixture = db0::Fixture;
    using ClassFlags = db0::ClassFlags;    
    class Object;
    class Class;
    struct ObjectId;

    struct [[gnu::packed]] o_class: public db0::o_fixed<o_class>
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
        db0_ptr<VFieldVector> m_members_ptr;
        ClassFlags m_flags;
        // language specific class type
        std::uint64_t m_singleton_address = 0;
        const std::uint32_t m_base_class_ref;
        // unused, reserved for future purposes
        std::array<std::uint64_t, 4> m_reserved;
        
        o_class(RC_LimitedStringPool &, const std::string &name, std::optional<std::string> module_name, const VFieldVector &,
            const char *type_id, const char *prefix_name, ClassFlags, const std::uint32_t);
    };
    
    // NOTE: Class type uses SLOT_NUM = TYPE_SLOT_NUM
    // NOTE: class allocations are NOT unique
    class Class: public db0::ObjectBase<Class, db0::v_object<o_class, Fixture::TYPE_SLOT_NUM>, StorageClass::DB0_CLASS, false>,
        public std::enable_shared_from_this<Class>
    {
        GC0_Declare
        using super_t = db0::ObjectBase<Class, db0::v_object<o_class, Fixture::TYPE_SLOT_NUM>, StorageClass::DB0_CLASS, false>;
    public:
        static constexpr std::uint32_t SLOT_NUM = Fixture::TYPE_SLOT_NUM;
        // e.g. PyObject*
        using LangToolkit = db0::python::PyToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
        
        struct Member
        {
            FieldID m_field_id;
            std::string m_name;
            
            Member(FieldID, const char *);
            Member(FieldID, const std::string &);
        };
        
        // Pull existing type
        Class(db0::swine_ptr<Fixture> &, std::uint64_t address);
        ~Class();

        // set the model field names
        void setInitVars(const std::vector<std::string> &init_vars);

        // Get class name in the underlying language object model
        std::string getName() const;

        std::optional<std::string> getTypeId() const;
        
        FieldID addField(const char *name);
        
        // @return field ID / assigned on initialization flag (see Schema Extensions)
        std::pair<FieldID, bool> findField(const char *name) const;
        
        // Get the number of fields declared in this class
        std::size_t size() const {
            return m_members.size();
        }
        
        const Member &get(FieldID field_id) const;
        
        const Member &get(const char *name) const;

        /* FIXME: review & implement
        // Clear the type book (tags) and instance map (registered instances)
        void clear();        
        */

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
        
        /**
         * Check if this class has an associated singleton instance
        */
        bool isExistingSingleton() const;

        // Check if singleton exists on a specific fixture (identified by UUID)
        bool isExistingSingleton(std::uint64_t fixure_uuid) const;
        
        // Construct singleton's ObjectId without unloading it
        ObjectId getSingletonObjectId() const;

        void setSingletonAddress(Object &);

        void commit();

        std::string getTypeName() const;

        std::optional<std::string> tryGetModuleName() const;
        std::string getModuleName() const;
        
        /**
         * Update field layout by changing field name
        */
        void renameField(const char *from_name, const char *to_name);

        /**
         * Class must implement detach since it has v_bvector as a member
        */
        void detach();

        bool operator!=(const Class &rhs) const;

        std::uint32_t getUID() const { return m_uid; }

        // get class id (UUID) as an ObjectId type
        ObjectId getClassId() const;
        
        // @return field name / field index map
        std::unordered_map<std::string, std::uint32_t> getMembers() const;
        
        // Get null class instance (e.g. for testing)
        static std::shared_ptr<Class> getNullClass();

        std::shared_ptr<Class> tryGetBaseClass();
                
    protected:
        friend class ClassFactory;
        friend ClassPtr;
        friend class Object;
        friend super_t;
        
        // DBZero class instances should only be created by the ClassFactory
        // construct a new DBZero class
        // NOTE: module name may not be available in some contexts (e.g. classes defined in notebooks)
        Class(db0::swine_ptr<Fixture> &, const std::string &name, std::optional<std::string> module_name,
            const char *type_id, const char *prefix_name, const std::vector<std::string> &init_vars, ClassFlags, const std::uint32_t);
        
        void unlinkSingleton();
        
        // Get unique class identifier within its fixture
        std::uint32_t fetchUID() const;

    private:
        // member field definitions
        VFieldVector m_members;
        std::shared_ptr<Class> m_base_class_ptr;
        mutable std::vector<Member> m_member_cache;
        // Field by-name index (cache)
        // values: field id / assigned on initialization flag
        mutable std::unordered_map<std::string, std::pair<FieldID, bool> > m_index;
        // fields initialized on class creation (from static code analysis)
        std::unordered_set<std::string> m_init_vars;
        const std::uint32_t m_uid = 0;
        // null-class constructor (for testing only)
        Class() = default;

        /**
         * Load changes to the internal cache
        */
        void refreshMemberCache() const;
    };
    
    // retrieve one of 4 possible type name variants
    std::optional<std::string> getNameVariant(std::optional<std::string> type_id, std::optional<std::string> type_name,
        std::optional<std::string> module_name, std::optional<std::string> type_fields_str, int variant_id);
    
    std::optional<std::string> getNameVariant(const Class &, int variant_id);
    
}
