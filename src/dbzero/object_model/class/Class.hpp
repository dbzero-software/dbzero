#pragma once

#include "Field.hpp"

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
    
    struct [[gnu::packed]] o_class: public db0::o_fixed<o_class>
    {
        // common object header
        db0::o_object_header m_header;
        // auto-generated class UUID
        const std::uint64_t m_uuid;
        LP_String m_name;
        LP_String m_module_name;
        LP_String m_type_id;
        // optional scoped-class prefix
        LP_String m_prefix_name;
        db0_ptr<VFieldVector> m_members_ptr;
        ClassFlags m_flags;
        // language specific class type
        std::uint64_t m_singleton_address = 0;
        // unused, reserved for future purposes
        std::array<std::uint64_t, 4> m_reserved;
        
        o_class(RC_LimitedStringPool &, const std::string &name, const std::string &module_name, const VFieldVector &,
            const char *type_id, const char *prefix_name, ClassFlags);
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
        using TypeObjectPtr = typename LangToolkit::TypeObjectPtr;
        using TypeObjectSharedPtr = typename LangToolkit::TypeObjectSharedPtr;
        
        struct Member
        {
            std::uint32_t m_field_id;
            std::string m_name;
            
            Member(std::uint32_t, const char *);
            Member(std::uint32_t, const std::string &);
        };
        
        // Pull existing type (language class unknown)
        Class(db0::swine_ptr<Fixture> &, std::uint64_t address);
        ~Class();
        
        /**
         * Retrieves associated language specific class (raw pointer)
        */
        TypeObjectSharedPtr getLangClass() const;

        // Get class name in the underlying language object model
        std::string getName() const;

        std::optional<std::string> getTypeId() const;
        
        std::uint32_t addField(const char *name);

        std::uint32_t findField(const char *name) const;

        // Get the number of fields declared in this class
        std::size_t size() const {
            return m_members.size();
        }
        
        const Member &get(std::uint32_t index) const;

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

        void setSingletonAddress(Object &);

        void commit();

        std::string getTypeName() const;

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

    protected:
        friend class ClassFactory;
        friend ClassPtr;
        friend class Object;
        
        // DBZero class instances should only be created by the ClassFactory
        // construct a new DBZero class
        Class(db0::swine_ptr<Fixture> &, const std::string &name, const std::string &module_name, TypeObjectPtr lang_type_ptr, 
            const char *type_id, const char *prefix_name, ClassFlags);
        
        void unlinkSingleton();
        
        // Get unique class identifier within its fixture
        std::uint32_t fetchUID() const;

    public:
        static constexpr std::uint32_t NField = std::numeric_limits<std::uint32_t>::max();
                
    private:
        // member field definitions
        VFieldVector m_members;
        mutable TypeObjectSharedPtr m_lang_type_ptr = 0;
        mutable std::vector<Member> m_member_cache;
        // field by-name index (cache)
        mutable std::unordered_map<std::string, std::uint32_t> m_index;
        const std::uint32_t m_uid;

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
