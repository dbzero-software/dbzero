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
        db0_ptr<VFieldVector> m_members_ptr;
        ClassFlags m_flags;
        // language specific class type
        std::uint64_t m_singleton_address = 0;
        // unused, reserved for future purposes
        std::array<std::uint64_t, 4> m_reserved;
        
        o_class(RC_LimitedStringPool &, const std::string &name, const std::string &module_name, const VFieldVector &,
            const char *type_id, ClassFlags);
    };
    
    // Note that Class type uses SLOT_NUM = TYPE_SLOT_NUM
    class Class: public db0::ObjectBase<Class, db0::v_object<o_class, Fixture::TYPE_SLOT_NUM>, StorageClass::DB0_CLASS>,
        public std::enable_shared_from_this<Class>
    {
        GC0_Declare
        using super_t = db0::ObjectBase<Class, db0::v_object<o_class, Fixture::TYPE_SLOT_NUM>, StorageClass::DB0_CLASS>;
    public:
        // e.g. PyObject*
        using LangToolkit = db0::python::PyToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
        using TypeObjectPtr = typename LangToolkit::TypeObjectPtr;
        using TypeObjectSharedPtr = typename LangToolkit::TypeObjectSharedPtr;
        
        class Member
        {
        public:
            std::string m_name;
            StorageClass m_storage_class;
            
            Member(const char *, StorageClass, std::shared_ptr<Class> type = nullptr);
            Member(const std::string &, StorageClass, std::shared_ptr<Class> type = nullptr);

            std::shared_ptr<Class> getType(ClassFactory &) const;

        private:
            // lazy - initialized member
            mutable std::shared_ptr<Class> m_type;
        };
        
        // Pull existing type (language class unknown)
        Class(db0::swine_ptr<Fixture> &, std::uint64_t address);
        
        /**
         * Retrieves associated language specific class (raw pointer)
        */
        TypeObjectSharedPtr getLangClass() const;

        // Get class name in the underlying language object model
        std::string getName() const;

        std::optional<std::string> getTypeId() const;
        
        std::uint32_t addField(const char *name, StorageClass, std::shared_ptr<Class> type = nullptr);

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

        /**
         * Check if this is a singleton class
        */
        bool isSingleton() const;
        
        /**
         * Check if this class has an associated singleton instance
        */
        bool isExistingSingleton() const;

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
                
    protected:
        friend class ClassFactory;
        friend ClassPtr;
        friend class Object;
        
        // DBZero class instances should only be created by the ClassFactory
        // construct a new DBZero class
        Class(db0::swine_ptr<Fixture> &, const std::string &name, const std::string &module_name, TypeObjectPtr lang_type_ptr, 
            const char *type_id, ClassFlags);
                
        void unlinkSingleton();
        
    public:
        static constexpr std::uint32_t NField = std::numeric_limits<std::uint32_t>::max();
                
    private:
        // member field definitions
        VFieldVector m_members;
        mutable TypeObjectSharedPtr m_lang_type_ptr = 0;
        mutable std::vector<Member> m_member_cache;
        // field by-name index (cache)
        mutable std::unordered_map<std::string, std::uint32_t> m_index;
        
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
