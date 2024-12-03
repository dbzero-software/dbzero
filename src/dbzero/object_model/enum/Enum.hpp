#pragma once

#include "EnumValue.hpp"
#include "EnumDef.hpp"
#include <dbzero/object_model/object_header.hpp>
#include <dbzero/core/collections/b_index/v_bindex.hpp>
#include <dbzero/core/collections/vector/v_bvector.hpp>
#include <dbzero/core/vspace/db0_ptr.hpp>
#include <dbzero/core/collections/pools/StringPools.hpp>
#include <dbzero/object_model/ObjectBase.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/LangConfig.hpp>

namespace db0::object_model

{

    using namespace db0::pools;
    using LP_String = db0::LP_String;

    struct [[gnu::packed]] o_enum: public o_fixed<o_enum>
    {
        db0::o_object_header m_header;
        // enum type name
        LP_String m_name;
        LP_String m_module_name;
        LP_String m_type_id;
        // enum values
        db0::db0_ptr<db0::v_bindex<LP_String> > m_values;
        db0::db0_ptr<db0::v_bvector<LP_String> > m_ordered_values;
        
        o_enum(Memspace &);
    };
    
    /**
     * NOTE: enum types use SLOT_NUM = TYPE_SLOT_NUM
     * NOTE: enum allocations are NOT unique
    */
    class Enum: public db0::ObjectBase<Enum, db0::v_object<o_enum, Fixture::TYPE_SLOT_NUM>, StorageClass::DB0_ENUM_TYPE_REF, false>
    {
        // GC0 specific declarations
        GC0_Declare
    public:
        static constexpr std::uint32_t SLOT_NUM = Fixture::TYPE_SLOT_NUM;
        using super_t = db0::ObjectBase<Enum, db0::v_object<o_enum, SLOT_NUM>, StorageClass::DB0_ENUM_TYPE_REF, false>;
        using LangToolkit = LangConfig::LangToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
        
        Enum(const Enum &) = delete;
        Enum(Enum &&) = delete;
        Enum(db0::swine_ptr<Fixture> &, std::uint64_t address);
        Enum(db0::swine_ptr<Fixture> &, const std::string &name, const std::string &module_name, 
            const std::vector<std::string> &values, const char *type_id = nullptr);
        ~Enum();
                
        std::string getName() const;
        std::string getModuleName() const;
        
        // exception thrown if value not found
        LP_String find(const char *value) const;

        static Enum *makeNew(void *at_ptr, db0::swine_ptr<Fixture> &, const std::string &name, const std::string &module_name,
            const std::vector<std::string> &values, const char *type_id = nullptr);

        // Get unique 32-bit identifier
        // it's implemented as a relative address from the underlying SLOT
        std::uint32_t getUID() const { return m_uid; }
        
        EnumValue get(const char *value) const;
        EnumValue get(EnumValue_UID) const;
        
        // Get enum value as a language-specific type
        ObjectSharedPtr getLangValue(const char *value) const;
        ObjectSharedPtr getLangValue(EnumValue_UID) const;
        ObjectSharedPtr getLangValue(const EnumValue &) const;
        
        // Retrieve all enum defined values ordered by index
        std::vector<EnumValue> getValues() const;

        EnumDef getEnumDef() const;
        std::optional<std::string> getTypeID() const;

        void detach() const;

        void commit() const;
        
    private:
        const std::uint64_t m_fixture_uuid;
        const std::uint32_t m_uid;
        RC_LimitedStringPool &m_string_pool;
        db0::v_bindex<LP_String> m_values;
        // values in order defined by the user
        db0::v_bvector<LP_String> m_ordered_values;
        // enum-values cache (lang objects)
        mutable std::unordered_map<std::string, ObjectSharedPtr> m_cache;

        std::uint32_t fetchUID() const;
    };
    
    std::optional<std::string> getEnumKeyVariant(std::optional<std::string> type_id, std::optional<std::string> enum_name,
        std::optional<std::string> module_name, const std::vector<std::string> &values, int variant_id);

}

namespace std

{

    ostream &operator<<(ostream &os, const db0::object_model::Enum &);

}