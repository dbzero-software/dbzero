#pragma once

#include <dbzero/object_model/object_header.hpp>
#include <dbzero/core/collections/b_index/v_bindex.hpp>
#include <dbzero/core/vspace/db0_ptr.hpp>
#include <dbzero/core/collections/pools/StringPools.hpp>
#include <dbzero/object_model/ObjectBase.hpp>
#include <dbzero/workspace/Fixture.hpp>

namespace db0::object_model

{

    using namespace db0::pools;
    using LP_String = db0::LP_String;

    struct [[gnu::packed]] o_enum: public o_fixed<o_enum>
    {
        db0::o_object_header m_header;
        // enum values
        db0::db0_ptr<db0::v_bindex<LP_String> > m_values;

        o_enum(Memspace &);
    };
    
    /**
     * Note that enum types use SLOT_NUM = TYPE_SLOT_NUM
    */
    class Enum: public db0::ObjectBase<Enum, db0::v_object<o_enum, Fixture::TYPE_SLOT_NUM>, StorageClass::DB0_ENUM_TYPE_REF>
    {
        // GC0 specific declarations
        GC0_Declare
    public:
        static constexpr std::uint32_t SLOT_NUM = Fixture::TYPE_SLOT_NUM;
        using super_t = db0::ObjectBase<Enum, db0::v_object<o_enum, SLOT_NUM>, StorageClass::DB0_ENUM_TYPE_REF>;
        
        Enum(const Enum &) = delete;
        Enum(Enum &&) = delete;
        Enum(db0::swine_ptr<Fixture> &, std::uint64_t address);
        Enum(db0::swine_ptr<Fixture> &, const std::vector<std::string> &values);

        // exception thrown if value not found
        LP_String find(const char *value) const;

        static Enum *makeNew(void *at_ptr, db0::swine_ptr<Fixture> &, const std::string &name,
            const std::vector<std::string> &values);

        // Get unique 32-bit identifier
        // it's implemented as a relative address from the underlying SLOT
        std::uint32_t getUID() const;

    private:         
        RC_LimitedStringPool &m_string_pool;
        db0::v_bindex<LP_String> m_values;
    };
    
}