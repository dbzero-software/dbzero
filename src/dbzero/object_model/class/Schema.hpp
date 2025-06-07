#pragma once

#include <dbzero/bindings/TypeId.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/collections/vector/v_sorted_vector.hpp>
#include <dbzero/core/collections/vector/v_bvector.hpp>
#include <dbzero/core/vspace/db0_ptr.hpp>

namespace db0::object_model

{   

    struct [[gnu::packed]] o_type_item: public db0::o_fixed<o_type_item>
    {
        using TypeId = db0::bindings::TypeId;
        TypeId m_type_id;
        // the number of occurences of the specific type ID
        std::uint32_t m_count = 0;
    };
    
    struct [[gnu::packed]] o_schema: public db0::o_fixed<o_schema>
    {
        using TypeId = db0::bindings::TypeId;
        using TypeVector = db0::v_sorted_vector<o_type_item>;
        // the primary type ID (e.g. db0::bindings::TypeId::STRING, NONE inclusive)
        TypeId m_primary_type_id = TypeId::UNKNOWN;
        // the secondary (second most common) type ID
        TypeId m_secondary_type_id = TypeId::UNKNOWN;
        // the number of occurences of the secondary type ID
        std::uint32_t m_secondary_type_id_count = 0;
        // optional type vector for storing additional, less common type IDs
        db0::db0_ptr<TypeVector> m_type_vector_ptr;

        o_schema() = default;        
        // construct populated with values
        o_schema(
            std::vector<std::tuple<unsigned int, TypeId, int> >::const_iterator begin,
            std::vector<std::tuple<unsigned int, TypeId, int> >::const_iterator end
        );

        // get primary & secondary type for a given field ID
        std::pair<TypeId, TypeId> getType() const;
    };

    class Schema: protected db0::v_bvector<o_schema>
    {
    public:
        using TypeId = db0::bindings::TypeId;
        using super_t = db0::v_bvector<o_schema>;

        Schema(Memspace &);
        Schema(mptr);
        ~Schema();
        
        // add occurrence of a specicifc type (as a specific field ID)
        void add(unsigned int field_id, TypeId);
        void remove(unsigned int field_id, TypeId);

        // flush updates from the associated builder
        void flush();

        // get primary & secondary type for a given field ID
        std::pair<TypeId, TypeId> getType(unsigned int field_id) const;
        
    private:
        class Builder;
        friend class Builder;
        std::unique_ptr<Builder> m_builder;

        Builder &getBuilder();

        // batch update a specific field's statistics
        // field ID, type ID, update count
        void update(unsigned int field_id,             
            std::vector<std::tuple<unsigned int, TypeId, int> >::const_iterator begin,
            std::vector<std::tuple<unsigned int, TypeId, int> >::const_iterator end
        );
    };

}