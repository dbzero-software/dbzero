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
        TypeId m_type_id = TypeId::UNKNOWN;
        // the number of occurences of the specific type ID
        std::uint32_t m_count = 0;
        
        o_type_item() = default;
        o_type_item(TypeId, std::uint32_t count = 0);
        o_type_item(TypeId, int count);

        bool operator!() const;

        // match by type ID only
        inline bool operator==(const o_type_item &other) const {
            return (m_type_id == other.m_type_id);
        }

        inline bool operator<(const o_type_item &other) const {
            return (m_type_id < other.m_type_id);
        }
                
        // assign from field ID, type ID and count
        o_type_item &operator=(std::tuple<unsigned int, TypeId, int>);
    };
    
    struct [[gnu::packed]] o_schema: public db0::o_fixed<o_schema>
    {
        using TypeId = db0::bindings::TypeId;
        using TypeVector = db0::v_sorted_vector<o_type_item>;
        using total_func = std::function<std::uint32_t()>;

        // the primary type ID (e.g. db0::bindings::TypeId::STRING, NONE inclusive)
        TypeId m_primary_type_id = TypeId::UNKNOWN;
        // the secondary (second most common)
        o_type_item m_secondary_type;
        // total number of occurrences of all extra type IDs
        std::uint32_t m_total_extra = 0;
        // optional type vector for storing additional, less common type IDs
        // sorted by type ID for fast updates
        db0::db0_ptr<TypeVector> m_type_vector_ptr;

        o_schema() = default;        
        // construct populated with values
        o_schema(Memspace &memspace,
            std::vector<std::tuple<unsigned int, TypeId, int> >::const_iterator begin,
            std::vector<std::tuple<unsigned int, TypeId, int> >::const_iterator end
        );

        // get primary & secondary type for a given field ID
        std::pair<TypeId, TypeId> getType() const;

        // get all types from the most to least common
        std::vector<TypeId> getAllTypes(Memspace &) const;

        void update(Memspace &memspace,
            std::vector<std::tuple<unsigned int, TypeId, int> >::const_iterator begin,
            std::vector<std::tuple<unsigned int, TypeId, int> >::const_iterator end,
            const total_func &get_total
        );
    };
    
    class Schema: protected db0::v_bvector<o_schema>
    {
    public:
        using TypeId = db0::bindings::TypeId;
        using super_t = db0::v_bvector<o_schema>;
        using total_func = std::function<std::uint32_t()>;

        // @param total_func - a function that returns the total number of instance occurrences in the schema
        // it is required for the primary type ID occurrence calculation / only invoked on need-to-know basis
        Schema(Memspace &, total_func);
        Schema(mptr, total_func);
        ~Schema();
        
        // add occurrence of a specicifc type (as a specific field ID)
        void add(unsigned int field_id, TypeId);
        void remove(unsigned int field_id, TypeId);

        // flush updates from the associated builder
        void flush();

        // get primary & secondary type for a given field ID
        std::pair<TypeId, TypeId> getType(unsigned int field_id) const;
        // get all types from the most to least common for a given field ID
        std::vector<TypeId> getAllTypes(unsigned int field_id) const;
        
    private:
        class Builder;
        friend class Builder;
        std::unique_ptr<Builder> m_builder;
        total_func m_get_total;

        Builder &getBuilder();

        // batch update a specific field's statistics
        // field ID, type ID, update count
        void update(unsigned int field_id,             
            std::vector<std::tuple<unsigned int, TypeId, int> >::const_iterator begin,
            std::vector<std::tuple<unsigned int, TypeId, int> >::const_iterator end
        );
    };

}