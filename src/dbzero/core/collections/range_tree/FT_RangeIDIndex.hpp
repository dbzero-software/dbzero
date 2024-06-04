#pragma once

#include <dbzero/core/serialization/Fixed.hpp>
#include <dbzero/core/vspace/db0_ptr.hpp>
#include <dbzero/core/collections/full_text/key_value.hpp>
#include <dbzero/core/collections/b_index/v_bindex.hpp>

namespace db0

{

    using range_id_index_item = db0::key_value<std::uint64_t, std::uint32_t>;
    using RangeIDIndexT = db0::v_bindex<range_id_index_item>;

    struct [[gnu::packed]] o_range_id_index: public o_fixed<o_range_id_index>
    {
        db0::db0_ptr<RangeIDIndexT> m_data_ptr;
        o_range_id_index(Memspace &);
    };

    class FT_RangeIDIndex: public db0::v_object<o_range_id_index>
    {
        using super_t = db0::v_object<o_range_id_index>;
    public: 
        FT_RangeIDIndex(Memspace &);
        FT_RangeIDIndex(mptr);

    private:
        db0::v_bindex<range_id_index_item> m_data;
    };

}