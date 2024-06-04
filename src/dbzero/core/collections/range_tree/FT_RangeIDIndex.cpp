#include "FT_RangeIDIndex.hpp"

namespace db0

{

    o_range_id_index::o_range_id_index(Memspace &memspace)
        : m_data_ptr(memspace)
    {
    }
    
    FT_RangeIDIndex::FT_RangeIDIndex(Memspace &memspace)
        : super_t(memspace, memspace)
        , m_data((*this)->m_data_ptr(memspace))
    {
    }
    
    FT_RangeIDIndex::FT_RangeIDIndex(mptr ptr)
        : super_t(ptr)
        , m_data((*this)->m_data_ptr(getMemspace()))
    {
    }

}