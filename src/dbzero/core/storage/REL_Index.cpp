#include "REL_Index.hpp"
#include <dbzero/core/memory/utils.hpp>

namespace db0

{

    REL_Index::REL_Index(Memspace &memspace, std::uint32_t step_size)
        : super_t(memspace)
        , m_step_size(step_size)
        , m_shift(db0::getPageShift(step_size, true))
    {        
    }
    
    REL_Index::REL_Index(mptr ptr, std::uint32_t step_size)
        : super_t(ptr)
        , m_step_size(step_size)
        , m_shift(db0::getPageShift(step_size, true))
    {        
    }

    void REL_Index::add(std::uint64_t page_num, std::uint64_t page_io_address) {
        super_t::setItem(page_num >> m_shift, page_io_address);
    }

    std::uint64_t REL_Index::get(std::uint64_t page_num) const {
        return super_t::operator[](page_num >> m_shift);
    }
        
    void REL_Index::detach() const {
        super_t::detach();
    }
    
    void REL_Index::commit() const {
        super_t::commit();
    }

}