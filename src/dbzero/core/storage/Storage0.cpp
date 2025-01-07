#include <dbzero/core/storage/Storage0.hpp>
#include <iostream>
#include <cstring>

namespace db0

{

    std::uint64_t Storage0::STATE_NULL = 0;
    
    Storage0::Storage0(std::size_t page_size)
        : BaseStorage(AccessType::READ_WRITE)
        , m_page_size(page_size)
    {
    }
    
    void Storage0::read(std::uint64_t, std::uint64_t, std::size_t size, void *buffer, FlagSet<AccessOptions>) const {
        std::memset(buffer, 0, size);
    }

    void Storage0::write(std::uint64_t, std::uint64_t, std::size_t, void *) {
    }

    void Storage0::writeDiffs(std::uint64_t, std::uint64_t, std::size_t, void *, const std::vector<std::uint16_t> &, unsigned int) {
    }

    std::uint64_t Storage0::findMutation(std::uint64_t, std::uint64_t state_num) const {
        return state_num;
    }
    
    bool Storage0::tryFindMutation(std::uint64_t address, std::uint64_t state_num, std::uint64_t &mutation_id) const
    {
        mutation_id = state_num;
        return true;
    }
    
    std::size_t Storage0::getPageSize() const {
        return m_page_size;
    }
    
    std::uint64_t Storage0::getLastUpdated() const {
        return 0;
    }
    
}