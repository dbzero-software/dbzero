#include "Diff_IO.hpp"

namespace db0

{

    Diff_IO::Diff_IO(std::size_t header_size, CFile &file, std::uint32_t page_size, std::uint32_t block_size, std::uint64_t address,
        std::uint32_t page_count, std::function<std::uint64_t()> tail_function)
        : Page_IO(header_size, file, page_size, block_size, address, page_count, tail_function)
    {    
    }
    
    Diff_IO::Diff_IO(std::size_t header_size, CFile &file, std::uint32_t page_size)
        : Page_IO(header_size, file, page_size)    
    {
    }

    std::uint64_t Diff_IO::tail() const {
        return Page_IO::tail();
    }

    std::uint32_t Diff_IO::getPageSize() const {
        return Page_IO::getPageSize();
    }

}