#include "mu_store.hpp"
#include <cassert>

namespace db0

{

    o_mu_store::o_mu_store(std::size_t max_bytes)
        : m_capacity(max_bytes)
    {
        assert(max_bytes <= 0xFFFF);
    }

    std::size_t o_mu_store::measure(std::size_t max_bytes) 
    {
        assert(max_bytes <= 0xFFFF);
        return max_bytes;
    }

    std::size_t o_mu_store::sizeOf() const {
        return m_capacity;
    }

    bool o_mu_store::ConstIterator::operator!=(const ConstIterator &other) const {
        return m_current != other.m_current;
    }
    
    o_mu_store::ConstIterator &o_mu_store::ConstIterator::operator++()
    {
        m_current += 3;
        return *this;
    }

    std::pair<std::uint16_t, std::uint16_t> o_mu_store::ConstIterator::operator*() const 
    {
        std::uint16_t offset = (m_current[0] << 4) | (m_current[1] >> 4);
        std::uint16_t size = ((m_current[1] & 0x0F) << 8) | m_current[2];
        return { offset, size };
    }

    o_mu_store::ConstIterator::ConstIterator(const std::uint8_t *current)
        : m_current(const_cast<std::uint8_t *>(current))
    {
    }
    
    o_mu_store::ConstIterator o_mu_store::begin() const {
        return ConstIterator((std::uint8_t*)this + sizeof(o_mu_store));
    }

    o_mu_store::ConstIterator o_mu_store::end() const {
        return ConstIterator((std::uint8_t*)this + sizeof(o_mu_store) + m_size * 3);
    }
    
    std::size_t o_mu_store::size() const {
        return m_size;
    }    

    bool o_mu_store::tryAppend(std::uint16_t offset, std::uint16_t size)
    {
        std::uint8_t *at = (std::uint8_t*)this + sizeof(o_mu_store) + m_size * 3;
        if (at + 3 > (std::uint8_t*)this + m_capacity) {
            return false;
        }
        
        compact(offset, size, *reinterpret_cast<std::array<std::uint8_t, 3>*>(at));        
        ++m_size;
        return true;
    }

}