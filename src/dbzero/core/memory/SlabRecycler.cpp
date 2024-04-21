#include "SlabRecycler.hpp"

namespace db0

{

    SlabRecycler::SlabRecycler(unsigned int max_size)
        : m_max_size(max_size)
    {
    }

    void SlabRecycler::append(std::shared_ptr<SlabAllocator> slab) 
    {
        m_slabs.push_back(slab);
        while (m_slabs.size() > m_max_size) {
            m_slabs.pop_front();
        }
    }
    
    std::size_t SlabRecycler::size() const {
        return m_slabs.size();
    }

    std::size_t SlabRecycler::capacity() const {
        return m_max_size;
    }
    
    void SlabRecycler::close(std::function<bool(const SlabAllocator &)> predicate, bool only_first)
    {
        for (auto it = m_slabs.begin(); it != m_slabs.end();) {
            if (predicate(**it)) {
                it = m_slabs.erase(it);
                if (only_first) {
                    break;
                }
            } else {
                ++it;
            }
        }
    }
    
    void SlabRecycler::closeOne(std::function<bool(const SlabAllocator &)> predicate) {
        close(predicate, true);
    }

    void SlabRecycler::clear() {
        m_slabs.clear();
    }
    
}