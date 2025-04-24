#include "SlotAllocator.hpp"
#include "SlabAllocator.hpp"
#include <cassert>
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{

    SlotAllocator::SlotAllocator(std::shared_ptr<Allocator> allocator)
        : m_allocator(allocator)
        , m_allocator_ptr(m_allocator.get())
    {
    }

    void SlotAllocator::setSlot(std::uint32_t slot_num, std::shared_ptr<SlabAllocator> slot_allocator)
    {
        if (slot_num == 0) {
            THROWF(db0::InternalException) << "slot 0 is reserved for the general allocator";
        }
        if (slot_num >= m_slots.size()) {
            m_slots.resize(slot_num + 1);
        }
        m_slots[slot_num] = slot_allocator;
    }

    struct ScopedAllocBuf
    {
        SlabAllocator &m_allocator;
        std::vector<std::uint64_t> m_pending_free;
        
        ScopedAllocBuf(SlabAllocator &allocator)
            : m_allocator(allocator)
        {
        }

        ~ScopedAllocBuf() 
        {
            for (auto addr: m_pending_free) {
                m_allocator.free(addr);
            }
        }

        void add(std::uint64_t addr) {
            m_pending_free.push_back(addr);
        }
    };
    
    std::optional<Address> SlotAllocator::tryAlloc(std::size_t size, std::uint32_t slot_num,bool aligned) 
    {
        if (!slot_num) {
            return m_allocator_ptr->tryAlloc(size, 0, aligned, unique);
        }
        
        return select(slot_num).tryAlloc(size, 0, aligned, false);
    }
    
    void SlotAllocator::free(Address address) {
        // can free from the general allocator
        m_allocator_ptr->free(address);
    }
    
    std::size_t SlotAllocator::getAllocSize(Address address) const {
        return m_allocator_ptr->getAllocSize(address);
    }
    
    bool SlotAllocator::isAllocated(Address address) const {
        return m_allocator_ptr->isAllocated(address);
    }

    void SlotAllocator::commit() const
    {
        m_allocator_ptr->commit();
        for (auto &slot: m_slots) {
            if (slot) {
                slot->commit();
            }
        }
    }
    
    void SlotAllocator::detach() const
    {
        m_allocator_ptr->detach();
        for (auto &slot: m_slots) {
            if (slot) {
                slot->detach();
            }
        }
    }
    
    Allocator &SlotAllocator::select(std::uint32_t slot_num)
    {
        if (slot_num == 0) {
            return *m_allocator_ptr;
        }
        assert(slot_num < m_slots.size() && m_slots[slot_num]);
        return *m_slots[slot_num];
    }
    
    SlabAllocator &SlotAllocator::getSlot(std::uint32_t slot_num) const
    {
        if (!slot_num || slot_num >= m_slots.size() || !m_slots[slot_num]) {
            THROWF(db0::InternalException) << "slot " << slot_num << " not found";
        }
        return *m_slots[slot_num];
    }
    
    bool SlotAllocator::inRange(Address address) const {
        return m_allocator_ptr->inRange(address);
    }

}
