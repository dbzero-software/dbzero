#include "SlotAllocator.hpp"
#include <cassert>
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{

    SlotAllocator::SlotAllocator(std::shared_ptr<Allocator> allocator)
        : m_allocator(allocator)
        , m_allocator_ptr(m_allocator.get())
    {
    }

    void SlotAllocator::setSlot(std::uint32_t slot_num, std::shared_ptr<Allocator> slot_allocator) 
    {
        if (slot_num == 0) {
            THROWF(db0::InternalException) << "slot 0 is reserved for the general allocator";
        }
        if (slot_num >= m_slots.size()) {
            m_slots.resize(slot_num + 1);
        }
        m_slots[slot_num] = slot_allocator;
    }

    std::optional<std::uint64_t> SlotAllocator::tryAlloc(std::size_t size, std::uint32_t slot_num, bool aligned) {
        return select(slot_num).tryAlloc(size, 0, aligned);
    }

    void SlotAllocator::free(std::uint64_t address) {
        // can free from the general allocator
        m_allocator_ptr->free(address);
    }

    std::size_t SlotAllocator::getAllocSize(std::uint64_t address) const {
        return m_allocator_ptr->getAllocSize(address);
    }
    
    void SlotAllocator::commit()
    {
        m_allocator_ptr->commit();
        for (auto &slot: m_slots) {
            if (slot) {
                slot->commit();
            }
        }
    }
    
    void SlotAllocator::detach()
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
    
    const Allocator &SlotAllocator::getSlot(std::uint32_t slot_num) const 
    {
        if (slot_num >= m_slots.size() || !m_slots[slot_num]) {
            THROWF(db0::InternalException) << "slot " << slot_num << " not found";
        }
        return *m_slots[slot_num];
    }

}
