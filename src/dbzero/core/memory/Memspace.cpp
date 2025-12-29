// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "Memspace.hpp"
#include <dbzero/core/utils/ProcessTimer.hpp>
#include <dbzero/core/memory/utils.hpp>

namespace db0

{
    
    Memspace::Memspace(std::shared_ptr<Prefix> prefix, std::shared_ptr<Allocator> allocator, std::optional<std::uint64_t> uuid)
        : m_prefix(prefix)
        , m_storage_ptr(&prefix->getStorage())
        , m_allocator(allocator)
        , m_allocator_ptr(m_allocator.get())
        , m_derived_UUID(uuid)
        , m_page_size(prefix->getPageSize())
        // NOTE: some memspaces may have a non-standard page size in which case m_page_shift will be 0
        , m_page_shift(getPageShift(m_page_size, false))
    {
    }

    Memspace::Memspace(tag_from_reference, std::shared_ptr<Prefix> prefix, Allocator &allocator, std::optional<std::uint64_t> uuid)
        : m_prefix(prefix)
        , m_storage_ptr(&prefix->getStorage())
        , m_allocator_ptr(&allocator)
        , m_derived_UUID(uuid)
        , m_page_size(prefix->getPageSize())
        // NOTE: some memspaces may have a non-standard page size in which case m_page_shift will be 0
        , m_page_shift(getPageShift(m_page_size, false))
    {
    }
    
    bool Memspace::operator==(const Memspace &other) const {
        return m_prefix == other.m_prefix;
    }
    
    bool Memspace::operator!=(const Memspace &other) const {
        return m_prefix != other.m_prefix;
    }
    
    void Memspace::init(std::shared_ptr<Prefix> prefix, std::shared_ptr<Allocator> allocator)
    {
        m_prefix = prefix;
        m_allocator = allocator;
        m_allocator_ptr = m_allocator.get();
        m_page_size = prefix->getPageSize();
    }
    
    std::size_t Memspace::getPageSize() const {
        return m_page_size;
    }
    
    bool Memspace::commit(ProcessTimer *timer)
    {       
        assert(m_prefix);
        m_maybe_need_flush.clear();
        m_maybe_modified.clear();

        // prepare the allocator for the next transaction
        getAllocatorForUpdate().commit();
        auto state_num = m_prefix->getStateNum(false);
        auto new_state_num = m_prefix->commit(timer);
        return new_state_num != state_num;
    }
    
    void Memspace::detach() const {
        getAllocator().detach();
    }
    
    void Memspace::close(ProcessTimer *timer_ptr)
    {
        std::unique_ptr<ProcessTimer> timer;
        if (timer_ptr) {
            timer = std::make_unique<ProcessTimer>("Memspace::close", timer_ptr);
        }
        
        m_maybe_need_flush.clear();
        m_maybe_modified.clear();
        getAllocatorForUpdate().close();        
        m_allocator_ptr = nullptr;
        m_allocator = nullptr;
        m_prefix->close(timer.get());
        m_prefix = nullptr;
    }
    
    bool Memspace::isClosed() const {
        return !m_allocator_ptr || !m_prefix;
    }

    AccessType Memspace::getAccessType() const
    {
        assert(m_prefix);
        return m_prefix->getAccessType();
    }
    
    bool Memspace::beginRefresh()
    {
        assert(getAccessType() == AccessType::READ_ONLY);
        return m_prefix->beginRefresh();
    }
    
    void Memspace::completeRefresh() {
        m_prefix->completeRefresh();
    }
    
    std::uint64_t Memspace::getStateNum() const
    {
        assert(m_prefix);
        return m_prefix->getStateNum();
    }

    std::uint64_t Memspace::getUUID() const
    {
        if (!m_derived_UUID) {
            THROWF(db0::InternalException) << "Memspace.UUID is not set";
        }
        return *m_derived_UUID;        
    }
    
    void Memspace::beginAtomic()
    {
        assert(!m_atomic);
        m_atomic = true;
        getAllocatorForUpdate().commit();
        // note that we don't flush from prefix on begin atomic
        m_prefix->beginAtomic();
    }
    
    void Memspace::endAtomic()
    {
        assert(m_atomic);
        m_atomic = false;
        getAllocator().detach();
        m_prefix->endAtomic();
    }
    
    void Memspace::cancelAtomic()
    {
        assert(m_atomic);
        m_atomic = false;
        // NOTE: the deferred operations on the allocator get cancelled
        getAllocator().detach();
        m_prefix->cancelAtomic();
    }
    
    Address Memspace::alloc(std::size_t size, std::uint32_t slot_num, unsigned char realm_id, unsigned char locality) {
        // align if the alloc size > page size
        return getAllocatorForUpdate().alloc(size, slot_num, size > m_page_size, realm_id, locality);
    }
    
    UniqueAddress Memspace::allocUnique(std::size_t size, std::uint32_t slot_num, unsigned char realm_id, unsigned char locality) {
        return getAllocatorForUpdate().allocUnique(size, slot_num, size > m_page_size, realm_id, locality);
    }
    
    void Memspace::free(Address address) {
        getAllocatorForUpdate().free(address);
    }
    
    bool Memspace::isAddressValid(Address address, unsigned char realm_id, std::size_t *size_of_result) const
    {
        assert(m_allocator_ptr);
        return m_allocator_ptr->isAllocated(address, realm_id, size_of_result);
    }
    
}