#include "Memspace.hpp"

namespace db0

{

    Memspace::Memspace(std::shared_ptr<Prefix> prefix, std::shared_ptr<Allocator> allocator, std::optional<std::uint64_t> uuid)
        : m_prefix(prefix)
        , m_storage_ptr(&prefix->getStorage())
        , m_allocator(allocator)
        , m_allocator_ptr(m_allocator.get())
        , m_derived_UUID(uuid)
        , m_page_size(prefix->getPageSize())
    {
    }

    Memspace::Memspace(tag_from_reference, std::shared_ptr<Prefix> prefix, Allocator &allocator, std::optional<std::uint64_t> uuid)
        : m_prefix(prefix)
        , m_storage_ptr(&prefix->getStorage())
        , m_allocator_ptr(&allocator)
        , m_derived_UUID(uuid)
        , m_page_size(prefix->getPageSize())
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
    
    void Memspace::commit()
    {
        assert(m_prefix);
        // prepare the allocator for the next transaction
        getAllocatorForUpdate().commit();
        m_prefix->commit();
    }
    
    void Memspace::close()
    {
        m_allocator_ptr = nullptr;
        m_allocator = nullptr;
        m_prefix->close();
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
    
    bool Memspace::refresh()
    {
        assert(getAccessType() == AccessType::READ_ONLY);
        return m_prefix->refresh();
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
        getAllocator().detach();
        m_prefix->cancelAtomic();
    }
    
    std::uint64_t Memspace::alloc(std::size_t size, std::uint32_t slot_num) {
        // align if the alloc size > page size
        return getAllocatorForUpdate().alloc(size, slot_num, size > m_page_size);
    }
    
    void Memspace::free(std::uint64_t address) {
        getAllocatorForUpdate().free(address);
    }
        
}