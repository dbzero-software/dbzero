#include "Memspace.hpp"
#include <dbzero/core/utils/ProcessTimer.hpp>

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
        
    void Memspace::commit(ProcessTimer *timer)
    {       
        assert(m_prefix);
        // prepare the allocator for the next transaction
        getAllocatorForUpdate().commit();
        m_prefix->commit(timer);
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
    
    std::uint64_t Memspace::alloc(std::size_t size, std::uint32_t slot_num, bool unique) {
        // align if the alloc size > page size
        return getAllocatorForUpdate().alloc(size, slot_num, size > m_page_size, unique);
    }
    
    void Memspace::free(std::uint64_t address) {
        getAllocatorForUpdate().free(address);
    }

    void Memspace::beginLocked(unsigned int locked_section_id)
    {        
        if (locked_section_id >= m_mutation_flags.size()) {
            m_mutation_flags.resize(locked_section_id + 1, -1);
        }
        m_mutation_flags[locked_section_id] = 0;
        m_all_mutation_flags_set = false;
    }
    
    bool Memspace::endLocked(unsigned int locked_section_id)
    {
        assert(locked_section_id < m_mutation_flags.size());
        auto result = m_mutation_flags[locked_section_id];
        m_mutation_flags[locked_section_id] = -1;
        // clean-up released slots
        while (!m_mutation_flags.empty() && m_mutation_flags.back() == -1) {
            m_mutation_flags.pop_back();
        }

        return result;
    }
    
    void Memspace::onDirty()
    {
        if (!m_mutation_flags.empty() && !m_all_mutation_flags_set) {
            // set all flags to "true" where not released
            for (auto &flag: m_mutation_flags) {
                if (flag == 0) {
                    flag = 1;
                }
            }
            m_all_mutation_flags_set = true;
        }
    }

}