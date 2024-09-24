#pragma once

#include <unordered_map>
#include <functional>
#include <optional>
#include <dbzero/core/memory/Allocator.hpp>

namespace db0

{

    /**
     * EmbeddedAllocator implementation for testing purposes.
    */
    class EmbeddedAllocator: public Allocator
    {
    public:
        using AllocCallbackT = std::function<void(std::size_t, std::uint32_t, bool, bool, std::optional<std::uint64_t>)>;
        EmbeddedAllocator() = default;
        
        std::optional<std::uint64_t> tryAlloc(std::size_t size, std::uint32_t, 
            bool aligned = false, bool unique = false) override;
        
        void free(std::uint64_t address) override;

        std::size_t getAllocSize(std::uint64_t address) const override;

        void commit() const override;

        void detach() const override;

        // size, slot_num, aligned, address (result)
        void setAllocCallback(AllocCallbackT callback);

    private:
        unsigned int m_count = 0;
        std::unordered_map<std::uint64_t, std::size_t> m_allocations;
        AllocCallbackT m_alloc_callback;
    };
    
}