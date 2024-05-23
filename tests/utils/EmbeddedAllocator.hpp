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
        EmbeddedAllocator() = default;
        
        std::optional<std::uint64_t> tryAlloc(std::size_t size, std::uint32_t) override;
        
        void free(std::uint64_t address) override;

        std::size_t getAllocSize(std::uint64_t address) const override;

        void commit() override;

        void setAllocCallback(std::function<void(std::size_t, std::uint32_t, std::optional<std::uint64_t>)> callback);

    private:
        unsigned int m_count = 0;
        std::unordered_map<std::uint64_t, std::size_t> m_allocations;
        std::function<void(std::size_t, std::uint32_t, std::optional<std::uint64_t>)> m_alloc_callback;
    };
    
}