// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "SlabAllocator.hpp"

namespace db0

{
    
DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR CapacityItem
    {
        // primary key (high part)
        std::uint32_t m_remaining_capacity;
        std::uint32_t m_lost_capacity;
        // primary key (low part)
        std::uint32_t m_slab_id;
        
        CapacityItem() = default;

        CapacityItem(std::uint32_t remaining_capacity, std::uint32_t lost_capacity, std::uint32_t slab_id)
            : m_remaining_capacity(remaining_capacity)
            , m_lost_capacity(lost_capacity)
            , m_slab_id(slab_id)
        {
        }

        static std::uint64_t getKey(const CapacityItem &item) {
            return ((std::uint64_t)item.m_remaining_capacity << 32) | item.m_slab_id;
        }
        
        // Construct key from construction args
        static std::uint64_t getKey(std::uint32_t remaining_capacity, std::uint32_t, std::uint32_t slab_id) {
            return ((std::uint64_t)remaining_capacity << 32) | slab_id;
        }

        inline static std::uint32_t first(std::uint64_t key) {
            return static_cast<std::uint32_t>(key >> 32);
        }

        inline static std::uint32_t second(std::uint64_t key) {
            return static_cast<std::uint32_t>(key & 0xFFFFFFFF);
        }

        // note descending order of comparisons
        struct CompT
        {
            inline bool operator()(const CapacityItem &lhs, const CapacityItem &rhs) const {
                if (lhs.m_remaining_capacity == rhs.m_remaining_capacity)
                    return lhs.m_slab_id < rhs.m_slab_id;
                return rhs.m_remaining_capacity < lhs.m_remaining_capacity;
            }
            
            inline bool operator()(const CapacityItem &lhs, std::uint64_t rhs) const {
                if (lhs.m_remaining_capacity == first(rhs))
                    return lhs.m_slab_id < second(rhs);
                return first(rhs) < lhs.m_remaining_capacity;
            }

            inline bool operator()(std::uint64_t lhs, const CapacityItem &rhs) const {
                if (first(lhs) == rhs.m_remaining_capacity)
                    return second(lhs) < rhs.m_slab_id;
                return rhs.m_remaining_capacity < first(lhs);
            }
        };

        struct EqualT
        {
            inline bool operator()(const CapacityItem &lhs, const CapacityItem &rhs) const {
                return lhs.m_remaining_capacity == rhs.m_remaining_capacity && lhs.m_slab_id == rhs.m_slab_id;
            }
            
            inline bool operator()(const CapacityItem &lhs, std::uint64_t rhs) const {
                return lhs.m_remaining_capacity == first(rhs) && lhs.m_slab_id == second(rhs);                    
            }
            
            inline bool operator()(std::uint64_t lhs, const CapacityItem &rhs) const {
                return first(lhs) == rhs.m_remaining_capacity && second(lhs) == rhs.m_slab_id;
            }
        };
    };
DB0_PACKED_END

DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR SlabDef
    {
        // primary key
        std::uint32_t m_slab_id;
        std::uint32_t m_remaining_capacity;
        std::uint32_t m_lost_capacity;
        
        SlabDef(std::uint32_t slab_id, std::uint32_t remaining_capacity, std::uint32_t lost_capacity)
            : m_slab_id(slab_id)
            , m_remaining_capacity(remaining_capacity)
            , m_lost_capacity(lost_capacity)
        {
        }
        
        static inline std::uint32_t getKey(const SlabDef &item) {
            return item.m_slab_id;
        }

        // Extract key from construction args
        static inline std::uint32_t getKey(std::uint32_t slab_id, std::uint32_t, std::uint32_t) {
            return slab_id;
        }
        
        struct CompT
        {
            inline bool operator()(const SlabDef &lhs, const SlabDef &rhs) const {                    
                return lhs.m_slab_id < rhs.m_slab_id;                    
            }
            
            inline bool operator()(const SlabDef &lhs, std::uint32_t rhs) const {
                return lhs.m_slab_id < rhs;
            }

            inline bool operator()(std::uint32_t lhs, const SlabDef &rhs) const {
                return lhs < rhs.m_slab_id;
            }
        };

        struct EqualT
        {
            inline bool operator()(const SlabDef &lhs, const SlabDef &rhs) const {
                return lhs.m_slab_id == rhs.m_slab_id;
            }
            
            inline bool operator()(const SlabDef &lhs, std::uint32_t rhs) const {
                return lhs.m_slab_id == rhs;
            }

            inline bool operator()(std::uint32_t lhs, const SlabDef &rhs) const {
                return lhs == rhs.m_slab_id;
            }
        };
    };
DB0_PACKED_END
    
    struct SlabItem
    {            
        std::shared_ptr<SlabAllocator> m_slab;
        // the capacity item as last retrieved from the backend (may need update)
        CapacityItem m_cap_item;
        bool m_is_dirty = false;
        
        SlabItem(std::shared_ptr<SlabAllocator> slab, CapacityItem cap);
        ~SlabItem();
        
        void commit() const;
        void detach() const;
        
        bool operator==(std::uint32_t slab_id) const {
            assert(m_slab);
            return m_cap_item.m_slab_id == slab_id;
        }

        bool operator==(const SlabItem &rhs) const {            
            return *this == rhs.m_cap_item.m_slab_id;
        }

        SlabAllocator &operator*() {
            assert(m_slab);
            return *m_slab;
        }

        const SlabAllocator &operator*() const {
            assert(m_slab);
            return *m_slab;
        }

        const SlabAllocator *operator->() const {
            assert(m_slab);
            return m_slab.get();
        }

        SlabAllocator *operator->(){
            assert(m_slab);
            return m_slab.get();
        }
    };

}

namespace std

{
    
    ostream &operator<<(ostream &os, const db0::CapacityItem &item);
    ostream &operator<<(ostream &os, const db0::SlabDef &item);

}