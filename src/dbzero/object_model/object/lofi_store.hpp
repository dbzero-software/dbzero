// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstddef>
#include <cstdint>
#include <cassert>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0::object_model

{

    /**
     * The lofi_store is a simple fixed-size array for storing very small objects 
     * such as bool values (2-bit). It also allows allocating  and releasing slots for values
     * @tparam SizeOf bit size of a single element (e.g. 2 max is 16)
     */
    template <unsigned int SizeOf>
DB0_PACKED_BEGIN
    class DB0_PACKED_ATTR lofi_store
    {
    public:
        // @return the capacity of the store (e.g. 21 for 2-bit elements)
        static constexpr std::size_t size() {
            return 64u / (SizeOf + 1u);
        }
        
        static inline std::uint64_t mask(unsigned int index)
        {
            assert(index < size());
            constexpr unsigned int stride = SizeOf + 1u; // value bits + occupancy bit
            constexpr std::uint64_t base_mask = ((std::uint64_t)0x01 << (SizeOf + 1)) - 1;
            return base_mask << (index * stride);
        }
        
        // check if the element at the specific index is set
        bool isSet(unsigned int index) const;
        // check if all elements are set
        bool isFull() const;
        
        void set(unsigned int at, std::uint64_t value);
        // remove element at a specific position
        void reset(unsigned int at);

        std::uint64_t get(unsigned int at) const;

        // add a new element and return its index (must not be full)
        unsigned int add(std::uint64_t value);
        
        template <typename ValueT> static inline lofi_store<SizeOf> &fromValue(ValueT &value) {
            static_assert(sizeof(ValueT) == sizeof(lofi_store<SizeOf> ), "ValueT must be 64-bit");
            return reinterpret_cast<lofi_store<SizeOf>&>(value);
        }

        template <typename ValueT> static inline const lofi_store<SizeOf> &fromValue(const ValueT &value) {
            static_assert(sizeof(ValueT) == sizeof(lofi_store<SizeOf> ), "ValueT must be 64-bit");
            return reinterpret_cast<const lofi_store<SizeOf>&>(value);
        }

        // Create value with a single element set
        static std::uint64_t create(unsigned int at, std::uint64_t value);

        class const_iterator
        {
        public:
            const_iterator(std::uint64_t data, unsigned int start, unsigned int end);
            
            bool isEnd() const;
            bool operator!=(const const_iterator &other) const;
            const_iterator &operator++();
            // decoded value
            std::uint64_t operator*() const;

            // current position (set)
            unsigned int getOffset() const;

        private:
            std::uint64_t m_data;
            unsigned int m_current;
            unsigned int m_end;

            void fix();
        };

        const_iterator begin() const;
        const_iterator end() const;

    private:
        std::uint64_t m_data = 0;

        unsigned int findFreeIndex() const;
    };

    template <unsigned int SizeOf>
    bool lofi_store<SizeOf>::isSet(unsigned int index) const {
        assert(index < size());
        constexpr unsigned int stride = SizeOf + 1u; // value bits + occupancy bit
        const unsigned int bit_pos = index * stride + SizeOf; // occupancy bit placed after value bits
        return (m_data >> bit_pos) & 0x1u;
    }

    namespace detail {
        // Precomputed occupancy masks: occupancy bits are the (SizeOf)-th bit in each stride (SizeOf value bits + 1 occupancy bit)
        template <unsigned int SizeOf> struct lofi_full_mask; // primary template left undefined to trigger static_assert below

        template <> struct lofi_full_mask<1>  { static constexpr std::uint64_t value = 0xAAAAAAAAAAAAAAAAull; }; // pattern 10 repeated
        template <> struct lofi_full_mask<2>  { static constexpr std::uint64_t value = 0x4924924924924924ull; };
        template <> struct lofi_full_mask<4>  { static constexpr std::uint64_t value = 0x0842108421084210ull; };
        template <> struct lofi_full_mask<8>  { static constexpr std::uint64_t value = 0x4020100804020100ull; };
        template <> struct lofi_full_mask<16> { static constexpr std::uint64_t value = 0x0004000200010000ull; };

        // Value bit-width masks (for a single element, unshifted)
        template <unsigned int SizeOf> struct lofi_value_mask; // undefined primary
        template <> struct lofi_value_mask<1>  { static constexpr std::uint64_t value = 0x1ull; };
        template <> struct lofi_value_mask<2>  { static constexpr std::uint64_t value = 0x3ull; };
        template <> struct lofi_value_mask<4>  { static constexpr std::uint64_t value = 0xFull; };
        template <> struct lofi_value_mask<8>  { static constexpr std::uint64_t value = 0xFFull; };
        template <> struct lofi_value_mask<16> { static constexpr std::uint64_t value = 0xFFFFull; };
    }

    template <unsigned int SizeOf>
    bool lofi_store<SizeOf>::isFull() const 
    {
        static_assert(SizeOf == 1 || SizeOf == 2 || SizeOf == 4 || SizeOf == 8 || SizeOf == 16, "Unsupported SizeOf for lofi_store::isFull mask");
        constexpr std::uint64_t full_mask = detail::lofi_full_mask<SizeOf>::value;
        return (m_data & full_mask) == full_mask; // all occupancy bits set
    }

    template <unsigned int SizeOf>
    void lofi_store<SizeOf>::set(unsigned int index, std::uint64_t value) 
    {
        static_assert(SizeOf == 1 || SizeOf == 2 || SizeOf == 4 || SizeOf == 8 || SizeOf == 16, "Unsupported SizeOf for lofi_store::set mask");
        assert(index < size());
        constexpr std::uint64_t value_mask = detail::lofi_value_mask<SizeOf>::value;
        assert((value & ~value_mask) == 0 && "value does not fit in SizeOf bits");

        constexpr unsigned int stride = SizeOf + 1u; // value bits + occupancy bit
        const unsigned int bit_pos_value = index * stride; // start of value bits
        const unsigned int bit_pos_occ = bit_pos_value + SizeOf; // occupancy bit

        // Clear previous value bits
        m_data &= ~(value_mask << bit_pos_value);
        // Set new value bits
        m_data |= (static_cast<std::uint64_t>(value) & value_mask) << bit_pos_value;
        // Mark occupancy
        m_data |= (1ull << bit_pos_occ);
    }

    template <unsigned int SizeOf>
    void lofi_store<SizeOf>::reset(unsigned int index) 
    {
        assert(index < size());
        m_data &= ~mask(index);
    }
    
    template <unsigned int SizeOf>
    std::uint64_t lofi_store<SizeOf>::get(unsigned int index) const 
    {
        static_assert(SizeOf == 1 || SizeOf == 2 || SizeOf == 4 || SizeOf == 8 || SizeOf == 16, "Unsupported SizeOf for lofi_store::set mask");
        assert(index < size());
        assert(isSet(index) && "index not set");
        constexpr std::uint64_t value_mask = detail::lofi_value_mask<SizeOf>::value;

        constexpr unsigned int stride = SizeOf + 1u; // value bits + occupancy bit
        const unsigned int bit_pos_value = index * stride; // start of value bits
        return (m_data >> bit_pos_value) & value_mask;
    }

    template <unsigned int SizeOf>
    unsigned int lofi_store<SizeOf>::add(std::uint64_t value) 
    {
        assert(!isFull() && "add() called on full lofi_store");
        const unsigned int idx = findFreeIndex();
        set(idx, value);
        return idx;
    }

    template <unsigned int SizeOf>
    unsigned int lofi_store<SizeOf>::findFreeIndex() const 
    {
        // We assume caller ensured the store is not full.
        // Occupancy bit for index i is located at: i * (SizeOf + 1) + SizeOf
        constexpr unsigned int stride = SizeOf + 1u; // value bits + occupancy bit

        unsigned int bit_pos = SizeOf; // first occupancy bit
        for (unsigned int idx = 0; idx < size(); ++idx, bit_pos += stride) {
            if (((m_data >> bit_pos) & 0x1ull) == 0) {
                return idx; // free slot
            }
        }
        // Should be unreachable if caller checked !isFull().
        assert(false && "findFreeIndex() called on full lofi_store");
        return 0u;
    }
DB0_PACKED_END

    template <unsigned int SizeOf>
    std::uint64_t lofi_store<SizeOf>::create(unsigned int at, std::uint64_t value) 
    {
        lofi_store<SizeOf> store;
        store.set(at, value);
        return store.m_data;
    }

    template <unsigned int SizeOf>
    lofi_store<SizeOf>::const_iterator::const_iterator(std::uint64_t data, unsigned int start, unsigned int end)
        : m_data(data)
        , m_current(start)
        , m_end(end)
    {
        assert(m_current <= m_end);
        fix();
    }

    template <unsigned int SizeOf>
    void lofi_store<SizeOf>::const_iterator::fix()
    {
        while (m_current != m_end) {
            if (lofi_store<SizeOf>::fromValue(m_data).isSet(m_current)) {
                break; // found occupied                
            }
            ++m_current;
        }
        if (m_current == m_end) {
            m_current = m_end = 0;
        }
    } 

    template <unsigned int SizeOf>
    bool lofi_store<SizeOf>::const_iterator::isEnd() const {
        return m_current == m_end;
    }
    
    template <unsigned int SizeOf>
    bool lofi_store<SizeOf>::const_iterator::operator!=(const const_iterator &other) const
    {
        return m_current != other.m_current || m_end != other.m_end;
    }

    template <unsigned int SizeOf>
    typename lofi_store<SizeOf>::const_iterator &lofi_store<SizeOf>::const_iterator::operator++()
    {
        assert(!isEnd());
        ++m_current;
        fix();
        return *this;
    }

    template <unsigned int SizeOf>
    std::uint64_t lofi_store<SizeOf>::const_iterator::operator*() const {
        return lofi_store<SizeOf>::fromValue(m_data).get(m_current);
    }

    template <unsigned int SizeOf>
    unsigned int lofi_store<SizeOf>::const_iterator::getOffset() const {
        return m_current;
    }
    
    template <unsigned int SizeOf>
    typename lofi_store<SizeOf>::const_iterator lofi_store<SizeOf>::begin() const {
        return { m_data, 0, static_cast<unsigned int>(this->size()) };
    }
    
    template <unsigned int SizeOf>
    typename lofi_store<SizeOf>::const_iterator lofi_store<SizeOf>::end() const {
        return { m_data, 0, 0 };
    }

}