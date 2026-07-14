// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstring>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{
    
DB0_PACKED_BEGIN
    template <unsigned int BitN> struct DB0_PACKED_ATTR o_fixed_bitset:
        public db0::o_fixed<o_fixed_bitset<BitN> >
    {
    public:
        static constexpr unsigned int npos = BitN;

        o_fixed_bitset() {
            std::memset(&m_data, 0, sizeof(m_data));
        }

        void set(unsigned int at, bool value);

        bool get(unsigned int at) const;

        unsigned int firstIndexOf(bool value) const;

        /// returns -1 if the value is not found
        int lastIndexOf(bool value) const;

        unsigned int count(bool value) const;

        static constexpr unsigned int size() {
            return BitN;
        }

        void reset();

    private:
        std::uint32_t m_data[(BitN  - 1) / 32 + 1];
    };
DB0_PACKED_END
    
    template <unsigned int BitN> class VFixedBitset: public db0::v_object<o_fixed_bitset<BitN> >
    {
    public:
        static constexpr unsigned int npos = BitN;

        VFixedBitset(Memspace &memspace)
            : db0::v_object<o_fixed_bitset<BitN> >(memspace)
        {
        }

        VFixedBitset(mptr ptr)
            : db0::v_object<o_fixed_bitset<BitN> >(ptr)
        {
        }

        static constexpr std::size_t sizeOf() {
            return sizeof(o_fixed_bitset<BitN>);
        }

        static void create(Memspace &memspace, Address address)
        {
            // create at a specific v-space address
            auto at = MappedAddress {
                address, memspace.getPrefix().mapRange(address, sizeOf(), { AccessOptions::write })
            };            
            db0::v_object<o_fixed_bitset<BitN> > new_instance(memspace, at);
            new_instance.modify().reset();
        }

        static unsigned int size() {
            return BitN;
        }
    };
    
    template <unsigned int BitN> void o_fixed_bitset<BitN>::set(unsigned int at, bool value)
    {
        assert(at < BitN);
        if (value) {
            m_data[at >> 5] |= (1ULL << (at & 0x1f));
        } else {
            m_data[at >> 5] &= ~(1ULL << (at & 0x1f));
        }
    }
    
    template <unsigned int BitN> bool o_fixed_bitset<BitN>::get(unsigned int at) const
    {
        assert(at < BitN);
        return (m_data[at >> 5] & (1ULL << (at & 0x1f))) != 0;
    }
    
    template <unsigned int BitN> unsigned int o_fixed_bitset<BitN>::firstIndexOf(bool value) const
    {        
        std::uint32_t mask = 0x1;
        std::uint32_t data = m_data[0];
        for (unsigned int i = 0; i < BitN; ++i) {
            if (((data & mask) != 0) == value) {
                return i;
            }
            mask <<= 1;
            if (mask == 0) {
                mask = 1;
                data = m_data[(i + 1) >> 5];
            }
        }
        // not found
        return npos;
    }

    template <unsigned int BitN> unsigned int o_fixed_bitset<BitN>::count(bool value) const
    {
        unsigned int result = 0;
        std::uint32_t mask = 0x1;
        std::uint32_t data = m_data[0];
        for (unsigned int i = 0; i < BitN; ++i) {
            if (((data & mask) != 0) == value) {
                ++result;
            }
            mask <<= 1;
            if (mask == 0) {
                mask = 1;
                data = m_data[(i + 1) >> 5];
            }
        }
        return result;
    }

    template <unsigned int BitN> void o_fixed_bitset<BitN>::reset() {
        memset(&m_data, 0, sizeof(m_data));
    }
    
    template <unsigned int BitN> int o_fixed_bitset<BitN>::lastIndexOf(bool value) const
    {
        std::uint32_t mask = (BitN % 32) == 0 ? 0x80000000 : 1U << ((BitN % 32) - 1);
        std::uint32_t data = m_data[(BitN - 1) >> 5];
        for (int i = BitN - 1; i >= 0; --i) {
            if (((data & mask) != 0) == value) {
                return i;
            }
            mask >>= 1;
            if (mask == 0 && i > 0) {
                mask = 0x80000000;
                data = m_data[(i - 1) >> 5];
            }
        }
        // not found
        return -1;
    }

}