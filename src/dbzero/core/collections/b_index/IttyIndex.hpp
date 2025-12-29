// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/collections/vector/joinable_const_iterator.hpp>
#include <dbzero/core/metaprog/binary_cast.hpp>
#include <dbzero/core/memory/Memspace.hpp>
#include "empty_index.hpp"

namespace db0 

{
    
	/**
	 * IttyIndex consists of a single element (ItemT)
	 * ItemT value is stored in IttyIndex pointer (i.e. IttyIndex is fully contained in its own pointer)
	 * ItemT must be assignment compatible with AddrT
	 * cast_functor - must be able to cast between ItemT and AddrT in both directions
	 */
	template <typename ItemT = std::uint64_t, typename AddrT = Address, typename item_comp_t = std::less<ItemT> >
    class IttyIndex
	{		
		item_comp_t m_compare;
		ItemT m_value;
	public:
		using addr_t = AddrT;
		using joinable_const_iterator = db0::joinable_const_iterator<ItemT, item_comp_t>;
		using CompT = item_comp_t;
		
		// size of itty_index is always 1
		static constexpr std::size_t size = 1;

		/**
         * Create new index and write value
         */
        IttyIndex(Memspace &, ItemT value)
            : m_value(value)
        {
        }

		/**
         * Create new index and write value
         */
		IttyIndex(Memspace &, const ItemT *value)
			: m_value(*value)
		{
		}

		/**
         * Create from single element range (end - begin == 1)
         */
		IttyIndex(Memspace &, const ItemT *begin, const ItemT *end)
			: m_value(*begin)
		{
			assert((end - begin)==1);
		}

		/**
         * ItemT must be constructible from AddrT
         * NOTICE: no mptr constructor provided because resolving Memspace reference is not possible (hack)
         */
		IttyIndex(std::pair<Memspace*, AddrT> addr)
			: m_value(binary_cast_one<ItemT, AddrT>()(addr.second))
		{
		}

		IttyIndex(const empty_index<AddrT> &in, ItemT value)
			: IttyIndex(in.getMemspace(), value)
		{
		}
		
		AddrT getAddress() const {
			// return value as address
			return binary_cast_one<AddrT, ItemT>()(m_value);
		}

		ItemT getValue() const {
			return m_value;
		}
		
		bool updateExisting(const ItemT &value, ItemT *old_value = nullptr)
		{
			// only update if compares equal
			if (m_compare(m_value, value) || m_compare(value, m_value)) {
				return false;
			}
			if (old_value) {
				*old_value = m_value;
			}
			m_value = value;
			return true;
		}

		bool findOne(ItemT &value) const
		{
			if (m_compare(m_value, value) || m_compare(value, m_value)) {
				return false;				
			}
			value = m_value;
			return true;
		}
		
		/**
         * @return dbzero storage size used by this data structure
         */
		static std::uint64_t getStorageSize() {
			// reported storage size is 0 (since no BN storage is used for itty_index, all data kept in pointer)
			return 0;
		}

		class const_iterator
        {
			ItemT m_value;
		    unsigned int m_pos = 0;

		public :
			const_iterator(const ItemT &value, unsigned int pos)
				: m_value(value)
				, m_pos(pos)
			{
			}

			const_iterator &operator++()
			{
		        assert(m_pos < 1u);
		        ++m_pos;
		        return *this;
			}

			const ItemT &operator*() const {
				return m_value;
			}
		};

		joinable_const_iterator beginJoin(int direction) const {
			return joinable_const_iterator(&m_value, &m_value + 1, &m_value, direction);
		}

		const_iterator begin() const {
			return const_iterator(m_value, 0);
		}

		const_iterator end() const {
            return const_iterator(m_value, 1);
		}
	};
	
} 
