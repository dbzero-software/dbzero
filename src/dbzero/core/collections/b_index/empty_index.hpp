// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/memory/Memspace.hpp>

namespace db0 

{
    
    /**
     * Implementation of empty data index
     */
	template <typename AddrT> class empty_index
    {
		Memspace &m_memspace;

	public :
        using addr_t = AddrT;
		empty_index(Memspace &);

		empty_index(std::pair<Memspace *, std::uint64_t>);

		Memspace &getMemspace();

		Memspace &getMemspace() const;
        
		/**
         * @return dbzero storage size used by this data structure
         */
		static std::uint64_t getStorageSize() {
			return 0;
		}

		class const_iterator {};

		static const_iterator begin() {
			return const_iterator();
		}
	};

	template <typename AddrT> empty_index<AddrT>::empty_index(Memspace &memspace)
        : m_memspace(memspace)
    {
    }

    template <typename AddrT> empty_index<AddrT>::empty_index(std::pair<Memspace*, std::uint64_t> addr)
        : m_memspace(*addr.first)
    {
    }
	
    template <typename AddrT> Memspace &empty_index<AddrT>::getMemspace() {
        return m_memspace;
    }

    template <typename AddrT> Memspace &empty_index<AddrT>::getMemspace() const {
        return m_memspace;
    }

}
