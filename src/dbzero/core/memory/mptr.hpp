// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <cstdlib>
#include <functional>
#include "AccessOptions.hpp"
#include <dbzero/core/memory/Address.hpp>

namespace db0

{

    class Memspace;

	struct mptr
	{
		std::reference_wrapper<Memspace> m_memspace;
		Address m_address = {};
		FlagSet<AccessOptions> m_access_mode;

        mptr() = default;

		inline mptr(Memspace &memspace, Address address, FlagSet<AccessOptions> access_mode = {})
			: m_memspace(memspace)
			, m_address(address)
			, m_access_mode(access_mode)
		{
		}

		/**
         * Combine access modes
         */
		mptr(mptr, FlagSet<AccessOptions> access_mode);

		bool operator==(const mptr &) const;

        bool isNull() const;

		std::size_t getPageSize() const;
	};

}