#pragma once

#include <cstdint>
#include <cstdlib>
#include "AccessOptions.hpp"

namespace db0

{

    class Memspace;

	struct mptr
	{
		std::reference_wrapper<Memspace> m_memspace;
		std::uint64_t m_address = 0;		
		FlagSet<AccessOptions> m_access_mode;

        mptr() = default;

		inline mptr(Memspace &memspace, std::uint64_t address, FlagSet<AccessOptions> access_mode = {})
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