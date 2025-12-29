// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstring>

namespace db0 

{

	// one-way binary cast
	template <typename ToType, typename FromType> struct binary_cast_one
	{
		ToType operator()(FromType value) const
		{
			static_assert(sizeof(ToType) >= sizeof(FromType), "destination type size not sufficient");
			// this is to avoid default constructor
			char result[sizeof(ToType)];
#ifdef  __linux__
	#pragma GCC diagnostic push
	#pragma GCC diagnostic ignored "-Wclass-memaccess"
#endif
			std::memcpy(result, &value, sizeof(value));
#ifdef  __linux__
	#pragma GCC diagnostic pop
#endif
			return *reinterpret_cast<const ToType*>(result);
		}
	};
	
	/**
	 * This is a two-way binary cast
	 * Provides a binary cast with the use of memcpy from a smaller or equal size type to a larger size type
	 * T_MORE - type of larger size (storage)
	 */
	template <typename ToType, typename FromType> struct binary_cast
	{
		ToType operator()(FromType value) const {
			return binary_cast_one<ToType, FromType>()(value);
		}

		FromType operator()(ToType value) const volatile {
			return binary_cast_one<FromType, ToType>()(value);
		}
	};

} 
