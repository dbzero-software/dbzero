#pragma once

#include <cstring>

namespace db0 

{
    
	/**
	 * Provides a binary cast with the use of memcpy from a smaller or equal size type to a larger size type
	 * T_MORE - type of larger size (storage)
	 */
	template <typename ToType, typename FromType> struct binary_cast
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

		/**
         * NOTICE: volatile used here to be able to compile with ToType = FromType (identical types)
         */
		FromType operator()(ToType value) const volatile
		{
			static_assert(sizeof(ToType) >= sizeof(FromType), "destination type size not sufficient");
			// this is to avoid the default constructor
			char result[sizeof(FromType)];
#ifdef  __linux__
	#pragma GCC diagnostic push
	#pragma GCC diagnostic ignored "-Wclass-memaccess"
#endif
			std::memcpy(result, &value, sizeof(value));
#ifdef  __linux__
	#pragma GCC diagnostic pop
#endif
			return *reinterpret_cast<const FromType*>(result);
		}
	};

} 
