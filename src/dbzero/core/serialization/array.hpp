// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "Types.hpp"    
#include <dbzero/core/metaprog/is_sequence.hpp>
#include <dbzero/core/metaprog/misc_utils.hpp>
#include <dbzero/core/platform/utils.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{
DB0_PACKED_BEGIN

	template <typename T, typename SizeT> class o_array;
	template <typename T, typename SizeT> std::ostream &operator<<(std::ostream &, const o_array<T,SizeT> &);

	/**
	 * T - must be o_fixed overlaid type
	 */

	template <typename T, typename SizeT = std::uint32_t>
    class o_array : public o_base<o_array<T, SizeT>, 0, false>
	{
	protected :
		using self = o_array<T, SizeT>;
		friend o_base<o_array<T, SizeT>, 0, false  >;

		explicit o_array(SizeT size)
			: m_header(size)
		{
		}

		/**
         * create from STL sequence list / vector / set
         */
		template <typename SequenceT, typename std::enable_if<is_sequence<SequenceT>::value, SequenceT>::type* = nullptr>
		explicit o_array (const SequenceT &data)
			: o_array(data.size())
		{
			auto arranger = self::arrangeMembers();
			for(const auto &d: data)
			{
				arranger = arranger(T::type(), d);
			}
		}

        DEFINE_HAS_FUNCTION(sizeOf);

	public :
		using iterator = T*;
		using const_iterator = const T*;

        class DB0_PACKED_ATTR o_array_header : public o_fixed<o_array_header> {
        public:
            SizeT m_this_size;

            o_array_header(SizeT size) : m_this_size(size) {}

            std::size_t getOBaseSize() const {
                return o_array::measure(m_this_size);
            }
        };
        using fixed_header_type = o_array_header;

		static size_t measure(SizeT size)
		{
            if constexpr(has_function_sizeOf<T>::value) {
			    return (size_t)(self::measureMembers()) + T::sizeOf() * size;
            }
            else {
                return (size_t)(self::measureMembers()) + sizeof(T) * size;
            }
		}

		template <typename SequenceT, typename std::enable_if<is_sequence<SequenceT>::value, SequenceT>::type* = nullptr>
		static size_t measure(const SequenceT &s)
		{
			return measure(s.size());
		}

		bool empty() const
		{
			return (size()==0);
		}

		SizeT size() const
		{
			return m_header.m_this_size;
		}

		inline T &at(int index)
		{
			assert(index < static_cast<int>(m_header.m_this_size));
			return *(begin() + index);
		}

		inline const T &at(int index) const
		{
			assert(index < static_cast<int>(m_header.m_this_size));
			return *(begin() + index);
		}

		inline T &operator[](int index)
		{
			return at(index);
		}

		inline const T &operator[](int index) const
		{
			return at(index);
		}

		template <typename buf_t> static size_t safeSizeOf(buf_t at)
		{
			std::size_t result = (std::size_t)(self::sizeOfMembers(at)) + T::sizeOf() * self::__const_ref(at).size();
			// bounds check
			at += result;
			return result;
		}

		iterator begin()
		{
			return reinterpret_cast<iterator>(self::beginOfDynamicArea());
		}

		iterator end()
		{
			return begin() + m_header.m_this_size;
		}

		const_iterator begin() const
		{
			return reinterpret_cast<const_iterator>(self::beginOfDynamicArea());
		}

		const_iterator end() const
		{
			return begin() + m_header.m_this_size;
		}
		
	private :
		// header containing size of this array (number of items)
        fixed_header_type m_header;
	};

	
	template <class T, class SizeT> std::ostream &operator<<(std::ostream &os, const o_array<T,SizeT> &array)
	{
		os << "[";
		for(int i=0;(i < array.size());++i)
		{
			if (i!=0)
			{
				os << ",";
			}
			os << array[i];
		}
		os << "]";
		return os;
	}

DB0_PACKED_END
} 
