// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cassert>
#include <cstddef>
#include <vector>
#include <functional>
#include <cstdint>

namespace db0

{

	/**
	 * Buffer with validated bounds, unsigned char * compatible type
	 * throws user defined exception
	 */
	class const_bounded_buf_t
	{
	public :
		const_bounded_buf_t(const std::function<void()> &throw_func);

		const_bounded_buf_t(const const_bounded_buf_t &);
		
		const_bounded_buf_t(const std::function<void()> &throw_func, const std::byte *begin, const std::byte *end);

		const_bounded_buf_t(const std::function<void()> &throw_func, const std::vector<std::byte> &buf);

		void init(const std::byte *begin, const std::byte *end);

		void init(const std::vector<std::byte> &);
		
		inline const std::byte *get() const {
			return begin;
		}

		inline const std::byte &operator*() const {
			return *get();
		}

		inline operator const std::byte*() const {
			return get();
		}

		/**
         * Validate bounds, throws
         */
		inline void operator+=(std::size_t size)
		{
			begin += size;
			if (begin > end) {
				m_throw_func();
			}
		}

		inline const_bounded_buf_t &operator++()
		{
			++begin;
			if (begin > end) {
				m_throw_func();
			}
			return *this;
		}

		void operator=(const const_bounded_buf_t &&other)
		{
			m_throw_func = other.m_throw_func;
			begin = other.begin;
			end = other.end;
		}

		/**
         * Only accepted to assign buf = 0
         * this is the method of the hi-bound validation
         */
		void operator=(const std::byte *)
		{
			if (begin != end) {
                m_throw_func();
			}
		}

		class const_ref_t
		{
		public :
			inline const_ref_t(const const_bounded_buf_t &buf, size_t offset)
				: buf(buf)
				, offset(offset)
			{
			}

			inline const_bounded_buf_t operator&() {
				return const_bounded_buf_t(buf.m_throw_func, buf.begin + offset,buf.end);
			}

		private :
			const const_bounded_buf_t &buf;
			std::size_t offset;
		};

		inline const_ref_t operator[](std::size_t offset) const 
		{
			if (begin + offset > end) {
				m_throw_func();
			}
			return const_ref_t(*this, offset);
		}

		/**
		 * Calculate difference, bounds not validated here
		 */
		std::size_t operator-(const const_bounded_buf_t &other) {
            return begin - other.begin;
		}

	protected:
		friend class const_ref_t;
		std::reference_wrapper<const std::function<void()> > m_throw_func;
		const std::byte *begin = 0;
		const std::byte *end = 0;
	};

	class bounded_buf_t: public const_bounded_buf_t
	{
		using super_t = const_bounded_buf_t;

	public :
		bounded_buf_t(const std::function<void()> &throw_func);
		bounded_buf_t(const bounded_buf_t &);
		bounded_buf_t(const std::function<void()> &throw_func, std::byte *begin, std::byte *end);
		bounded_buf_t(const std::function<void()> &throw_func, std::vector<std::byte> &buf);
	};

}