// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "bounded_buf_t.hpp"

namespace db0

{
	
	const_bounded_buf_t::const_bounded_buf_t(const std::function<void()> &throw_func)
		: m_throw_func(throw_func)
	{
	}

	const_bounded_buf_t::const_bounded_buf_t(const std::function<void()> &throw_func, 
		const std::byte *begin, const std::byte *end)
		: m_throw_func(throw_func)
		, begin(begin)
		, end(end)
	{
		assert(!(begin > end));
	}

	void const_bounded_buf_t::init(const std::byte *begin, const std::byte *end)
	{
		assert(!(begin > end));
		this->begin = begin;
		this->end = end;
	}

	const_bounded_buf_t::const_bounded_buf_t(const const_bounded_buf_t &other)
		: m_throw_func(other.m_throw_func)
		, begin(other.begin)
		, end(other.end)
	{
	}

	bounded_buf_t::bounded_buf_t(const std::function<void()> &throw_func)
		: super_t(throw_func)
	{
	}

	bounded_buf_t::bounded_buf_t(const bounded_buf_t &other)
		: super_t(other)
	{
	}

	bounded_buf_t::bounded_buf_t(const std::function<void()> &throw_func,
		std::byte *begin, std::byte *end)
		: super_t(throw_func, begin, end)
	{
	}

}
