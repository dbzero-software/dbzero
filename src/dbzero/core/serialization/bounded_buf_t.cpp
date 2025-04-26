#include "bounded_buf_t.hpp"

namespace db0

{
	
	const_bounded_buf_t::const_bounded_buf_t(const std::function<void()> &throw_func)
		: m_throw_func(throw_func)
	{
	}
			
	const_bounded_buf_t::const_bounded_buf_t(const IExceptionFactory &error,const dbz::byte *begin,const dbz::byte *end)
		: error(&error)
		, begin(begin)
		, end(end)
	{
		assert(!(begin > end));
	}

	const_bounded_buf_t::const_bounded_buf_t(const IExceptionFactory &error,const MEM_BLOCK &buf)
		: error(&error)
		, begin(buf.getBuffer())
		, end(buf.getBuffer() + buf.size())
	{
	}

	const_bounded_buf_t::const_bounded_buf_t(const IExceptionFactory &error,const MEMSPEC &buf)
		: error(&error)
		, begin(buf.getBuffer())
		, end(buf.getBuffer() + buf.getSize())
	{
	}

	void const_bounded_buf_t::init(const dbz::byte *begin, const dbz::byte *end) {
		assert(!(begin > end));
		this->begin = begin;
		this->end = end;
	}
		
	void const_bounded_buf_t::init(const MEM_BLOCK &buf) {
		this->begin = buf.getBuffer();
		this->end = buf.getBuffer() + buf.size();
	}

	void const_bounded_buf_t::init(const MEMSPEC &buf) {
		this->begin = buf.getBuffer();
		this->end = buf.getBuffer() + buf.getSize();
	}

	dbz::const_bounded_buf_t::const_bounded_buf_t(const_bounded_buf_t &&other)
		: error(other.error)
		, begin(other.begin)
		, end(other.end)
	{
	}

	dbz::const_bounded_buf_t::const_bounded_buf_t(const const_bounded_buf_t& other)
		: error(other.error)
		, begin(other.begin)
		, end(other.end)
	{
	}

	dbz::bounded_buf_t::bounded_buf_t(const IExceptionFactory& error)
		: super_t(error)
	{
	}

	dbz::bounded_buf_t::bounded_buf_t(bounded_buf_t&& other)
		: super_t(std::move(other))
	{
	}

	dbz::bounded_buf_t::bounded_buf_t(const bounded_buf_t& other)
		: super_t(other)
	{
	}

	dbz::bounded_buf_t::bounded_buf_t(const IExceptionFactory& error, byte* begin, byte* end)
		: super_t(error, begin, end)
	{
	}

	dbz::bounded_buf_t::bounded_buf_t(const IExceptionFactory& error, MEM_BLOCK& buf)
		: super_t(error, buf)
	{
	}

}
