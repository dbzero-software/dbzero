// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "Types.hpp"
#include <cassert>

namespace db0

{

    o_binary::o_binary(const o_binary &other)
        : super_t()
        , m_bytes(other.m_bytes)
    {
        std::copy(&other.m_buf, &other.m_buf + m_bytes, &m_buf);
    }

	o_binary::o_binary(std::size_t size)
	    : m_bytes(size)
	{
	}

	o_binary::o_binary(const std::byte *data, std::size_t data_size)
	    : m_bytes(data_size)
	{
        std::copy(data, data + data_size, &m_buf);
	}

	o_binary::o_binary(const std::vector<std::byte> &data)
		: m_bytes(data.size())
	{
		std::copy(data.data(), data.data() + m_bytes, &m_buf);
	}

	o_binary &o_binary::operator=(const o_binary &binary) 
	{
		assert(m_bytes == binary.size());
		// avoid copy into itself
		if (this == &binary) {
			return *this;
		}
		std::copy(binary.begin(), binary.end(), this->begin());
		return *this;
	}

	std::uint32_t o_binary::size() const {
		return m_bytes;
	}

	std::byte *o_binary::getBuffer() {
		return &m_buf;
	}

	const std::byte *o_binary::getBuffer() const {
		return &m_buf;
	}

	std::size_t o_binary::measure(std::size_t content_size) {
		return sizeof(std::uint32_t) + content_size;
	}

	std::size_t o_binary::measure(const std::vector<std::byte> &data)
	{
		return measure(data.size());
	}
	
    std::size_t o_binary::measure(const o_binary &other) {
        return other.sizeOf();
    }

    std::size_t o_binary::measure(const std::byte *, std::size_t content_size) {
        return measure(content_size);
    }

	o_null &o_null::__ref(void *buf) {
		return *reinterpret_cast<o_null *>(buf);
	}

	const o_null &o_null::__const_ref(const void *buf) {
		return *reinterpret_cast<const o_null *>(buf);
	}

	size_t o_null::sizeOf() {
		return 0;
	}

	void o_null::destroy(db0::Memspace &) const {
	}
    
	std::uint16_t o_null::getVersion() const {
		return 0;
	}

	constexpr bool o_null::versionIsStored() {
		return false;
	}

	bool db0::o_binary::empty() const {
		return m_bytes == 0;
	}

}
