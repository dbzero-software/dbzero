// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "ansi_ptr.hpp"
#include "packed_int.hpp"
#include <iostream>
#include <cstring>
#include <iostream>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{

    /**
     * StrT  - underlying string pointer type (utf8_ptr / ansi_ptr)
     * @tparam is_nullable flag indicating if this string can be nullable
     * NOTICE : length of the string is stored with the packed_int member
     */
DB0_PACKED_BEGIN
	template <typename StrT, bool is_nullable = false> class DB0_PACKED_ATTR o_base_string:
	public o_base<o_base_string<StrT, is_nullable>, 0, false>
    {
	protected:
		using self = o_base_string<StrT, is_nullable>;
		using super_t = o_base<o_base_string<StrT, is_nullable>, 0, false >;
        using SizeType = db0::o_packed_int<std::uint32_t, is_nullable>;
        friend super_t;

        // new empty string (or null if nullable)
        o_base_string()
        {
            if constexpr (is_nullable) {
                this->arrangeMembers()
                        (SizeType::type());
            } else {
                this->arrangeMembers()
                        (SizeType::type(), 0);
            }
        }

        o_base_string(const StrT &data)
        {
            if constexpr (is_nullable) {
                if (data.isNull()) {
                    // create null instance
                    this->arrangeMembers()
                            (SizeType::type());
                    return;
                }
            }
            std::byte *buf = (std::byte*)this;
            std::size_t str_size = data.size();
            buf += SizeType::__new(buf, (std::uint32_t)str_size).sizeOf();
            std::memcpy(buf, data.begin().get_uraw(), str_size);
        }

        o_base_string(const std::string &data)
            : o_base_string(StrT(data))
        {
        }

        o_base_string(const char *data)
            : o_base_string(StrT(data))
        {
        }

        o_base_string(const self &other)
            : super_t()
        {
            std::memcpy((std::byte*)this, (std::byte*)&other, other.sizeOf());
        }

	public :
		using Initializer = StrT;

		static std::size_t measure()
        {
		    if constexpr (is_nullable) {
                return SizeType::measure();
		    } else {
                return SizeType::measure(0);
            }
		}

		static std::size_t measure(const Initializer &data) 
		{
			size_t size = data.size();
			size += SizeType::measure((std::uint32_t)size);
			return size;
		}

		static std::size_t measure(const std::string &data) {
			return measure(StrT(data));
		}

		static std::size_t measure(const char *data) {
			return measure(StrT(data));
		}

		template <typename buf_t> static std::size_t safeSizeOf(buf_t at)
        {
		    auto buf = at;
            buf += SizeType::safeSizeOf(at);
            const auto &len = SizeType::__const_ref(at);
			if constexpr (is_nullable) {
			    if (!len.isNull()) {
                    buf += len.value();
			    }
			} else {
			    buf += len.value();
            }
			return buf - at;
		}

		StrT get() const
        {
		    const auto &len = getSizeMember();
		    if constexpr (is_nullable) {
                if (len.isNull()) {
                    return StrT();
                }
		    }
			std::byte *buf = (std::byte*)this + len.sizeOf();
			return StrT((const char*)buf, (const char*)(buf + len.value()));
		}

		operator StrT() const {
			return get();
		}

		std::string toString() const {
			return get().toString();
		}

		std::string extract() const {
			return get().toString();
		}

		void extract(std::string &result) const {
			result = get().toString();
		}

		operator std::string () const {
			return get().toString();
		}

		/**
         * Default comparator (less)
         */
		struct comp_t
        {
			bool operator()(const o_base_string &lhs, const o_base_string &rhs) const {
				return lhs.get() < rhs.get();
			}

			bool operator()(const char *lhs, const o_base_string &rhs) const {
				return StrT(lhs) < rhs.get();
			}

			bool operator()(const std::string &lhs, const o_base_string &rhs) const {
				return StrT(lhs) < rhs.get();
			}

			bool operator()(const o_base_string &lhs, const char *rhs) const {
				return lhs.get() < StrT(rhs);
			}

			bool operator()(const o_base_string &lhs, const std::string &rhs) const {
				return lhs.get() < StrT(rhs);
			}
		};

		void unpack(Initializer &result) const {
			result = get();
		}

		bool operator<(const o_base_string &other) const {
			return get() < other.get();
		}

		bool operator<(const std::string &str) const {
			return get() < StrT(str);
		}

		bool operator>(const std::string &str) const {
			return get() > StrT(str);
		}

		bool operator<(StrT other) const {
			return get() < other;
		}
		
		bool operator>(const o_base_string &other) const {
			return get() > other.get();
		}

		bool operator>(StrT other) const {
			return get() > other;
		}

		bool operator==(const std::string &str) const {
			bool result = get() == StrT(str);
			return result;
		}

		bool operator==(const o_base_string &other) const {
			bool result = get() == other.get();
			return result;
		}

		bool operator==(const char *other) const {
			bool result = get() == StrT(other);
			return result;
		}

		bool operator==(const StrT& other) const {
			bool result = get() == other;
			return result;
		}

		std::size_t getHash() const {
			return get().getHash();
		}

		/**
         * str - some string initializer type
         */
		template <class T> static std::size_t getHash(const T &str) {
			return StrT(str).getHash();
		}

		typename StrT::formatter delimited(const std::string &str) const {
			return get().delimited(str);
		}

		friend std::ostream &operator<<(std::ostream &os, const self &str) {
			return os << str.toString();
		}

		inline bool isNull() const
		{
		    if constexpr (is_nullable) {
		        return getSizeMember().isNull();
		    } else {
		        return false;
		    }
		}

		/**
		 * Get size (number of bytes) of the underlying string object
		 * @return size of the string
		 */
		inline std::size_t size() const {
		    return getSizeMember().value();
		}

		/**
		 * Get length (as number of characters) of the underlyging string
		 * @return string length
		 */
		std::size_t length() const {
            return get().length();
		}

    private:

        inline const SizeType &getSizeMember() const {
            return this->getDynFirst(SizeType::type());
        }
	};
DB0_PACKED_END
    
	using o_string = db0::o_base_string<db0::ansi_cs_ptr>;
    using o_nullable_string = db0::o_base_string<db0::ansi_cs_ptr, true>;

} 

namespace std 

{

	// std::hash specialization for o_base_string type family
	template <> struct hash<db0::o_string> {
		std::size_t operator()(const db0::o_string &str) const noexcept {
			return std::hash<std::string>()(str.toString());
		}
	};

	template <> struct hash<db0::o_nullable_string> {
		std::size_t operator()(const db0::o_nullable_string &str) const noexcept {
			return std::hash<std::string>()(str.toString());
		}
	};

} 
