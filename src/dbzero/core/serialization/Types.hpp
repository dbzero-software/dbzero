// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <vector>
#include <dbzero/core/serialization/Base.hpp>
#include <dbzero/core/serialization/Fixed.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0 

{

    class Memspace;

    template <class T> class o_simple;
    template <class T> std::ostream &operator<<(std::ostream &os, const o_simple<T> &);

    /**
     * Overlaid simple type wrapper
     */
DB0_PACKED_BEGIN
    template <class T> class DB0_PACKED_ATTR o_simple: public o_fixed<o_simple<T> >
    {
    public :
        // ctor init data
        template<typename... Args> inline o_simple(Args&& ... args)
            : m_data(std::forward<Args>(args)...)
        {
        }

        inline o_simple(const o_simple<T> &other)
            : m_data(other.m_data)
        {
        }

        inline operator T() const {
            return m_data;
        }

        inline void operator=(T value) {
            m_data = value;
        }

        bool operator<(const o_simple &other) const {
            return m_data < other.m_data;
        }

        bool operator>(const o_simple &other) const {
            return m_data > other.m_data;
        }

        bool operator!=(const o_simple &other) const {
            return m_data != other.m_data;
        }

        bool operator<(T other) const {
            return m_data < other;
        }

        bool operator>(T other) const {
            return m_data > other;
        }

        bool operator==(T other) const {
            return m_data == other;
        }

        bool operator!=(T other) const {
            return m_data != other;
        }

        std::string toString() const {
            std::stringstream str;
            str << m_data;
            return str.str();
        }

        inline T value() const {
            return m_data;
        }

        friend std::ostream &operator<<(std::ostream &os,const o_simple<T> &simple_item) {
            return os << simple_item.value();
        }

        friend std::istream &operator>>(std::istream &in, o_simple<T> &simple_item) {
            T value;
            in >> value;
            simple_item = value;
            return in;
        }

    private:
        T m_data;
    };

    /**
     * General purpose variable length binary buffer
     */
    class DB0_PACKED_ATTR o_binary : public o_base<o_binary, 0, false>
    {
    protected:
        using super_t = o_base<o_binary, 0, false>;
        friend super_t;

        /// Construct empty
        o_binary() = default;

        o_binary(const o_binary &);

        o_binary(std::size_t);

        o_binary(const std::byte *, std::size_t size);

        o_binary(const std::vector<std::byte> &);

    public:
        /**
         * Get content size
         */
        std::uint32_t size() const;

        std::byte *getBuffer();

        const std::byte *getBuffer() const;

        inline std::byte *begin() {
            return &m_buf;
        }

        inline const std::byte *begin() const {
            return &m_buf;
        }

        inline const std::byte *end() const {
            return &m_buf + m_bytes;
        }

        bool empty() const;

        template <class buf_t> static std::size_t safeSizeOf(buf_t at) 
        {
            auto buf = at;
            buf += sizeof(m_bytes); // size member
            buf += o_binary::__const_ref(at).m_bytes; // content size
            return buf - at;
        }

        /**
         * @param content_size size of the content (in bytes) to be stored as o_binary
         */
        static std::size_t measure(std::size_t content_size);

        static std::size_t measure(const o_binary &);

        static std::size_t measure(const std::byte *, std::size_t);

        static std::size_t measure(const std::vector<std::byte> &);

        o_binary &operator=(const o_binary &binary);

        o_binary &operator=(const std::vector<std::byte> &);
        
        bool operator==(const std::string &s) const;

        bool operator!=(const std::string &s) const;

        static constexpr std::size_t sizeOfFixedPart() {
            return sizeof(m_bytes);
        }

    private :
        std::uint32_t m_bytes = 0;
        std::byte m_buf;
    };
DB0_PACKED_END

    /**
     * Overlaid null type (derived from o_base)
     */
DB0_PACKED_BEGIN
    class DB0_PACKED_ATTR o_null: public o_base<o_null>
    {
    public :
        template <typename... Args> static inline o_null &__new(void *buf, Args&& ...args)
        {
            return *reinterpret_cast<o_null*>(buf);
        }

        static o_null &__ref(void *buf);
        static const o_null &__const_ref(const void *buf);
        static size_t sizeOf();

        template<typename... Args> static inline size_t measure(Args&&... args)
        {
            return 0;
        }

        template <class buf_t> o_null &__safe_ref(buf_t buf)
        {
            return __ref(buf);
        }

        template <class buf_t> static std::size_t safeSizeOf(buf_t)
        {
            return 0;
        }

        void destroy(db0::Memspace &) const;
        std::uint16_t getVersion() const;
        static constexpr bool versionIsStored();
    };
DB0_PACKED_END
    
DB0_PACKED_BEGIN
    // NOTE: when used as a base class, the sizeof should be zero! (due to empty base optimization)
    struct DB0_PACKED_ATTR o_fixed_null: public o_fixed<o_fixed_null>
    {
        static std::size_t sizeOf() {
            return 0;
        }
    };
DB0_PACKED_END    
    
    /// some predefined simple overlaid types
    using o_int = o_simple<int>;
    using o_uint = o_simple<std::uint32_t>;
    using o_float = o_simple<float>;
    using o_double = o_simple<double>;

}

namespace std

{

    // and hashes to some predefined simple overlaid types
    template<typename T> class hash<db0::o_simple<T> > : public hash<T> {};

} // std namespace {
