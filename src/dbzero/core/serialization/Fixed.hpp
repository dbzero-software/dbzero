// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <vector>
#include <dbzero/core/serialization/Foundation.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{
DB0_PACKED_BEGIN

    class Memspace;

    /**
    * base class for fixed-size safe overlaid objects ,
    * implements __safe_ref
    * @tparam T - actual implemented overlaid type
    */
    template <typename T> class DB0_PACKED_ATTR o_fixed {
        struct NullInitializer {
        };
    public:
        typedef std::true_type has_constant_size;

    public:
        using Initializer = NullInitializer;

        o_fixed() = default;

        // bcs of constant measure, we can safely call placement new...
        template<typename... Args>
        static T &__new(void *buf, Args &&...args) {
            return *(new(buf) T(std::forward<Args>(args)...));
        }

        static constexpr bool getIsVerStored() {
            return false;
        }

        static constexpr bool isExtType() {
            return false;
        }

        static constexpr std::uint16_t getObjVer() {
            return 0;
        }

        static constexpr std::uint16_t getImplVer() {
            return 0;
        }

        static inline T &__ref(void *buf) {
            return *reinterpret_cast<T *>(buf);
        }

        static inline const T &__const_ref(const void *buf) {
            return *reinterpret_cast<const T *>(buf);
        }

        static constexpr std::size_t sizeOf() {
            return true_size_of<T>();
        }

        static constexpr std::size_t sizeOf(const void *at) {
            return sizeOf();
        }

        // measure size of object
        template<typename... Args>
        static inline std::size_t measure(Args &&... ) {
            return true_size_of<T>();
        }
        
        /**
         * safe object reference (buffer bounds validated by buf_t)
         */
        template<typename buf_t> static T &__safe_ref(buf_t buf)
        {
            const std::byte *_buf = static_cast<std::byte*>(buf);
            // validate bounds here
            buf += true_size_of<T>();
            return __ref(const_cast<std::byte*>(_buf));
        }
        
        template <typename buf_t> static const T &__safe_const_ref(buf_t buf)
        {            
            const std::byte *_buf = static_cast<const std::byte*>(buf);
            // validate bounds here
            buf += true_size_of<T>();
            return __const_ref(_buf);
        }

        template <typename buf_t> static std::size_t safeSizeOf(buf_t buf) 
        {
            // validate bounds
            size_t result = true_size_of<T>();
            buf += result;
            return result;
        }

        void destroy(db0::Memspace &) const {}

        /**
         * swap content of the containers
         */
        void swap(T &other) {
            std::size_t size_of = sizeOf();
            std::vector<std::byte> temp_buf(size_of);
            memcpy(temp_buf.data(), this, size_of);
            memcpy(this, &other, size_of);
            memcpy(&other, temp_buf.data(), size_of);
        }

        /**
         * function required when instantiating type with arranger object
         */
        static db0::Foundation::Type<T> type() {
            return db0::Foundation::Type<T>();
        }

        void assertImplVersion() const {
        }
    };
        
DB0_PACKED_END
}
