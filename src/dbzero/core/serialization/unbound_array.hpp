// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "Types.hpp"
#include <dbzero/core/metaprog/is_sequence.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0 

{
DB0_PACKED_BEGIN

    /**
     * Unbound array is simply an array, size of which is stored externally
    */
    template <typename T> class DB0_PACKED_ATTR o_unbound_array: public o_base<o_unbound_array<T>, 0, false>
    {
    protected:
        using super_t = o_base<o_unbound_array<T>, 0, false>;
        friend super_t;
        
        o_unbound_array(std::size_t size);

        /**
         * Initialize with default value
        */
        o_unbound_array(std::size_t size, T default_value);

		/**
         * Construct fully initialized
         * create from STL sequence list / vector / set
         */
		template <typename SequenceT, typename std::enable_if<is_sequence<SequenceT>::value, SequenceT>::type* = nullptr>
		explicit o_unbound_array(const SequenceT &data)
		{
            auto out = begin();
			for (const auto &d: data) {
                *out = d;
                ++out;
			}
		}

    public:

        static std::size_t measure(std::size_t size) {
            return super_t::measureMembers() + size * sizeof(T);
        }

        static std::size_t measure(std::size_t size, T) {
            return measure(size);
        }

		template <typename SequenceT, typename std::enable_if<is_sequence<SequenceT>::value, SequenceT>::type* = nullptr>
		static std::size_t measure(const SequenceT &data)
        {
            return measure(data.size());
        }

        std::size_t sizeOf() const {
            throw std::runtime_error("o_unbound_array has no sizeOf");
        }

        T *begin() {
            return reinterpret_cast<T*>(&this->getDynFirst(o_null::type()));
        }

        const T *begin() const {
            return reinterpret_cast<const T*>(&this->getDynFirst(o_null::type()));
        }

        inline T operator[](std::size_t index) const {
            return begin()[index];
        }

        inline T &operator[](std::size_t index) {
            return begin()[index];
        }
        
        inline T get(std::size_t index) const {
            return begin()[index];
        }
    };

    template <typename T> o_unbound_array<T>::o_unbound_array(std::size_t)
    {
    }
    
    template <typename T> o_unbound_array<T>::o_unbound_array(std::size_t size, T default_value) {
        std::fill_n(begin(), size, default_value);
    }

DB0_PACKED_END
}
