// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "Types.hpp"
#include "packed_int.hpp"
#include <dbzero/core/metaprog/is_sequence.hpp>
#include <dbzero/core/compiler_attributes.hpp>
#include <cassert>

namespace db0 

{
    
DB0_PACKED_BEGIN
    
    /**
     * Constant-capacity array of fixed-size elements with variable-length packed header
     * NOTE: offset is an optional member, only stored but not used for accessing elements
    */
    template <typename T, bool has_offset = false> class DB0_PACKED_ATTR o_micro_array:
    public o_base<o_micro_array<T, has_offset>, 0, false>
    {
    protected:
        using super_t = o_base<o_micro_array<T, has_offset>, 0, false>;
        friend super_t;
        
        // Initialize with default value
        o_micro_array(std::size_t size, T default_value, unsigned int offset = 0);
        
        // Initialize without default value
        o_micro_array(std::size_t size, unsigned int offset = 0);

		/**
         * Construct fully initialized
         * create from STL sequence list / vector / set
         */
		template <typename SequenceT, typename std::enable_if<is_sequence<SequenceT>::value, SequenceT>::type* = nullptr>
		explicit o_micro_array(const SequenceT &data, unsigned int offset = 0)
		{
            if constexpr (has_offset) {
                // add one extra element to store offset
                this->arrangeMembers()
                    (packed_int32::type(), data.size())
                    (packed_int32::type(), offset);
            } else {
                assert(offset == 0);
                this->arrangeMembers()
                    (packed_int32::type(), data.size());
            }

            auto out = begin();
			for (const auto &d: data) {
                *out = d;
                ++out;
			}
		}

        /**
         * Initialize from a possibly empty range of values
        */
        o_micro_array(const T *begin = nullptr, const T *end = nullptr, unsigned int offset = 0)
        {
            if constexpr (has_offset) {
                // add one extra element to store offset
                this->arrangeMembers()
                    (packed_int32::type(), end - begin)
                    (packed_int32::type(), offset);
            } else {
                assert(offset == 0);
                this->arrangeMembers()
                    (packed_int32::type(), end - begin);
            }

            auto out = this->begin();
            for (auto it = begin; it != end; ++it) {
                *out = *it;
                ++out;
            }
        }

    public:
        const packed_int32 &packed_size() const;
    
        std::size_t size() const;

        template <bool B = has_offset>
        typename std::enable_if<B, const packed_int32&>::type packed_offset() const {
            return this->getDynAfter(packed_size(), packed_int32::type());
        }

        template <bool B = has_offset>
        typename std::enable_if<B, unsigned int>::type offset() const {
            return packed_offset().value();
        }

        // Decoding both packed members at once
        template <bool B = has_offset>
        typename std::enable_if<B, std::pair<std::size_t, unsigned int> >::type getSizeAndOffset() const 
        {
            std::pair<std::size_t, unsigned int> result;
            const std::byte *buf = this->beginOfDynamicArea();
            result.first = packed_int32::read(buf);
            result.second = packed_int32::read(buf);
            return result;
        }
        
        // Try finding element with a specific index, return its position if found
        bool find(unsigned int index, unsigned int &pos) const;

        static std::size_t measure(std::size_t size, unsigned int offset = 0);

        static std::size_t measure(std::size_t size, T, unsigned int offset = 0);

        template <typename SequenceT, typename std::enable_if<is_sequence<SequenceT>::value, SequenceT>::type* = nullptr>
        static std::size_t measure(const SequenceT &data, unsigned int offset = 0)
        {
            return measure(data.size(), offset);
        }

        static std::size_t measure(const T *begin = nullptr, const T *end = nullptr, unsigned int offset = 0) {
            return measure(end - begin, offset);
        }

        template <typename BufT> static std::size_t safeSizeOf(BufT buf)
        {
            std::size_t size;
            if constexpr (has_offset) {
                size = super_t::sizeOfMembers(buf)
                    (packed_int32::type())
                    (packed_int32::type());
            } else {
                size = super_t::sizeOfMembers(buf)
                    (packed_int32::type());
            }
            return size + super_t::__const_ref(buf).size() * sizeof(T);
        }
        
        inline T *begin()
        {
            if constexpr (has_offset) {
                return reinterpret_cast<T*>(&this->getDynAfter(packed_offset(), o_null::type()));
            } else {
                return reinterpret_cast<T*>(&this->getDynAfter(packed_size(), o_null::type()));
            }
        }

        inline const T *begin() const
        {
            if constexpr (has_offset) {
                return reinterpret_cast<const T*>(&this->getDynAfter(packed_offset(), o_null::type()));
            } else {
                return reinterpret_cast<const T*>(&this->getDynAfter(packed_size(), o_null::type()));
            }
        }

        inline T *end() {
            return begin() + size();
        }

        inline const T *end() const {
            return begin() + size();
        }

        inline T operator[](std::size_t index) const {
            return begin()[index];            
        }

        inline T &operator[](std::size_t index) {
            return begin()[index];            
        }        
    };
    
    template <typename T, bool has_offset>
    o_micro_array<T, has_offset>::o_micro_array(std::size_t size, unsigned int offset)
    {
        if constexpr (has_offset) {
            // add one extra element to store offset
            this->arrangeMembers()
                (packed_int32::type(), size)
                (packed_int32::type(), offset);
        } else {
            assert(offset == 0);        
            this->arrangeMembers()
                (packed_int32::type(), size);
        }
    }
    
    template <typename T, bool has_offset>
    o_micro_array<T, has_offset>::o_micro_array(std::size_t size, T default_value, unsigned int offset)
        : o_micro_array(size, offset)
    {
        std::fill_n(begin(), size, default_value);
    }
    
    template <typename T, bool has_offset> 
    const packed_int32 &o_micro_array<T, has_offset>::packed_size() const {
        return this->getDynFirst(packed_int32::type());
    }
    
    template <typename T, bool has_offset> 
    std::size_t o_micro_array<T, has_offset>::measure(std::size_t size, unsigned int offset)
    {
        std::size_t result;
        if constexpr (has_offset) {
            result = super_t::measureMembers()
                (packed_int32::type(), size)
                (packed_int32::type(), offset);          
        } else {
            assert(offset == 0);
            result = super_t::measureMembers()
                (packed_int32::type(), size);
        }
        
        result += size * sizeof(T);
        return result;
    }

    template <typename T, bool has_offset> 
    std::size_t o_micro_array<T, has_offset>::measure(std::size_t size, T, unsigned int offset) {
        return measure(size, offset);
    }

    template <typename T, bool has_offset> 
    std::size_t o_micro_array<T, has_offset>::size() const {
        return packed_size().value();
    }

    template <typename T, bool has_offset>
    bool o_micro_array<T, has_offset>::find(unsigned int index, unsigned int &pos) const 
    {
        if constexpr (has_offset) {
            auto [size, offset] = this->getSizeAndOffset();
            if (index < offset || index >= offset + size) {
                // index not in the range
                return false;
            }
            pos = index - offset;
            return true;
        }
        if (index >= this->size()) {
            // index not in the range
            return false;
        }
        
        pos = index;
        return true;
    }

DB0_PACKED_END

}
