#pragma once

#include "Types.hpp"
#include "packed_int.hpp"
#include <dbzero/core/metaprog/is_sequence.hpp>

namespace db0 

{
    
    /**
     * Constant-capacity array of fixed-size elements with variable-length packed header
    */
    template <typename T> class [[gnu::packed]] o_micro_array: 
    public o_base<o_micro_array<T>, 0, false>
    {
    protected:
        using super_t = o_base<o_micro_array<T>, 0, false>;
        friend super_t;

        /**
         * Initialize with default value
        */
        o_micro_array(std::size_t size, T default_value);

        /**
         * Initialize without default value
        */
        o_micro_array(std::size_t size);

		/**
         * Construct fully initialized
         * create from STL sequence list / vector / set
         */
		template <typename SequenceT, typename std::enable_if<is_sequence<SequenceT>::value, SequenceT>::type* = nullptr>
		explicit o_micro_array(const SequenceT &data)
		{
            this->arrangeMembers()
                (packed_int32::type(), data.size());

            auto out = begin();
			for (const auto &d: data) {
                *out = d;
                ++out;
			}
		}

        /**
         * Initialize from a possibly empty range of values
        */
        o_micro_array(const T *begin = nullptr, const T *end = nullptr)
        {
            this->arrangeMembers()
                (packed_int32::type(), end - begin);

            auto out = this->begin();
            for (auto it = begin; it != end; ++it) {
                *out = *it;
                ++out;
            }
        }

    public:

        const packed_int32 &packed_size() const;

        std::size_t size() const;

        static std::size_t measure(std::size_t size);

        static std::size_t measure(std::size_t size, T);

        template <typename SequenceT, typename std::enable_if<is_sequence<SequenceT>::value, SequenceT>::type* = nullptr>
        static std::size_t measure(const SequenceT &data)
        {
            return measure(data.size());
        }

        static std::size_t measure(const T *begin = nullptr, const T *end = nullptr) {
            return measure(end - begin);
        }
        
        std::size_t sizeOf() const;

        template <typename BufT> static std::size_t safeSizeOf(BufT buf)
        {
            auto start = buf;
            auto size = packed_int32::__const_ref(buf).value();
            buf += packed_int32::safeSizeOf(buf);
            buf += size * sizeof(T);
            return buf - start;
        }
        
        inline T *begin() {
            return reinterpret_cast<T*>(&this->getDynAfter(packed_size(), o_null::type()));
        }

        inline const T *begin() const {
            return reinterpret_cast<const T*>(&this->getDynAfter(packed_size(), o_null::type()));
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
    
    template <typename T> o_micro_array<T>::o_micro_array(std::size_t size)
    {
        this->arrangeMembers()
            (packed_int32::type(), size);        
    }

    template <typename T> o_micro_array<T>::o_micro_array(std::size_t size, T default_value)
        : o_micro_array(size)
    {
        std::fill_n(begin(), size, default_value);
    }
    
    template <typename T> const packed_int32 &o_micro_array<T>::packed_size() const {
        return this->getDynFirst(packed_int32::type());
    }

    template <typename T> std::size_t o_micro_array<T>::measure(std::size_t size)
    {
        std::size_t result = super_t::measureMembers()
            (packed_int32::type(), size);
        
        result += size * sizeof(T);
        return result;
    }

    template <typename T> std::size_t o_micro_array<T>::measure(std::size_t size, T) {
        return measure(size);
    }
    
    template <typename T> std::size_t o_micro_array<T>::sizeOf() const
    { 
        auto result = packed_size().sizeOf();
        result += packed_size().value() * sizeof(T);
        return result;
    }

    template <typename T> std::size_t o_micro_array<T>::size() const {
        return packed_size().value();
    }

}