// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "Base.hpp"
#include <cstdint>
#include <dbzero/core/compiler_attributes.hpp>
	
namespace db0

{
DB0_PACKED_BEGIN
    
    template <class T> class DB0_PACKED_ATTR o_list: public o_base<o_list<T>, 0, false>
    {
    protected :
        using self_t = o_list<T>;
        using super_t = o_base<o_list<T>, 0, false>;
        friend super_t;

        /**
         * Constructs empty list instance
         */
        explicit o_list()
            : count(0)
        {
            size_of = self_t::arrangeMembers();
        }

        o_list(const self_t &other)
            : super_t()
        {
            // copy raw bytes
            memcpy(this, &other, other.sizeOf());
        }

        template <class sequence_t, typename... Args> explicit o_list(const sequence_t &data, Args&& ...args)
            : count(data.size())
        {
            auto arranger = self_t::arrangeMembers();
            auto it = data.begin(), end = data.end();
            while (it != end)
            {
                arranger = arranger(T::type(), *it, std::forward<Args>(args)...);
                ++it;
            }
            size_of = arranger;
        }

    public :
        /** Measure empty list
         *
         */
        static std::size_t measure() {
            return self_t::measureMembers();
        }

        static std::size_t measure(const self_t &other) {
            return other.sizeOf();
        }

        template <typename SequenceT, typename... Args> static std::size_t measure(const SequenceT &data, Args&& ...args)
        {
            auto meter = self_t::measureMembers();
            auto it = data.begin(), end = data.end();
            while (it != end)
            {
                meter = meter(T::type(), *it, std::forward<Args>(args)...);
                ++it;
            }
            return meter;
        }
        
        std::size_t sizeOf () const {
            return static_cast<std::size_t>(size_of);
        }
        
        template <typename buf_t> static std::size_t safeSizeOf(buf_t at)
        {
            std::uint32_t count = self_t::__const_ref(at).count;
            auto meter = self_t::sizeOfMembers(at);
            for (unsigned i = 0;i < count;++i)
            {
                meter = meter(T::type());
            }
            return meter;
        }
        
        inline std::uint32_t size() const {
            return this->count;
        }
        
        bool empty() const {
            return this->count==0;
        }

        class const_iterator
        {
        public :
            // as invalid
            const_iterator() = default;
            const_iterator(const T *item)
                : item(item)			
            {
            }
            
            const T *operator->() const {
                return this->item;
            }
            
            const T &operator*() const {
                return *this->item;
            }
            
            const_iterator &operator++()
            {
                item = (const T*)((char*)item + item->sizeOf());
                return *this;
            }
            
            bool operator==(const const_iterator &it) const {
                return (item==it.item);
            }
            
            bool operator!=(const const_iterator &it) const {
                return (item!=it.item);
            }
            
        protected :
            const T *item = nullptr;
        };
        
        const_iterator begin() const {
            return const_iterator(reinterpret_cast<const T*>(self_t::beginOfDynamicArea()));
        }
        
        const_iterator end() const {
            // past the end of data
            return const_iterator (reinterpret_cast<const T*>(self_t::beginOfMemberArea() + size_of));
        }

    public :
        std::uint32_t size_of;
        // number of list elements
        std::uint32_t count;
    };
    
DB0_PACKED_END
}

