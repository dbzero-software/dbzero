// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <vector>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/serialization/packed_int.hpp>
#include <dbzero/core/serialization/list.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{

DB0_PACKED_BEGIN
    template <typename ItemT> struct DB0_PACKED_ATTR o_rle_item: public o_base<o_rle_item<ItemT>, 0, false>
    {
    protected:
        using super_t = o_base<o_rle_item<ItemT>, 0, false>;
        friend super_t;

        o_rle_item(std::pair<ItemT, unsigned int> rle_item)
            : m_value(rle_item.first)
        {
            this->arrangeMembers()
                (packed_int32::type(), rle_item.second);
        }

        // the run_length member
        const packed_int32 &run_length() const {
            return this->getDynFirst(packed_int32::type());
        }

    public:
        ItemT m_value;
                
        std::uint32_t getRunLength() const {
            return run_length();
        }

        static std::size_t measure(std::pair<ItemT, unsigned int> rle_item)
        {
            return super_t::measureMembers()
                (packed_int32::type(), rle_item.second);
        }

        std::size_t sizeOf() const {
            return this->sizeOfMembers()
                (packed_int32::type());
        }

        template <typename T> static std::size_t safeSizeOf(T buf)
        {
            return super_t::sizeOfMembers(buf)
                (packed_int32::type());
        }
    };
DB0_PACKED_END
    
DB0_PACKED_BEGIN
    template <typename ItemT> class DB0_PACKED_ATTR o_rle_sequence : protected o_list<o_rle_item<ItemT> >
    {
    public:
        using super_t = o_list<o_rle_item<ItemT> >;
        template <typename... Args> static o_rle_sequence<ItemT> &__new(void *buf, Args&& ...args) {
            return reinterpret_cast<o_rle_sequence<ItemT> &>(super_t::__new(buf, std::forward<Args>(args)...));
        }

        template <typename SequenceT, typename... Args> 
        static std::size_t measure(const SequenceT &data, Args&& ...args)
        {
            return super_t::measure(data, std::forward<Args>(args)...);            
        }

        std::uint32_t size() const {
            return super_t::size();
        }

        std::size_t sizeOf () const {
            return super_t::sizeOf();
        }

        class ConstIterator
        {
        public:
            // as invalid
            ConstIterator() = default;
            
            bool operator!=(const ConstIterator &other) const {
                return m_item != other.m_item || m_run_length != other.m_run_length;
            }

            ConstIterator &operator++()
            {
                --m_run_length;
                if (m_run_length == 0) {
                    ++m_item;
                    if (m_item != m_end_item) {
                        m_value = (*m_item).m_value;
                        m_run_length = (*m_item).getRunLength();                            
                    }
                } else {
                    ++m_value;                    
                }
                
                return *this;
            }

            ItemT operator*() const {
                return m_value;
            }

        protected:
            friend class o_rle_sequence<ItemT>;
            ConstIterator(typename super_t::const_iterator item, typename super_t::const_iterator end_item)
                : m_item(item)
                , m_end_item(end_item)
            {
                if (m_item != m_end_item) {
                    m_value = (*m_item).m_value;
                    m_run_length = (*m_item).getRunLength();                            
                }
            }

        private:
            typename super_t::const_iterator m_item;
            typename super_t::const_iterator m_end_item;
            ItemT m_value;
            unsigned int m_run_length = 0;
        };

        ConstIterator begin() const {
            return { super_t::begin(), super_t::end() };
        }

        ConstIterator end() const {
            return { super_t::end(), super_t::end() };            
        }

        static auto type() {
            return super_t::type();
        }

        template <typename T> static std::size_t safeSizeOf(T buf) {
            return super_t::safeSizeOf(buf);
        }
    };
DB0_PACKED_END

    /**
     * RLE-compressed, sorted sequence of items
     * ItemT must implement: operator-, operator<, operator+=, operator++
     */
    template <typename ItemT> class RLE_SequenceBuilder
    {
    public:        
        RLE_SequenceBuilder() = default;
        
        /**
         * Append next item, items must be appended in ascending order
         * @param add_duplicate if false, the consecutive duplicate element will be ignored
         */
        void append(ItemT item, bool add_duplicate = true);
        
        // build the RLE sequence using a specific vector as buffer
        const o_rle_sequence<ItemT> &build(std::vector<char> &buffer) const;
        
        const std::vector<std::pair<ItemT, unsigned int> > &getData() const;

        // measure size of the rle_sequence to be created
        std::size_t measure() const;

        bool empty() const;

        void clear();

    private:
        ItemT m_last_item;
        mutable unsigned int m_run_length = 0;
        mutable std::vector<std::pair<ItemT, unsigned int> > m_data;
    };
    
    template <typename ItemT> void RLE_SequenceBuilder<ItemT>::append(ItemT item, bool add_duplicate)
    {
        if (m_run_length == 0) {
            m_last_item = item;
            m_run_length = 1;
        } else if (item - m_last_item == m_run_length) {
            ++m_run_length;
        } else if (add_duplicate || item != (m_last_item + m_run_length - 1)) {
            m_data.emplace_back(m_last_item, m_run_length);
            m_last_item = item;
            m_run_length = 1;
        }
    }

    template <typename ItemT> const std::vector<std::pair<ItemT, unsigned int> > &RLE_SequenceBuilder<ItemT>::getData() const
    {
        if (m_run_length > 0) {
            m_data.emplace_back(m_last_item, m_run_length);
            m_run_length = 0;
        }
        return m_data;
    }

    template <typename ItemT> std::size_t RLE_SequenceBuilder<ItemT>::measure() const {
        return o_rle_sequence<ItemT>::measure(getData());
    }

    template <typename ItemT> const o_rle_sequence<ItemT> &RLE_SequenceBuilder<ItemT>::build(std::vector<char> &buffer) const
    {
        auto size_of = this->measure();
        if (buffer.size() < size_of) {
            buffer.resize(size_of);
        }
        return o_rle_sequence<ItemT>::__new(buffer.data(), m_data);
    }
    
    template <typename ItemT> bool RLE_SequenceBuilder<ItemT>::empty() const {
        return m_data.empty() && m_run_length == 0;
    }
    
    template <typename ItemT> void RLE_SequenceBuilder<ItemT>::clear()
    {
        m_run_length = 0;
        m_data.clear();
    }
    
}
