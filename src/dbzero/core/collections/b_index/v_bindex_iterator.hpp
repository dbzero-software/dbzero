// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <functional>
#include <dbzero/core/collections/b_index/bindex_types.hpp>

namespace db0 

{

    template <typename item_t, typename AddrT, typename item_comp_t = std::less<item_t> >
	class v_bindex_iterator
    {
		using types_t = bindex_types<item_t, AddrT, item_comp_t>;
        using node_iterator = typename types_t::node_iterator;
		using DataVectorT = typename types_t::data_vector;

    public:
        using DestroyF = std::function<void(const item_t&)>;

        v_bindex_iterator() = default;

        v_bindex_iterator(Memspace &memspace, const node_iterator &it_node,
            const node_iterator &it_begin, const node_iterator &it_end, const DataVectorT &data_buf, 
            typename DataVectorT::const_iterator it_data)
            : m_memspace_ptr(&memspace)
            , m_node_iterator(it_node)
            , m_node_begin(it_begin)
            , m_node_end(it_end)
            , m_it_data(data_buf, it_data)
        {
        }

        // Initialize positioned at first item of the node
        v_bindex_iterator(Memspace &memspace, const node_iterator &it_node, const node_iterator &it_begin,
            const node_iterator &it_end)
            : m_memspace_ptr(&memspace)
            , m_node_iterator(it_node)
            , m_node_begin(it_begin)
            , m_node_end(it_end)        
        {
            if (m_node_iterator != m_node_end) {
                m_it_data = beginNode(m_node_iterator, true);
            }
        }
        
        const item_t &operator*() const
        {
            assert(!this->isEnd());
            return *m_it_data;
        }

        /**
         * NOTICE : not allowed to modify key part of the item
         */
        item_t &modifyItem()
        {
            assert(!this->isEnd());            
            return m_it_data.modify();
        }

        v_bindex_iterator &operator++()
        {
            ++m_it_data;
            if (m_it_data.isEnd()) {
                ++m_node_iterator;
                if (m_node_iterator == m_node_end) {
                    m_it_data = {};
                } else {
                    m_it_data = beginNode(m_node_iterator, true);
                }
            }

            return *this;
        }

        v_bindex_iterator& operator--()
        {
            // position at the last item of the node
            if (m_node_iterator == m_node_end) {
                --m_node_iterator;
                m_it_data = beginNode(m_node_iterator, false);
                return *this;
            }

            --m_it_data;
            if (m_it_data.isEnd()) {
                if (m_node_iterator == m_node_begin) {
                    m_node_iterator = m_node_end;
                    m_it_data = {};
                } else {
                    --m_node_iterator;
                    m_it_data = beginNode(m_node_iterator, false);
                }
            }

            return *this;
        }
        
        // FIXME: would be nice to unify is_end / isEnd
        bool is_end() const {
            return m_node_iterator == m_node_end;            
        }

        bool isEnd() const {
            return m_node_iterator == m_node_end;
        }
        
        bool operator!=(const v_bindex_iterator &it) const {
            return (m_node_iterator != it.m_node_iterator || m_it_data != it.m_it_data);
        }

        bool operator==(const v_bindex_iterator &it) const {
            return (m_node_iterator == it.m_node_iterator && m_it_data == it.m_it_data);
        }

        /**
         * Pull (forward) block of data into provided output iterator (push back)
         * pull operation may be continued (until end reached)
         * NOTICE: must not call over end state iterator
         */
        template <typename push_back_iterator> void pullBlock(push_back_iterator out)
        {
            while (!m_it_data.isEnd()) {
                *out = *m_it_data;
                ++out;
                ++m_it_data;
            }

            ++m_node_iterator;
            if (m_node_iterator == m_node_end) {
                m_it_data = {};
            } else {
                m_it_data = beginNode(m_node_iterator, true);
            }
        }

        /**
         * Erase invalidates the iterator
         * @return true if the node after erase becomes empty
         */
        bool eraseCurrent()
        {
            assert(!this->isEnd());
            bool was_addr_changed = false;
            bool is_empty = this->m_it_data.erase(was_addr_changed);

            // check front item modified (key)
            if (!is_empty && m_comp(this->m_node_iterator->m_data.lo_bound, this->m_it_data.front())) {
                // this is safe as order not violated
                this->m_node_iterator.modify().m_data.lo_bound = this->m_it_data.front();
            }

            if (was_addr_changed) {
                this->m_node_iterator.modify().m_data.ptr_b_data = this->m_it_data.getAddress();
            }
            return is_empty;
        }

        const node_iterator &getNode() const {
            return this->m_node_iterator;
        }

        node_iterator &getMutableNode() {
            return this->m_node_iterator;
        }
        
        bool tryAppendBlock(DataVectorT &&data_block) {
            return m_it_data.tryAppendBlock(std::move(data_block));
        }

        // Compaction invalidates the iterator
        void compact()
        {
            if (this->m_it_data.compact()) {
                this->m_node_iterator.modify().m_data.ptr_b_data = this->m_it_data.getAddress();
            }
        }

    protected:
        Memspace *m_memspace_ptr = nullptr;
        node_iterator m_node_iterator;
        node_iterator m_node_begin;
        node_iterator m_node_end;
        item_comp_t m_comp;
        DestroyF m_destroy_func;

        struct DataIterator
        {
            DataVectorT m_data_buf;
            typename DataVectorT::const_iterator m_it_data = nullptr;
            
            DataIterator() = default;
            DataIterator(const DataVectorT &data_buf, typename DataVectorT::const_iterator it_data)
                : m_data_buf(data_buf)
                , m_it_data(it_data)
            {
            }

            inline void operator++() {
                ++m_it_data;
            }
        
            inline void operator--()
            {
                if (m_it_data == m_data_buf->begin()) {
                    m_it_data = m_data_buf->end();
                } else {
                    --m_it_data;
                }
            }

            bool isEnd() const {
                return m_it_data == m_data_buf->end();
            }

            inline const item_t &operator*() const
            {
                assert(m_it_data);
                return *m_it_data;
            }

            const item_t &front() const {
                return m_data_buf->front();
            }

            inline item_t &modify()
            {
                assert(m_it_data);
                auto index = m_data_buf->getItemIndex(m_it_data);
                m_data_buf.modify();
                m_it_data = m_data_buf->begin() + index;
                return const_cast<item_t&>(*m_it_data);
            }

            /**
             * Erase invalidates the iterator
             * @return true node after erase becomes empty
             */
            bool erase(bool &was_addr_changed)
            {
                m_data_buf.eraseItem(m_it_data, was_addr_changed);
                return m_data_buf.empty();
            }

            bool compact() {
                return m_data_buf.compact();
            }

            bool tryAppendBlock(DataVectorT &&data_block)
            {
                if ((m_data_buf->m_capacity - m_data_buf->m_size) >= data_block->m_size) {
                    m_data_buf.moveSorted(std::move(data_block));
                    return true;
                }
                return false;
            }        

            Address getAddress() const {
                return m_data_buf.getAddress();
            }

            inline bool operator==(const DataIterator &it) const {
                return (m_it_data == it.m_it_data);
            }

            inline bool operator!=(const DataIterator &it) const {
                return (m_it_data != it.m_it_data);
            }
        };

        /// Data persistency buffer
        mutable DataIterator m_it_data;

        // Begin iteration from node (asc or desc)
        DataIterator beginNode(const node_iterator &node, bool asc)
        {
            auto data_buf = DataVectorT(m_memspace_ptr->myPtr(node->m_data.ptr_b_data));
            auto it_data = asc ? data_buf->begin() : data_buf->end();
            if (!asc) {
                if (it_data == data_buf->begin()) {
                    it_data = data_buf->end();
                } else {
                    --it_data;
                }                
            }
            return DataIterator(data_buf, it_data);
        }

    };

}  // db0 namespace {
