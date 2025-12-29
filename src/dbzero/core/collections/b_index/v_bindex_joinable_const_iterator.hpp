// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <functional>
#include "bindex_types.hpp"
#include <dbzero/core/utils/BoundCheck.hpp>

namespace db0

{
    
    template <typename item_t, typename AddrT, typename item_comp_t = std::less<item_t> >
    class v_bindex_joinable_const_iterator
    {
        using types_t = bindex_types<item_t, AddrT, item_comp_t>;
        using node_iterator = typename types_t::node_iterator;
        using bindex_tree_t = typename types_t::bindex_tree_t;
        using node_stack = typename types_t::node_stack;
        using data_vector = typename types_t::data_vector;
        using v_object_t = v_object<typename bindex_types<item_t, AddrT, item_comp_t>::bindex_container>;

    public:

        v_bindex_joinable_const_iterator(bindex_tree_t &index, std::uint32_t max_size, int direction)
            : m_index_ptr(&index)
            , m_max_size(max_size)
            , m_direction(direction)
            , m_bound_check(direction)
        {
            if (direction > 0) {
                m_node = index.begin();
                if (!index.empty()) {
                    // open bucket / bucket iterator at first item available
                    m_data_buf = data_vector(index.myPtr(m_node->m_data.ptr_b_data));
                    m_it_data = m_data_buf->beginJoin(direction);
                }
            } else {
                m_node = index.end();
                if (!index.empty()) {
                    --m_node;
                    // open bucket / bucket iterator at first item available
                    m_data_buf = data_vector(index.myPtr(m_node->m_data.ptr_b_data));
                    m_it_data = m_data_buf->beginJoin(direction);
                }
            }
        }

        /**
         * Initialize at specific b-node position
         */
        v_bindex_joinable_const_iterator(bindex_tree_t &index, uint32_t max_size, const node_iterator &it_node, int direction)
            : m_index_ptr(&index)
            , m_max_size(max_size)
            , m_direction(direction)
            , m_node(it_node)
            , m_bound_check(direction)
        {
            if (!index.empty()) {
                // open bucket / bucket iterator at first item available
                m_data_buf = data_vector(index.myPtr(it_node->m_data.ptr_b_data));
                m_it_data = m_data_buf->beginJoin(m_direction);
            }
        }
        
        v_bindex_joinable_const_iterator(const v_bindex_joinable_const_iterator &it)
            : m_index_ptr(it.m_index_ptr)
            , m_max_size(it.m_max_size)
            , m_direction(it.m_direction)
            , m_node(it.m_node)
            , m_bound_check(it.m_bound_check)
        {
            if (!it.is_end()) {
                if(!m_index_ptr->empty()) {
                    // open bucket / bucket iterator at first item available
                    m_data_buf = data_vector(m_index_ptr->myPtr(m_node->m_data.ptr_b_data));
                    m_it_data = m_data_buf->beginJoin(m_direction);
                    join(*it, m_direction);
                }
            } else {
                // render as end
                set_end();
            }
        }

        /**
         * Depending on current position either join backward or forward
         * @tparam KeyT
         * @param key
         * @return
         */
        template <class KeyT> bool join(const KeyT &key) 
        {
            if (is_end()) {
                return false;
            }
            if (m_comp(key, *m_it_data)) {
                return join(key, -1);
            }
            if (m_comp(*m_it_data, key)) {
                return join(key, 1);
            }
            // currently set at "key"
            return true;
        }

        template <typename KeyT> bool join(const KeyT &key, int direction) 
        {
            if (direction > 0) {
                // limit key pre-check
                if (m_bound_check.hasBound() && !m_bound_check(key)) {
                    // bound validation negative
                    set_end();
                    return false;
                }
                typename types_t::template cast_then_compare<KeyT> cast_comp;
                for (;;) {
                    // try join with current block first
                    if (m_it_data.isValid() && m_it_data.join(key, direction)) {
                        // limit check
                        if (m_bound_check.hasBound() && !m_bound_check(**this)) {
                            // bound validation negative
                            set_end();
                            return false;
                        }
                        return true;
                    }
                    if (m_index_ptr->join(m_stack, key, cast_comp, direction)) {
                        // must check with preceeding bucket unless equal match
                        node_iterator it = *m_stack;
                        if (m_comp(key, static_cast<KeyT>(it->m_data.lo_bound))) {
                            --it;
                            if (it==m_node) {
                                m_node = *m_stack;
                            } else {
                                m_node = it;
                            }
                        }
                        else {
                            m_node = it;
                        }
                    }
                        // must check with the last bucket
                    else {
                        node_iterator it = m_index_ptr->end();
                        --it;
                        if (it==m_node) {
                            // end of data reached, invalidate
                            m_it_data.reset();
                            return false;
                        }
                        else {
                            m_node = it;
                        }
                    }
                    // Open bucket / bucket iterator
                    m_data_buf = data_vector(m_index_ptr->myPtr(m_node->m_data.ptr_b_data));
                    m_it_data = m_data_buf->beginJoin(direction);
                }
            } else {
                // limit key pre-check
                if (m_bound_check.hasBound() && !m_bound_check(key)) {
                    // bound validation negative
                    set_end();
                    return false;
                }
                typename types_t::template cast_then_compare<KeyT> cast_comp;
                for (;;) {
                    // try join with current block first
                    if (m_it_data.isValid() && m_it_data.join(key, direction)) {
                        // limit check
                        if (m_bound_check.hasBound() && !m_bound_check(**this)) {
                            // bound validation negative
                            set_end();
                            return false;
                        }
                        return true;
                    }
                    if (m_index_ptr->join(m_stack, key, cast_comp, direction)) {
                        m_node = *m_stack;
                    }
                    else {
                        // end of data reached, invalidate
                        m_it_data.reset();
                        return false;
                    }
                    // open bucket / bucket iterator
                    m_data_buf = data_vector(m_index_ptr->myPtr(m_node->m_data.ptr_b_data));
                    m_it_data = m_data_buf->beginJoin(direction);
                }
            }
        }

        /**
         * join backward, not reaching end position
         */
        template <class KeyT> void joinBound(const KeyT &key)
        {
            // modify key to comply with existing limits
            if (m_bound_check.hasBound()) {
                THROWF(db0::InternalException) << "joinBound cannot be used with limited iterator, first call joinBound, set limits next";
            }
            typename types_t::template cast_then_compare<KeyT> cast_comp;
            // check with the index
            if (m_comp(key, static_cast<KeyT>(m_node->m_data.lo_bound)) && m_node != m_index_ptr->begin()) {
                m_index_ptr->joinBound(m_stack, key, cast_comp);
                if (m_node!=*m_stack) {
                    m_node = *m_stack;
                    m_data_buf = data_vector(m_index_ptr->myPtr(m_node->m_data.ptr_b_data));
                    m_it_data = m_data_buf->beginJoin(-1);
                }
                node_iterator it_prev = m_node;
                --it_prev;
                // check with the previous bucket
                data_vector buf = data_vector(m_index_ptr->myPtr(it_prev->m_data.ptr_b_data));
                if (!m_comp(static_cast<KeyT>(buf->back()), key)) {
                    m_node = it_prev;
                    m_data_buf = buf;
                    m_it_data = m_data_buf->beginJoin(-1);
                }
            }
            m_it_data.joinBound(key);
        }

        /**
         * Retrieve join backward result without actually performing it (not changing current position)
         */
        template <class KeyT> std::pair<KeyT, bool> peek(const KeyT &key) const 
        {
            std::pair<KeyT,bool> peek_res;
            // key limit pre-check
            if (m_bound_check.hasBound() && !m_bound_check(key)) {
                peek_res.second = false;
                return peek_res;
            }
            peek_res.second = false;
            auto peek_data_ptr = &m_it_data;
            std::unique_ptr<typename data_vector::joinable_const_iterator> peek_data;
            std::unique_ptr<node_stack> peek_stack;
            std::unique_ptr<data_vector> peek_buf;
            for (;;) {
                // try join with current block first
                if (peek_data_ptr->isValid()) {
                    peek_res = peek_data_ptr->peek(key);
                }
                if (peek_res.second) {
                    // limit check
                    if (m_bound_check.hasBound() && !m_bound_check(peek_res.first)) {
                        peek_res.second = false;
                    }
                    return peek_res;
                }
                // initialize ping stack
                if (!peek_stack.get()) {
                    peek_stack.reset(new node_stack(m_stack));
                }
                if (m_index_ptr->join(*peek_stack, key, -1)) {
                    // open ping bucket / bucket iterator
                    peek_buf.reset(new data_vector(m_index_ptr->myPtr((**peek_stack)->m_data.ptr_b_data)));
                    peek_data = std::make_unique<typename data_vector::joinable_const_iterator>((*peek_buf)->beginJoin(-1));
                } else {
                    // limit check
                    if (peek_res.second && m_bound_check.hasBound() && !m_bound_check(peek_res.first)) {
                        peek_res.second = false;
                    }
                    return peek_res;
                }
            }
        }
        
        /**
         * Apply limits (possibly with direction flag)
         * @return is iterator valid after applying limits
         */
        template <typename KeyT, typename comp_t> bool limitBy(const BoundCheck<KeyT, comp_t> &bounds) 
        {
            m_bound_check = bounds;
            if (!is_end()) {
                bool result = m_bound_check(**this);
                if (!result) {
                    set_end();
                }
                return result;
            }
            // is end anyways
            return false;
        }

        /**
         * @return is iterator valid after applying limits
         */
        template <class KeyT> bool limitBy(const KeyT &key) 
        {
            m_bound_check.limitBy(key);
            if (!is_end()) {
                bool result = m_bound_check(**this);
                if (!result) {
                    set_end();
                }
                return result;
            }
            // is end anyways
            return false;
        }

        const item_t &operator*() const {
            return *m_it_data;
        }

        /**
         * Check for iteration completed
         */
        bool is_end() const {
            return !m_it_data.isValid();
        }

        /**
         * Ends iteration
         */
        void stop() {
            m_it_data.reset();
        }

        /**
         * Step to next item forwards
         */
        void operator++()
        {
            ++m_it_data;
            if (m_it_data==m_data_buf->end()) {
                ++m_node;
                if (m_node==m_index_ptr->end()) {
                    // invalidate, set end
                    m_it_data.reset();
                    return;
                } else {
                    // open bucket / bucket iterator
                    m_data_buf = data_vector(m_index_ptr->myPtr(m_node->m_data.ptr_b_data));
                    m_it_data = m_data_buf->beginJoin(1);
                }
            }
            // limit check
            if (m_bound_check.hasBound() && !m_bound_check(**this)) {
                set_end();
            }
        }

        /**
         * Step to next item backwards
         */
        void operator--() 
        {
            if (m_it_data==m_data_buf->begin()) {
                if (m_node==m_index_ptr->begin()) {
                    // invalidate, set end
                    set_end();
                    return;
                } else {
                    --m_node;
                    // open bucket / bucket iterator
                    m_data_buf = data_vector(m_index_ptr->myPtr(m_node->m_data.ptr_b_data));
                    m_it_data = m_data_buf->beginJoin(-1);
                }
            }
            else {
                --m_it_data;
            }
            // limit check
            if (m_bound_check.hasBound() && !m_bound_check(**this)) {
                set_end();
            }
        }

        /**
         * @return min / max item from non empty collection
         */
        std::pair<item_t, item_t> getMinMax() const 
        {
            auto it = m_index_ptr->begin();
            assert(it!=m_index_ptr->end());
            data_vector first(v_object_t::myPtr(it->m_data.ptr_b_data));
            auto it_end = m_index_ptr->begin();
            --it_end;
            data_vector last(v_object_t::myPtr(it->m_data.ptr_b_data));
            return std::make_pair(first.front(), last.back());
        }

        /**
         * @return 0 based block position (block number) for valid iterator
         */
        std::uint64_t getBlockPosition() const
        {
            assert(!is_end());
            std::uint64_t block_num = 0;
            if (m_direction > 0) {
                auto it = m_index_ptr->begin();
                while (it!=m_node) {
                    ++block_num;
                    ++it;
                }
            } else {
                auto it = m_index_ptr->end();
                while (it!=m_node) {
                    --it;
                    ++block_num;
                }
                // this is to make this index 0-based
                --block_num;
            }
            return block_num;
        }

        bool hasLimit() const {
            return m_bound_check.hasBound();
        }

        const item_t& getLimit() const {
            return m_bound_check.getBound();
        }

        /**
         * Invalidate, render as "end"
         */
        void set_end() {
            m_it_data.reset();
        }

        Memspace &getMemspace() const {
            return m_index_ptr->getMemspace();
        }

        /**
         * Render this instance invalid relase all locked dbzero resources
         */
        void reset()
        {
            m_stack.clear();
            m_node.detach();
            m_data_buf.detach();
            m_it_data.reset();
        }
        
        bool isNextKeyDuplicated() const
        {
            assert(!is_end());
            auto it_data_next = m_it_data;
            bool has_next = false;
            if (m_direction > 0) {
                ++it_data_next;
                has_next = it_data_next != m_data_buf->end();
            } else {
                if (m_it_data != m_data_buf->begin()) {
                    --it_data_next;
                    has_next = true;
                }
            }

            if (!has_next) {
                auto it_node_next = m_node;
                if (m_direction > 0) {
                    ++it_node_next;
                    if (it_node_next == m_index_ptr->end()) {
                        return false;
                    }
                } else {
                    if (it_node_next == m_index_ptr->begin()) {
                        return false;
                    }
                    --it_node_next;
                }
                
                data_vector data_buf_next(m_index_ptr->myPtr(it_node_next->m_data.ptr_b_data));
                auto it_data_next = data_buf_next->beginJoin(m_direction);
                return !m_comp(*m_it_data, *it_data_next) && !m_comp(*it_data_next, *m_it_data);
            }

            return !m_comp(*m_it_data, *it_data_next) && !m_comp(*it_data_next, *m_it_data);
        }
        
    protected:
        friend class vso_b_index;
        item_comp_t m_comp;
        bindex_tree_t *m_index_ptr;
        std::uint32_t m_max_size;
        int m_direction;
        // stacked iterators
        node_stack m_stack;
        // iterator to data corresponding node
        node_iterator m_node;
        // data persistency buffer
        data_vector m_data_buf;
        typename data_vector::joinable_const_iterator m_it_data;
        BoundCheck<item_t, item_comp_t> m_bound_check;

        /**
         * Get position range relative to current data block : current position, end position coordinated with
         * the direction of iteration. NOTICE: end position will be -1 for backward iteration
         * @return current / end index within current data block
         */
        std::pair<int, int> getDataBlockIndexRange() const {
            assert (!is_end());
            return m_it_data.getIndexRange();
        }

        /**
         * Move within current block only, throws if iterated past end of block
         * @param diff
         * @return
         */
        v_bindex_joinable_const_iterator &operator+=(int diff) 
        {
            m_it_data += diff;
            if (m_it_data.is_end()) {
                THROWF(db0::InternalException) << "Internal error. Attempted iteration of bounds.";
            }
            // limit check
            if (m_bound_check.hasBound() && !m_bound_check(**this)) {
                set_end();
            }
            return *this;
        }

    };
    
}