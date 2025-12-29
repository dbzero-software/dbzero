// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "bindex_types.hpp"
#include "v_bindex_iterator.hpp"
#include "v_bindex_const_iterator.hpp"
#include "v_bindex_joinable_const_iterator.hpp"
#include "v_bindex_joinable_iterator.hpp"
#include <dbzero/core/serialization/Serializable.hpp>
#include <deque>

namespace db0

{

    template <typename item_t, typename AddrT = Address, typename item_comp_t = std::less<item_t> >
    class v_bindex: public v_object<typename bindex_types<item_t, AddrT, item_comp_t>::bindex_container>
    {
        using self_t = v_bindex<item_t, AddrT, item_comp_t>;
        using super_t = v_object<typename bindex_types<item_t, AddrT, item_comp_t>::bindex_container>;
        using types_t = bindex_types<item_t, AddrT, item_comp_t>;
        using node_iterator = typename types_t::node_iterator;
        using node_stack = typename types_t::node_stack;
        using data_vector = typename types_t::data_vector;
        using bindex_tree_t = typename types_t::bindex_tree_t;

        static std::size_t getPageSize(Memspace &memspace, std::optional<std::size_t> page_size_hint)
        {
            if (page_size_hint) {
                return *page_size_hint;
            }
            return memspace.getPageSize();
        }
        
    public :
        using ItemT = item_t;
        using addr_t = AddrT;
        using iterator = v_bindex_iterator<item_t, AddrT, item_comp_t>;
        using const_iterator = v_bindex_const_iterator<item_t, AddrT, item_comp_t>;
        using joinable_const_iterator = v_bindex_joinable_const_iterator<item_t, AddrT, item_comp_t>;
        using joinable_iterator = v_bindex_joinable_iterator<item_t, AddrT, item_comp_t>;
        using DestroyF = std::function<void(const item_t &)>;
        using CallbackT = std::function<void(item_t)>;

        /**
         * Construct null instance
         */
        v_bindex() = default;
                
        v_bindex(Memspace &memspace, std::optional<std::size_t> page_size_hint = {}, DestroyF item_destroy_func = {})
            : super_t(memspace)
            , m_index(memspace)
            , m_max_size(data_vector::getMaxSize(getPageSize(memspace, page_size_hint)))
            , m_item_destroy_func(item_destroy_func)
        {
            this->modify().ptr_index = m_index.getAddress();
        }
        
        /**
         * Note that this constructor has additional parameter "page_size_hint"
         * which is unusual for v_object descendants but is required to distinguish from the "data_vector" constructor
        */
        v_bindex(mptr ptr, std::optional<std::size_t> page_size_hint = {}, DestroyF item_destroy_func = {})
            : super_t(ptr)
            , m_index(this->myPtr((*this)->ptr_index))
            , m_max_size(data_vector::getMaxSize(getPageSize(ptr.m_memspace, page_size_hint)))
            , m_item_destroy_func(item_destroy_func)
        {
        }
        
        v_bindex(std::pair<Memspace*, AddrT> addr)
            : v_bindex(addr.first->myPtr(addr.second), addr.first->getPageSize())
        {            
        }

        /**
         * Create and populate with sorted data from buffer range begin / end
         */
        v_bindex(Memspace &memspace, const item_t *begin, const item_t *end, DestroyF item_destroy_func = {})
            : v_bindex(memspace, memspace.getPageSize(), item_destroy_func)
        {
            // we can push back since in is valid sorted collection
            bulkPushBack(begin, end);
        }

        /**
         * Create and populate with data from other sorted vector "in"
         * this constructor is required by the MorphingBIndex when the storage container type is changed
         */
        v_bindex(const data_vector &in, DestroyF item_destroy_func = {})
            : v_bindex(in.getMemspace(), in.begin(), in.end(), item_destroy_func)
        {
        }
        
        v_bindex(Memspace &memspace, const v_bindex &other)
            : super_t(memspace, *other.getData())
            , m_index(memspace, other.m_index)
            , m_max_size(other.m_max_size)
            , m_item_destroy_func(other.m_item_destroy_func)
        {
            this->modify().ptr_index = m_index.getAddress();
        }

        // static type ID requied for serialization
        static auto getSerialTypeId()
        {
            return db0::serial::typeId<self_t>(
                (db0::serial::typeId<item_t>() << 32) | (db0::serial::typeId<AddrT>() << 16) | 
                static_cast<std::uint16_t>(db0::serial::CollectionType::VBIndex)
            );
        }

        bindex::type getIndexType() const {
            return bindex::type::bindex;
        }

        static std::uint64_t createNew(Memspace &memspace) 
        {
            super_t b_index(memspace, 0, 0);
            bindex_tree_t index(memspace);
            b_index.modify().ptr_index = index.getAddress();
            return b_index.getAddress();
        }
        
        void destroy()
        {
            // must clear all nodes (item destroy)
            assert(!m_item_destroy_func && "Operation not implemented");
            // FIXME: functionality pending review
            /*
            auto &memspace = this->getMemspace();
            for (auto it = m_index.begin(), end = m_index.end(); it != end; ++it) {
                if (it->data.ptr_b_data) {
                    data_vector data_buf(memspace.myPtr(it->data.ptr_b_data), m_item_destroy_func);
                    data_buf.destroy();
                }                
            }
            */
            // now, safe to destroy index
            m_index.destroy();
            // destroy bindex_container
            super_t::destroy();
        }
        
        /**
         * Calculate total size of BN space occupied
         * @return size with additional information to help profile
         */
        BIndexStorageSize calculateStorageSize() const
        {
            // size of index
            BIndexStorageSize result;
            result.index_size = m_index->sizeOf();
            // iterate over index nodes
            auto begin_node = m_index.begin(), end_node = m_index.end();
            while (begin_node!=end_node) {
                // size of node
                ++result.node_count;
                result.node_size += begin_node->sizeOf();
                // size of data vector (under node)
                data_vector dv(v_bindex::myPtr(begin_node->m_data.ptr_b_data));
                result.data_size += dv->sizeOf();
                ++begin_node;
            }
            // size of end node (not included in node count)
            result.node_size += begin_node->sizeOf();
            return result;
        }

        /**
         * Number of blocks in this index (some proxy for size)
         */
        std::uint64_t getBlockCount() const {
            return m_index->size;
        }
        
        bool empty() const {
            return (m_index->size==0);
        }

        std::size_t size() const {
            return (*this)->size;
        }
        
        /**
         * insert only items not yet present in collection
         * @return items requested / items actually inserted
         */
        template <typename iterator_t> std::pair<std::uint32_t, std::uint32_t>
            bulkInsertUnique(iterator_t begin_item, iterator_t end_item, 
            CallbackT *callback_ptr = nullptr)
        {
            return bulkInsert(begin_item, end_item, true, false, callback_ptr);
        }

        /**
         * Either inserts new items or updates existing with specific "update" lambda function
         */
        template <typename iterator_t> std::pair<std::uint32_t, std::uint32_t> bulkUpdate(
            iterator_t begin_item, iterator_t end_item)            
        {
            return bulkInsert(begin_item, end_item, true, true);
        }

        /**
         * insert items (only unique if requested with flag)
         * @param callback - the function to receive new item notifications
         * @param update - if true then existing items will be updated (if not identical), may only be true when unique_only is true
         * @return items requested / items actually inserted
         */
        template <typename iterator_t> std::pair<std::uint32_t, std::uint32_t> bulkInsert(iterator_t begin_item,
            iterator_t end_item, bool unique_only = false, bool update = false,            
            CallbackT *callback_ptr = nullptr)
        {
            assert(!update || unique_only);
            std::pair<std::uint32_t, std::uint32_t> result(0, 0);
            std::uint32_t size_diff = 0;
            heap<item_t,item_comp_t> data_heap(16);
            while (begin_item != end_item) {
                data_heap.insert_grow(*begin_item);
                ++begin_item;
                ++result.first;
            }
            while (!data_heap.empty()) {
                insert_iterator insert_it(*this, data_heap.front());
                size_diff += insert_it.bulkInsert(data_heap, unique_only, update, callback_ptr);
            }
            if (size_diff != 0) {
                this->modify().size += size_diff;
            }
            result.second += size_diff;
            return result;
        }
        
        /**
         * insert sorted collection of items
         * NOTICE : all items must be greater or equal than currently contained
         */
        template <typename iterator_t> void bulkPushBack(iterator_t begin_item, iterator_t end_item)
        {
            if (begin_item == end_item) {
                return;
            }
            std::uint32_t total_diff = 0;
            // insert first data bucket
            node_iterator it_node;
            data_vector data_buf;
            if (m_index.empty()) {
                it_node = m_index.insert_equal(*begin_item);
                // create initial data block buffer
                data_buf = data_vector(this->getMemspace(), 8, sv_state::growing, m_item_destroy_func);
                it_node.modify().m_data.ptr_b_data = data_buf.getAddress();
            } else {
                it_node = m_index.end();
                --it_node;
                // open data bucket
                data_buf = data_vector(this->myPtr(it_node->m_data.ptr_b_data), m_item_destroy_func);
            }
            while (begin_item != end_item) {
                std::size_t diff = 0;
                int push_limit = (int)m_max_size - (int)data_buf->m_size;
                auto _end = begin_item;
                while ((push_limit-- > 0) && (_end!=end_item)) {
                    ++_end;
                    ++diff;
                }
                if (diff > 0) {
                    // grow data block
                    if (data_buf.bulkPushBack(begin_item, diff, m_max_size)) {
                        it_node.modify().m_data.ptr_b_data = data_buf.getAddress();
                    }
                    begin_item = _end;
                }
                // create next data block
                if (begin_item!=end_item) {
                    it_node = m_index.insert_equal(*begin_item);
                    // create initial data block buffer
                    data_buf = data_vector(this->getMemspace(), 8, sv_state::growing, m_item_destroy_func);
                    it_node.modify().m_data.ptr_b_data = data_buf.getAddress();
                }
                total_diff += diff;
            }
            if (total_diff != 0) {
                this->modify().size += total_diff;
            }
        }

        void insert(const item_t &item)
        {
            data_vector data_buf;
            node_iterator it_node = m_index.lower_equal_bound(item);
            if (it_node == m_index.end()) {
                // insert the first data bucket
                if (m_index->size == 0) {
                    it_node = m_index.insert_equal(item);
                    // create initial data block buffer (initial capacity =8)
                    data_buf = data_vector(
                        this->getMemspace(), 8, sv_state::growing, m_item_destroy_func
                    );
                    it_node.modify().m_data.ptr_b_data = data_buf.getAddress();
                } else {
                    // take first node, modify the bucket's key
                    it_node = m_index.begin();
                    it_node.modify().m_data.lo_bound = item;
                    // open data bucket
                    data_buf = data_vector(this->getMemspace().myPtr(it_node->m_data.ptr_b_data), m_item_destroy_func);
                }
            } else {
                // open data bucket
                data_buf = data_vector(this->getMemspace().myPtr(it_node->m_data.ptr_b_data), m_item_destroy_func);
            }
            
            // actual insert
            {
                bool addr_changed = false;
                data_buf.insert(item, addr_changed, this->m_max_size);
                if (addr_changed) {
                    it_node.modify().m_data.ptr_b_data = data_buf.getAddress();
                }
            }

            // try split data block if max_size reached
            if (data_buf->m_size >= this->m_max_size) {
                // choose split point
                auto it_split = data_buf->begin(data_buf->m_size >> 1);
                // if this is the last block then use different split strategy
                // (to limit fragmentation and space loss when adding growing only elements)
                if (isLastNode(it_node) && data_buf->m_size > 4) {
                    it_split = data_buf->begin(data_buf->m_size - 4);
                }
                auto it_next = it_split;
                ++it_next;
                while (it_next!=data_buf->end() && !m_comp(*it_split, *it_next)) {
                    ++it_split;
                    ++it_next;
                }
                if (it_next!=data_buf->end()) {
                    data_vector new_buf = data_buf.split(it_next);
                    node_iterator it_new = m_index.insert_equal(new_buf->front());
                    it_new.modify().m_data.ptr_b_data = new_buf.getAddress();
                }
            }
            ++(this->modify().size);
        }

        /**
         * NOTICE : all iterators get invalidated after insert / erase
         */
        template <class KeyT> iterator find(KeyT key)
        {
            node_iterator it_node = m_index.lower_equal_bound(key);
            if (it_node == m_index.end()) {
                // block not found
                return end();
            }
            data_vector data_buf(this->getMemspace().myPtr(it_node->m_data.ptr_b_data), m_item_destroy_func);
            auto it_data = data_buf->find(key);
            if (it_data) {
                return iterator(this->getMemspace(), it_node, m_index.begin(),
                    m_index.end(), data_buf, it_data);
            } else {
                return end();
            }
        }

        /**
         * Find lower equal bound of the key
         */
        template <class KeyT> iterator findLowerEqualBound(KeyT key) 
        {
            node_iterator it_node = m_index.lower_equal_bound(key);
            if (it_node == m_index.end()) {
                // block not found
                return end();
            }
            data_vector data_buf(this->getMemspace().myPtr(it_node->m_data.ptr_b_data), m_item_destroy_func);
            // find in this block
            auto it_data = data_buf->findLowerEqualBound(key);
            if (it_data) {
                return iterator(this->getMemspace(), it_node, m_index.begin(),
                    m_index.end(), data_buf, it_data);
            } else {
                return end();
            }
        }

        template <class KeyT> const_iterator findLowerEqualBound(KeyT key) const {
            return const_cast<v_bindex&>(*this).findLowerEqualBound(key);
        }

        /**
         * throwing version of "find"
         * NOTICE : all iterators get invalidated after insert / erase
         */
        template <class KeyT> iterator find_throw(KeyT key) 
        {
            node_iterator it_node = m_index.lower_equal_bound(key);
            if (it_node == m_index.end()) {
                // block / key not found
                THROWF(db0::InputException) << "key not found: " << key;
            }
            data_vector data_buf(this->getMemspace().myPtr(it_node->m_data.ptr_b_data), m_item_destroy_func);
            auto it_data = data_buf->find(key);
            if (it_data) {
                return iterator(this->getMemspace(), it_node, m_index.begin(),
                    m_index.end(), data_buf, it_data);
            } else {
                THROWF(db0::InputException) << "key not found: " << key;
            }
        }

        template <class KeyT> const_iterator find(KeyT key) const {
            return const_cast<v_bindex&>(*this).find(key);
        }

        template <class KeyT> const_iterator find_throw(KeyT key) const {
            return reinterpret_cast<v_bindex&>(*this).find_throw(key);
        }

        /**
         * Erase invalidates the iterator
        */
        void erase(iterator &it)
        {            
            // remove an empty data block
            if (it.eraseCurrent()) {
                m_index.erase(it.getMutableNode());
            } else {
                if (m_index->size == 1) {
                    // compact the last remaining block
                    it.compact();
                } else {
                    // try merging small blocks
                    auto it_next = it.getNode();
                    ++it_next;
                    if (it_next != m_index.end()) {
                        data_vector next_buf(this->getMemspace().myPtr(it_next->m_data.ptr_b_data), m_item_destroy_func);
                        if (it.tryAppendBlock(std::move(next_buf))) {
                            m_index.erase(it_next);
                        }
                    }
                }
            }

            --(this->modify().size);
        }

        iterator begin() {
            return iterator(this->getMemspace(), m_index.begin(), m_index.begin(), m_index.end());
        }

        const_iterator begin() const {
            auto &index = const_cast<bindex_tree_t&>(m_index);
            return const_iterator(this->getMemspace(), index.begin(), index.begin(), index.end());
        }
        
        iterator end() {
            return iterator(this->getMemspace(), m_index.end(), m_index.begin(), m_index.end());
        }

        const_iterator end() const 
        {
            auto &index = const_cast<bindex_tree_t&>(m_index);
            return const_iterator(
                this->getMemspace(), index.end(), index.begin(), index.end()
            );
        }

        joinable_const_iterator beginJoin(int direction) const
        {
            return joinable_const_iterator(
                const_cast<bindex_tree_t&>(m_index), m_max_size, direction
            );            
        }

        joinable_iterator beginJoin(int direction) {
            return joinable_iterator(m_index, m_max_size, direction);
        }

        /**
         * Debug & evaluation only member, throws
         */
        void validateContent()
        {
            bool is_first = true;
            item_t last_item;
            node_iterator it = m_index.begin();
            while (it!=m_index.end()) {
                if (!is_first && m_comp(it->m_data.lo_bound,last_item)) {
                    THROWF(db0::InternalException) << "key order violation";
                }
                last_item = it->data.lo_bound;
                is_first = false;
                data_vector data_buf(this->getMemspace().myPtr(it->m_data.ptr_b_data), m_item_destroy_func);
                if (m_comp(data_buf->front(), it->m_data.lo_bound)) {
                    THROWF(db0::InternalException) << "key order violation";
                }
                data_buf->validateContent();
                ++it;
            }
        }
        
        void clear() 
        {
            // destroy all blocks with items
            for (auto it = m_index.begin(), end = m_index.end(); it != end; ++it) {
                if (it->m_data.ptr_b_data.isValid()) {
                    data_vector data_buf(this->getMemspace().myPtr(it->m_data.ptr_b_data), m_item_destroy_func);
                    data_buf.destroy();
                    it.modify().m_data.ptr_b_data = {};
                }
            }
            // clear index next
            m_index.clear();
            this->modify().size = 0;
        }
        
        bool updateExisting(const item_t &item, item_t *old_item = nullptr)
        {
            node_iterator it_node = m_index.lower_equal_bound(item);
            if (it_node == m_index.end()) {
                // block not found
                return false;
            }
            data_vector data_buf(this->getMemspace().myPtr(it_node->m_data.ptr_b_data), m_item_destroy_func);
            auto it = data_buf.find(item);
            if (it == data_buf.end()) {
                // item not found
                return false;
            }
            
            if (old_item) {
                *old_item = *it;
            }
            auto index = data_buf->getItemIndex(it);
            data_buf.modify().modifyItem(index) = item;
            return true;
        }

        bool findOne(item_t &item) const
        {
            node_iterator it_node = m_index.lower_equal_bound(item);
            if (it_node == m_index.end()) {
                // block not found
                return false;
            }
            data_vector data_buf(this->getMemspace().myPtr(it_node->m_data.ptr_b_data), m_item_destroy_func);
            auto it = data_buf.find(item);
            if (it == data_buf.end()) {
                // item not found
                return false;
            }
            
            item = *it;
            return true;
        }
        
        void commit() const 
        {
            m_index.commit();
            super_t::commit();
        }
        
        void detach() const
        {
            m_index.detach();
            super_t::detach();
        }

    protected :
        bindex_tree_t m_index;
        // max number of items per single data block
        std::uint32_t m_max_size = 0;
        item_comp_t m_comp;
        DestroyF m_item_destroy_func;

        /**
         * @param it valid node iterator
         * @return true if it points to last node in index
         */
        bool isLastNode(node_iterator it) const
        {
            ++it;
            return (it==m_index.end());
        }

    private :

        class insert_iterator
        {
        public :
            insert_iterator(v_bindex &ref, const item_t &item)
                : m_ref(ref)
                , m_has_upper_bound(false)
            {
                // find bucket to place item
                m_it_node = ref.m_index.lower_equal_bound(item);
                if (m_it_node == ref.m_index.end()) {
                    // insert first data bucket
                    if (ref.m_index->size == 0) {
                        // create bucket
                        m_it_node = ref.m_index.insert_equal(item);
                        // create initial data block buffer                        
                        m_data_buf = data_vector(ref.getMemspace(), 8, sv_state::growing, ref.m_item_destroy_func);
                        m_it_node.modify().m_data.ptr_b_data = m_data_buf.getAddress();
                    } else {
                        m_it_node = ref.m_index.begin();
                        m_it_node.modify().m_data.lo_bound = item;
                    }
                }
                // check the upper bound
                node_iterator it_next = m_it_node;
                ++it_next;
                if (it_next != ref.m_index.end()) {
                    m_has_upper_bound = true;
                    m_upper_bound = it_next->m_data.lo_bound;
                }
                // open data bucket
                m_data_buf = data_vector(ref.getMemspace().myPtr(m_it_node->m_data.ptr_b_data), ref.m_item_destroy_func);
            }

            /**
             * insert unique items only
             */
            std::uint32_t bulkInsertUnique(heap<item_t, item_comp_t> &data) {
                return bulkInsert(data, true, nullptr);
            }

            /**
             * bulk insert into or update current bucket (data vector)
             * unique_only - if true, then only non duplicated items will be inserted
             * update - if true then existing items will be updated (if not identical), may only be true when unique_only is true
             * callback - the function to receive new item notifications
             * @return number of items inserted
             */
            std::uint32_t bulkInsert(heap<item_t, item_comp_t> &data, bool unique_only = false,
                bool update = false, CallbackT *callback_ptr = nullptr)
            {
                assert(!update || unique_only);
                std::uint32_t result = 0;
                bool force_insert = false;
                while (!data.empty()) {
                    std::deque<item_t> buf;
                    // select items that fit into the bucket
                    int push_limit = std::min((int)m_ref.m_max_size - (int)m_data_buf->m_size, (int)data.size());
                    // blocks are allowed to grow larger than max_size with forced insert
                    bool has_max_size = !force_insert;
                    while (!data.empty() && ((push_limit > 0) || force_insert) &&
                        (!m_has_upper_bound || m_ref.m_comp(data.front(), m_upper_bound)))
                    {
                        const item_t *item_ptr = nullptr;
                        if (!unique_only || (item_ptr = m_data_buf->find(data.front())) == nullptr) {
                            // existing item not found - just insert
                            buf.push_front(data.front());
                            if (callback_ptr) {
                                (*callback_ptr)(data.front());
                            }
                            if (unique_only) {
                                data.pop_front_all();
                            } else {
                                data.pop_front();
                            }
                            --push_limit;
                            ++result;
                            force_insert = false;
                        } else {
                            // update existing item (value part) if not identical as existing one
                            if (item_ptr && update && std::memcmp(item_ptr, &data.front(), sizeof(item_t)) != 0) {
                                auto at = m_data_buf->getItemIndex(item_ptr);
                                m_data_buf.modify().modifyItem(at) = data.front();
                            }
                            // duplicate item, remove all from insert heap
                            data.pop_front_all();
                        }
                    }
                    
                    if (buf.size() > 0) {
                        // grow data block
                        std::optional<std::uint32_t> max_size = has_max_size ? std::optional<std::uint32_t>(m_ref.m_max_size) : std::nullopt;
                        // blocks are allowed to grow larger than max_size with forced insert (i.e. when unable to split due to identical keys)
                        if (m_data_buf.bulkInsertReverseSorted(buf.begin(), buf.size(), max_size)) {
                            // update address (block size changed)
                            m_it_node.modify().m_data.ptr_b_data = m_data_buf.getAddress();
                        }
                    } else {
                        if (!data.empty()) {
                            // split data block to be let grow
                            if (m_data_buf->m_size >= m_ref.m_max_size) {
                                // choose split point
                                auto it_split = m_data_buf->begin(m_data_buf->m_size >> 1);
                                // if this is the last block then use different split strategy
                                // (to limit fragmentation and space loss when adding growing only elements)
                                if (isLastNode(m_it_node) && m_data_buf->m_size > 4) {
                                    it_split = m_data_buf->begin(m_data_buf->m_size - 4);
                                }
                                bool can_split = false;
                                if (it_split != m_data_buf->begin()) {
                                    auto it_prev = it_split;
                                    --it_prev;
                                    for (;;) {
                                        if (m_ref.m_comp(*it_prev, *it_split)) {
                                            can_split = true;
                                            break;
                                        }
                                        if (it_prev == m_data_buf->begin()) {
                                            break;
                                        }
                                        --it_prev;
                                        --it_split;
                                    }
                                }
                                
                                // cannot split a block with all identical keys
                                if (can_split) {
                                    data_vector new_buf = m_data_buf.split(it_split);
                                    node_iterator it_new = m_ref.m_index.insert_equal(new_buf->front());
                                    it_new.modify().m_data.ptr_b_data = new_buf.getAddress();
                                    // continue with either of the buckets
                                    if (m_ref.m_comp(data.front(), new_buf->front())) {
                                        // continue with old bucket & adjust the upper bound
                                        m_has_upper_bound = true;
                                        m_upper_bound = new_buf->front();
                                    } else {
                                        // continue with the new bucket
                                        m_it_node = it_new;
                                        m_data_buf = new_buf;
                                    }
                                    continue;
                                } else {
                                    // split failed, force insert & continue
                                    force_insert = true;
                                    continue;
                                }
                            }
                        }
                    }
                    break;
                }
                return result;
            }

        private :
            v_bindex &m_ref;
            node_iterator m_it_node;
            bool m_has_upper_bound;
            item_t m_upper_bound;
            data_vector m_data_buf;
            item_comp_t m_comp;

            /**
             * @param it must be valid node iterator (not end)
             * @return true if it points to last node in the data structure
             */
            bool isLastNode(node_iterator it) const 
            {
                ++it;
                return (it==m_ref.m_index.end());
            }
        };

        template <class KeyT> class erase_iterator
        {
        public:
            /**
             * @param ref - reference to the bindex
             * @param key - the first element to start erasing
             */
            erase_iterator(v_bindex &ref, std::optional<KeyT> key = {})
                : m_ref(ref)
                , m_has_upper_bound(false)
            {
                if (key) {
                    m_it_node = ref.m_index.lower_equal_bound(*key);
                } else {
                    m_it_node = ref.m_index.begin();
                }
                if (m_it_node == ref.m_index.end()) {
                    return;
                }
                // check the upper bound
                this->m_it_next = m_it_node;
                ++m_it_next;
                if (m_it_next != ref.m_index.end()) {
                    m_has_upper_bound = true;
                    m_upper_bound = m_it_next->m_data.lo_bound;
                }
                // open data bucket
                m_data_buf = data_vector(ref.getMemspace().myPtr(m_it_node->m_data.ptr_b_data), ref.m_item_destroy_func);
            }

            /**
             * bulk erase from current bucket
             * @callback_ptr - the function to receive erased item notifications
             * @return number of items erased
             */
            std::size_t bulkErase(heap<KeyT, item_comp_t> &data, CallbackT *callback_ptr = nullptr)
            {
                std::vector<KeyT> buf;
                int count = std::min((int)data.size(), (int)m_data_buf->m_size);
                while ((count-- > 0) && (!m_has_upper_bound || m_ref.m_comp(data.front(), m_upper_bound))) {
                    buf.push_back(data.front());
                    data.pop_front();
                }
                // never compact data block
                bool addr_changed = false;
                std::size_t erase_count = m_data_buf.bulkEraseSorted(buf.begin(), buf.end(), addr_changed, callback_ptr);
                if (addr_changed) {
                    m_it_node.modify().m_data.ptr_b_data = m_data_buf.getAddress();
                }
                
                if (m_data_buf.empty()) {
                    m_ref.m_index.erase(m_it_node);
                    if (m_ref.m_index->size == 1) {
                        m_it_node = m_ref.m_index.begin();
                        m_data_buf = data_vector(m_ref.getMemspace().myPtr(m_it_node->m_data.ptr_b_data), m_ref.m_item_destroy_func);
                    }
                } else {
                    // merge small blocks if possible
                    // check front item modified (key)
                    if (m_ref.m_comp(m_it_node->m_data.lo_bound, m_data_buf->front())) {
                        // this is safe as order not violated
                        m_it_node.modify().m_data.lo_bound = m_data_buf->front();
                    }
                    if (m_has_upper_bound) {
                        data_vector next_buf(m_ref.getMemspace().myPtr(m_it_next->m_data.ptr_b_data), m_ref.m_item_destroy_func);
                        if ((m_data_buf->m_capacity - m_data_buf->m_size) >= next_buf->m_size) {
                            m_data_buf.moveSorted(std::move(next_buf));
                            m_ref.m_index.erase(m_it_next);
                        }
                    }
                }

                // compact the last remaining block
                if (m_ref.m_index->size == 1) {
                    if (m_data_buf.compact()) {
                        m_it_node.modify().m_data.ptr_b_data = m_data_buf.getAddress();
                    }
                }

                return erase_count;
            }

            /**
             * Erase items matching specific condition
             * @param f - the function to test whether item should be erased
            */
            std::size_t bulkErase(std::function<bool(KeyT)> f, CallbackT *callback_ptr = nullptr)
            {
                // never compact data block
                bool addr_changed = false;
                std::size_t erase_count = m_data_buf.bulkErase(f, addr_changed, callback_ptr);
                if (addr_changed) {
                    m_it_node.modify().m_data.ptr_b_data = m_data_buf.getAddress();
                }

                // update bound item if erased
                if (!m_data_buf.empty() && m_ref.m_comp(m_it_node->m_data.lo_bound, m_data_buf->front())) {
                    // this is safe as order not violated
                    m_it_node.modify().m_data.lo_bound = m_data_buf->front();
                }
                
                // move on to the next node
                ++m_it_node;
                if (m_it_node != m_ref.m_index.end()) {
                    m_data_buf = data_vector(m_ref.getMemspace().myPtr(m_it_node->m_data.ptr_b_data), m_ref.m_item_destroy_func);                
                }
                return erase_count;
            }
            
            bool isEnd() const {
                return m_it_node == m_ref.m_index.end();
            }

        private :
            v_bindex &m_ref;
            node_iterator m_it_node;
            node_iterator m_it_next;
            bool m_has_upper_bound;
            item_t m_upper_bound;
            data_vector m_data_buf;
        };

        std::uint32_t getPageSize(std::optional<std::uint32_t> page_size, Memspace &memspace) {
            return page_size ? *page_size: memspace.getPageSize();
        }

        /**
         * Compact by removing empty nodes and merging the ones with low fill rate
        */
        void compactNodes()
        {
            // first pass is to detect and eliminate empty nodes
            for (auto it = m_index.begin(), end = m_index.end(); it != end;) {
                auto it_next = it;
                ++it_next;
                if (it->m_data.ptr_b_data.isValid()) {
                    data_vector data_buf(this->getMemspace().myPtr(it->m_data.ptr_b_data), m_item_destroy_func);
                    if (data_buf.empty()) {
                        m_index.erase(it);
                    }
                }
                it = it_next;
            }

            // second pass to merge small nodes
            auto it = m_index.begin();
            auto it_next = it;
            if (it_next != m_index.end()) {
                ++it_next;
            }

            if (it_next == m_index.end()) {
                return;
            }

            data_vector data_buf(this->myPtr(it->m_data.ptr_b_data), m_item_destroy_func);
            for (auto end = m_index.end(); it_next != end;) {
                data_vector next_buf(this->myPtr(it_next->m_data.ptr_b_data), m_item_destroy_func);
                // merge next buf
                if ((data_buf->m_capacity - data_buf->m_size) >= next_buf->m_size) {
                    data_buf.moveSorted(std::move(next_buf));
                    m_index.erase(it_next);
                    it_next = it;
                    ++it_next;
                } else {
                    ++it;
                    ++it_next;
                    data_buf = next_buf;
                }
            }
        }
        
    public :

        template <typename KeyT, class KeyIterator>
        std::size_t bulkErase(KeyIterator begin_key, KeyIterator end_key, const KeyT * = 0, 
            CallbackT *callback_ptr = nullptr)
        {            
            heap<KeyT,item_comp_t> key_heap(16);
            while (begin_key != end_key) {
                key_heap.insert_grow(*begin_key);
                ++begin_key;
            }
            std::size_t erase_count = 0;
            while (!key_heap.empty()) {
                erase_iterator<KeyT> erase_it(*this, key_heap.front());
                if (erase_it.isEnd()) {
                    key_heap.pop_front();
                } else {
                    erase_count += erase_it.bulkErase(key_heap, callback_ptr);
                }
            }
            if (erase_count != 0) {
                this->modify().size -= erase_count;
            }
            return erase_count;
        }
        
        // Erase with item selector function
        template <typename KeyT>
        std::size_t bulkErase(std::function<bool(KeyT)> f, CallbackT *callback_ptr = nullptr)
        {            
            std::size_t erase_count = 0;
            erase_iterator<KeyT> erase_it(*this);
            while (!erase_it.isEnd()) {
                erase_count += erase_it.bulkErase(f, callback_ptr);
            }
            if (erase_count != 0) {
                this->modify().size -= erase_count;
            }
            compactNodes();
            return erase_count;
        }
        
    };
    
}  
