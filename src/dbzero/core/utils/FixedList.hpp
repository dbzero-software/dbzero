// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <vector>
#include <cassert>
#include <cstdint>

namespace db0

{

    /**
     * Fast linked list with limited maximum number of elements.
     * List allocates all necessary space to hold 'max_size' number of elements.
     * It may not be memory efficient solution, but it eliminates allocation and dealocation
     * costs on insertion and erasure.
     * This list implementation was designed to resemble std::list, thus all rules regarding
     * iterator invalidation etc. still holds.
     * Notice that this list is, in fact, cyclic. You can take advantage of this property
     * if you find it useful.
     */
    template<typename T>
    class FixedList {
        struct NodeBase {
            NodeBase *m_prev;
            NodeBase *m_next;
        };
        struct Node : public NodeBase {
            T m_value; /// Actual value stored in list
            
            template<typename... Args>
            Node(Args&&... args)
            : m_value(std::forward<Args>(args)...) {}
        };
        
        struct alignas(alignof(Node)) NodeMemory {
            char node_memory[sizeof(Node)];
            
            Node& node() {
                return *reinterpret_cast<Node*>(node_memory);
            }
        };
        
        // List root node. List front and back nodes are attached to it.
        // This node doesn't contain value. Its used as 'end' node in iteration.
        NodeBase m_root;
        std::size_t m_size; /// Number of elements in list
        
        std::vector<NodeMemory> m_node_buffer; /// Memory buffer in which list nodes are stored.
        NodeBase *m_next_node; /// Top of memory buffer stack
        
        template<typename... Args>
        Node* makeNode(Args&&... args) {
            // Create new node in place pointed by top of the stack.
            NodeBase *free_node = m_next_node;
            assert(free_node); // List have free node
            Node *node = new (free_node) Node(std::forward<Args>(args)...);
            m_next_node = m_next_node->m_next; // Prepare next free node
            ++m_size;
            return node;
        }
        
        void freeNode(Node *node) {
            node->~Node();
            node->m_next = m_next_node;
            m_next_node = node;
            --m_size;
        }
        
        /**
         * Links two nodes with each other
         */
        static void link(NodeBase *first, NodeBase *second) {
            first->m_next = second;
            second->m_prev = first;
        }
        
        /**
         * Links sequence of nodes together
         */
        static void link(NodeBase *first, NodeBase *middle, NodeBase *last) {
            link(first, middle);
            link(middle, last);
        }
        
        void initFixedList(std::size_t max_size) {
            m_root.m_prev = m_root.m_next = &m_root;
            m_size = 0;
            if(max_size > 0) {
                m_node_buffer.resize(max_size);
                for(std::size_t i = 0; i < max_size-1; ++i) {
                    m_node_buffer[i].node().m_next = &m_node_buffer[i+1].node();
                }
                m_node_buffer.back().node().m_next = nullptr;
                m_next_node = &m_node_buffer.front().node();
            }
        }
        
    public:
        /*
        * Constructs empty list without initialized storage space.
        * Use resize() before inserting any elements.
        */
        FixedList() {
            initFixedList(0);
        }
        
        /*
        * Constructs list with given maximum size
        * @param max_size Maximum number of elements that can be inserted into the list
        */
        FixedList(std::size_t max_size) {
            initFixedList(max_size);
        }
        
        FixedList(const FixedList &other) {
            initFixedList(other.max_size());
            insert(end(), other.begin(), other.end());
        }
        
        FixedList(FixedList &&other)
        : m_size(other.m_size),
        m_node_buffer(std::move(other.m_node_buffer)),
        m_next_node(other.m_next_node)
        {
            if(!other.empty()) {
                // Link data nodes from other list to root node in this list  
                link(other.m_root.m_prev, &m_root, other.m_root.m_next);
                // Reset other list
                other.initFixedList(0);
            } else {
                // Nothing to move. Just initialize root.
                initFixedList(0);
            }
        }
        
        FixedList& operator=(const FixedList &other) {
            if(this != &other) {
                clear();
                initFixedList(other.max_size());
                insert(end(), other.begin(), other.end());
            }
            return *this;
        }
        
        FixedList& operator=(FixedList &&other) {
            if(this != &other) {
                clear();
                m_size = other.m_size;
                m_node_buffer = std::move(other.m_node_buffer);
                m_next_node = other.m_next_node;
                if(!other.empty()) {
                    link(other.m_root.m_prev, &m_root, other.m_root.m_next);
                    other.initFixedList(0);
                } else {
                    initFixedList(0);
                }
            }
            return *this;
        }
        
        ~FixedList() {
            clear();
        }
        
    private:
        template<typename IteratorConfig>
        class iterator_impl {
            friend FixedList;
        protected:
            using IteratorType = typename IteratorConfig::iterator_type;
            using difference_type = std::ptrdiff_t;
            using iterator_category = std::bidirectional_iterator_tag;
            using value_type = T;
            using pointer = typename IteratorConfig::pointer;
            using reference = typename IteratorConfig::reference;

            NodeBase *m_node;
            
            iterator_impl()
            : m_node(nullptr) {}
            
            iterator_impl(NodeBase *node)
            : m_node(node) {}
            
            template<typename OtherIteratorConfig>
            iterator_impl(const iterator_impl<OtherIteratorConfig> &other)
            : m_node(other.m_node) {}
            
        public:
            reference operator*() const {
                assert(m_node != nullptr);
                return static_cast<Node*>(m_node)->m_value;
            }
            
            pointer operator->() const {
                return &*(*this);
            }

            pointer get() const {
                return &*(*this);
            }

            IteratorType& operator++() {
                assert(m_node != nullptr);
                m_node = m_node->m_next;
                return *static_cast<IteratorType*>(this);
            }
            
            IteratorType operator++(int) {
                IteratorType it(*static_cast<IteratorType*>(this));
                ++(*this);
                return it;
            }
            
            IteratorType& operator--() {
                assert(m_node != nullptr);
                m_node = m_node->m_prev;
                return *static_cast<IteratorType*>(this);
            }
            
            IteratorType operator--(int) {
                IteratorType it(*static_cast<IteratorType*>(this));
                --(*this);
                return it;
            }
            
            bool operator==(const IteratorType &other) {
                return m_node == other.m_node;
            }
            
            bool operator!=(const IteratorType &other) {
                return m_node != other.m_node;
            }
        };

        /**
         * Both iterator and const_interator shares entirety of implementation.
         * They only differs in type returned on dereferencing. To avoid code duplication,
         * these are iterator configuration structures, containing information about
         * returned types and actual iterator type.
         */

    public:
        class iterator;
        class const_iterator;

    private:
        struct iterator_config {
            using iterator_type = FixedList<T>::iterator;
            using pointer = T*;
            using reference = T&;
        };
        struct const_iterator_config {
            using iterator_type = FixedList<T>::const_iterator;
            using pointer = const T*;
            using reference = const T&;
        };
        
    public:
        class iterator final : public iterator_impl<iterator_config> {
            friend FixedList;
            using super_t = iterator_impl<iterator_config>;
        public:
            using difference_type = typename super_t::difference_type;
            using iterator_category = typename super_t::iterator_category;
            using value_type = typename super_t::value_type;
            using pointer = typename super_t::pointer;
            using reference = typename super_t::reference;
            
            iterator() = default;
            iterator(NodeBase *node)
            : super_t(node) {}
        };
        
        class const_iterator final : public iterator_impl<const_iterator_config> {
            friend FixedList;
            using super_t = iterator_impl<const_iterator_config>;
        public:
            using difference_type = typename super_t::difference_type;
            using iterator_category = typename super_t::iterator_category;
            using value_type = typename super_t::value_type;
            using pointer = typename super_t::pointer;
            using reference = typename super_t::reference;
            
            const_iterator() = default;
            // We have to cast off constness from node to be able to use single,
            // consistent iterator implementation
            const_iterator(const NodeBase *node)
            : super_t(const_cast<NodeBase*>(node)) {}
            const_iterator(const iterator &it)
            : super_t(it) {}
        };
        
        using reverse_iterator = std::reverse_iterator<iterator>;
        using const_reverse_iterator = std::reverse_iterator<const_iterator>;
        
        const_iterator cbegin() const {
            return const_iterator(m_root.m_next);
        }
        
        const_iterator cend() const {
            return const_iterator(&m_root);
        }
        
        iterator begin() {
            return iterator(m_root.m_next);
        }
        
        const_iterator begin() const {
            return cbegin();
        }
        
        iterator end() {
            return iterator(&m_root);
        }
        
        const_iterator end() const {
            return cend();
        }
        
        const_reverse_iterator crbegin() const {
            return const_reverse_iterator(end());
        }
        
        const_reverse_iterator crend() const {
            return const_reverse_iterator(begin());
        }
        
        reverse_iterator rbegin() {
            return reverse_iterator(end());
        }
        
        const_reverse_iterator rbegin() const {
            return crbegin();
        }
        
        reverse_iterator rend() {
            return reverse_iterator(begin());
        }
        
        const_reverse_iterator rend() const {
            return crend();
        }
        
        /**
         * @return Maximum capacity of the list.
         */
        std::size_t max_size() const {
            return m_node_buffer.size();
        }
        
        /**
         * @return Number of elements in the list
         */
        std::size_t size() const {
            return m_size;
        }
        
        bool empty() const {
            return begin() == end();
        }
        
        template<typename... Args>
        iterator emplace(const_iterator pos, Args&&... args) {
            Node *node = makeNode(std::forward<Args>(args)...);
            link(pos.m_node->m_prev, node, pos.m_node);
            return iterator(node);
        }
        
        iterator insert(const_iterator pos, const T &value) {
            return emplace(pos, value);
        }
        
        iterator insert(const_iterator pos, T &&value) {
            return emplace(pos, std::move(value));
        }
        
        template<typename InputIt>
        iterator insert(const_iterator pos, InputIt first, InputIt last) {
            NodeBase *before_pos_node = pos.m_node->m_prev;
            NodeBase *prev_node = before_pos_node;
            for(; first != last; ++first) {
                Node *node = makeNode(*first);
                link(prev_node, node);
                prev_node = node;
            }
            link(prev_node, pos.m_node);
            return iterator(before_pos_node->m_next);
        }
        
        template<typename V>
        iterator insert(const_iterator pos, std::initializer_list<V> values) {
            return insert(pos, values.begin(), values.end());
        }
        
        iterator erase(const_iterator pos) {
            iterator it(pos.m_node->m_next);
            link(pos.m_node->m_prev, pos.m_node->m_next);
            freeNode(static_cast<Node*>(pos.m_node));
            return it;
        }
        
        iterator erase(const_iterator first, const_iterator last) {
            link(first.m_node->m_prev, last.m_node);
            while(first != last) {
                Node* node_to_free = static_cast<Node*>(first.m_node);
                ++first;
                freeNode(node_to_free);
            }
            return iterator(last.m_node);
        }

        /**
         * Changes position of element pointed by iterator.
         * No itrators or references are invalidated in the process.
         * @param pos Iterator to element before which, shifted element will be placed.
         * @param it Iterator to shifted element.
         */
        void splice(const_iterator pos, const_iterator it) {
            if(pos != it) {
                // Detach node from its current place
                link(it.m_node->m_prev, it.m_node->m_next);
                // Put it in new place
                link(pos.m_node->m_prev, it.m_node, pos.m_node);
            }
        }
        
        void clear() {
            erase(begin(), end());
        }
        
        template<typename... Args>
        T& emplace_front(Args&&... args) {
            return *emplace(begin(), std::forward<Args>(args)...);
        }
        
        template<typename... Args>
        T& emplace_back(Args&&... args) {
            return *emplace(end(), std::forward<Args>(args)...);
        }
        
        T& push_front(const T &value) {
            return emplace_front(value);
        }
        
        T& push_front(T &&value) {
            return emplace_front(std::move(value));
        }
        
        T& push_back(const T &value) {
            return emplace_back(value);
        }
        
        T& push_back(T &&value) {
            return emplace_back(std::move(value));
        }
        
        void pop_front() {
            erase(begin());
        }
        
        void pop_back() {
            erase(--end());
        }
        
        T& front() {
            return *begin();
        }
        
        const T& front() const {
            return *begin();
        }
        
        T& back() {
            return *(--end());
        }
        
        const T& back() const {
            return *(--end());
        }
        
        /**
         * Changes maximum size of the list.
         * @param new_max_size New maximum size of the list
         * @note This operation has cost O(n).
         */
        void resize(std::size_t new_max_size) {
            std::size_t element_count = size();
            // Get iterator to list in its current state
            iterator first = begin(), last = end();
            if(new_max_size < element_count) {
                // When new list size is smaller than current number of elements in list,
                // skip 'n' last elements in list.
                std::advance(last, -(std::int64_t)(element_count - new_max_size));
            }
            // Hold node buffer in temporary vector. Iterators stay valid becouse they
            // still points to memory in this vector.
            std::vector<NodeMemory> tmp_node_buffer = std::move(m_node_buffer);
            // List is now empty. Initialize in with new size.
            initFixedList(new_max_size);
            // Insert previously contained elements to the list.
            #if _WIN32
                insert(end(), first, last);
            #else
                insert(end(), std::make_move_iterator(first), std::make_move_iterator(last));
            #endif
        }
    };

}
