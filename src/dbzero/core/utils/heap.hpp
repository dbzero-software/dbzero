// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <set>
#include <vector>
#include <cassert>
#include <cstdlib>

namespace db0 

{

    /**
     * KeyT - key / value type
     * comp_t - key comparer
     */
	template <class KeyT,class comp_t = std::less<KeyT> > class heap
    {
	public :
		using self = heap<KeyT,comp_t>;

		heap(int size)
			: m_size(0)
		{
			init(size);
		}

		heap(int size,const comp_t &_comparer)
            : m_size(0)
            , m_comparer(_comparer)
		{
			init(size);
		}

		void init(int size) {
			m_heap.resize(size);
		}

		void clear() {
			this->m_size = 0;
		}

		bool empty() const {
			return (m_size==0);
		}

		void insert(const KeyT &_key) {
			assert (m_size < m_heap.size());
			m_heap[m_size] = _key;
			++m_size;
			fixheap(m_size - 1);
		}

		/**
         * insert, grow heap if necessary
         */
		void insert_grow(const KeyT &_key) {
			if (m_size==m_heap.size()) {
				m_heap.resize(std::max((uint32_t)2, (uint32_t)m_heap.size() << 1));
			}
			assert (m_size < m_heap.size());
			m_heap[m_size] = _key;
			++m_size;
			fixheap(m_size - 1);
		}

		/**
         * NOTE : remember to call "downfix" after front element modified
         */
		inline KeyT &front() {
			assert (m_size > 0);
			return m_heap[0];
		}

		inline const KeyT &front() const {
			assert (m_size > 0);
			return m_heap[0];
		}

		void replace_front(const KeyT &new_value) {
			m_heap[0] = new_value;
			this->downfix ();
		}

		void pop_front() {
			assert (m_size > 0);
			if ((--m_size) > 0) {
				m_heap[0] = m_heap[m_size];
				this->downfix();
			}
		}

		/**
         * pop front all equal values
         */
		void pop_front_all() {
			KeyT key_first = front();
			pop_front ();
			while (!empty() && (!m_comparer(key_first,front()) && !m_comparer(front(),key_first))) {
				pop_front();
			}
		}

		inline size_t size() const {
			return m_size;
		}

		/**
         * Fix heap after front item has changed
         */
		void downfix() {
			downfix(0);
		}

		// heap iterator for direct element access
		using iterator = typename std::vector<KeyT>::iterator;
		using const_iterator = typename std::vector<KeyT>::const_iterator;

		inline iterator begin() {
			return m_heap.begin();
		}

		inline const_iterator begin() const {
			return m_heap.begin();
		}

		inline iterator end() {
			return m_heap.begin() + m_size;
		}

		inline const_iterator end() const {
			return m_heap.begin() + m_size;
		}

		/**
         * operation does not affect preceeding iterator(s)
         */
		iterator erase(iterator it) {
			assert (!empty());
			assert (m_size > 0);
			size_t pos = it - begin();
			// last element erased
			if (pos==(m_size - 1)) {
				--m_size;
				return end();
			} else {
				*it = m_heap[m_size - 1];
				--m_size;
				downfix (pos);
				return it;
			}
		}

		/**
         * Fix all heap
         */
		void fix() {
			for (size_t i=0;i < m_size;++i) {
				fixheap(i);
			}
		}

		class pop_front_iterator {
		public :
			pop_front_iterator (heap<KeyT,comp_t> &ref)
					: ref(ref)
			{
			}

			const KeyT &operator*() const {
				return ref.front();
			}

			void operator++() {
				ref.pop_front();
			}

			bool is_end () const {
				return ref.empty();
			}

		private :
			heap<KeyT,comp_t> &ref;
		};

		pop_front_iterator beginPopFront () {
			return pop_front_iterator(*this);
		}

		/// check for duplicate top element instances (in terms of equality operator) existing on heap
		bool hasDuplicatesForTopElement () const {
			// it's sufficient to compare 0 -> 1 and 0 -> 2
			if (m_size > 1 && m_heap[0]==m_heap[1]) {
				return true;
			}
			if (m_size > 2 && m_heap[0]==m_heap[2]) {
				return true;
			}
			return false;
		}
		
		// Check if identical element as the front is present in the heap
		bool isFrontElementDuplicated () const
		{			
			if (m_size > 1 && m_heap[0]==m_heap[1]) {
				return true;
			}
			if (m_size > 2 && m_heap[0]==m_heap[2]) {
				return true;
			}			
			return false;
		}
		
	protected :
		/// actual size (number of items)
		std::size_t m_size;
		/// actual item storage
		std::vector<KeyT> m_heap;
		comp_t m_comparer;

		void fixheap(std::size_t pos) {
			std::size_t parent = 0;
			KeyT temp_K;
			while (pos > 0) {
				parent = (pos - 1) >> 1;
				// switch with parent
				if (m_comparer(m_heap[pos], m_heap[parent])) {
					temp_K = m_heap[parent];
					m_heap[parent] = m_heap[pos];
					m_heap[pos] = temp_K;
					pos = parent;
				} else {
					break;
				}
			}
		}

		void downfix(std::size_t pos) {
			if (m_size <= 1) {
				return;
			}
			std::size_t r_child = 2;
			std::size_t l_child = 1;
			std::size_t skip_child = 0;
			KeyT temp_K;
			do {
				if (r_child>=m_size) {
					r_child = l_child;
				}
				if (m_comparer(m_heap[l_child],m_heap[r_child])) {
					skip_child = l_child;
				} else {
					skip_child = r_child;
				}
				if (m_comparer(m_heap[skip_child],m_heap[pos])) {
					temp_K = m_heap[pos];
					m_heap[pos] = m_heap[skip_child];
					m_heap[skip_child] = temp_K;
					pos = skip_child;
				} else {
					break;
				}
				r_child = (pos + 1) << 1;
				l_child = r_child - 1;
			}
			while (l_child < m_size);
		}
	};

} 
