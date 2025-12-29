// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cassert>
#include <cstddef>
#include <functional>

namespace db0

{
	
	/**
	 * This template class implements operations on a sorted array
	 * consisting of type T (comparable) elements
	 * @tparam element / key type
	 * @tparam Iterator random access iterator type
	 */
	template <class T, class comp_t = std::less<T>, typename Iterator = const T*> class SortedArray
	{
	public:
		using ConstIteratorT = Iterator;
		Iterator m_begin = nullptr;
		Iterator m_end = nullptr;
		comp_t m_comp;

		SortedArray() = default;

		SortedArray(Iterator begin, Iterator end)
			: m_begin(begin)
			, m_end(end)
		{
		}

		SortedArray(const std::pair<Iterator, Iterator> &data)
			: m_begin(data.first)
			, m_end(data.second)
		{
		}

		SortedArray(Iterator begin, std::size_t size)
			: m_begin(begin)
			, m_end(begin + size)
		{
		}

		/**
         * @return end if join not possible
		 * @param it must be either begin (when direction >0 ) or end
		 */         
		template <class KeyT> Iterator join(Iterator it, KeyT key, int direction = -1) const 
		{
			if (direction > 0) {
				assert (!(m_end < it));
				Iterator end = this->m_end;
				std::size_t diff = (size_t)(end - it);
				while (diff > 2) {
					Iterator c = it + (diff >> 1);
					// "c" meets the join criteria
					if (m_comp(key,*c)) {
						end = (c + 1);
					} else {
						// "c" doesn't meet the join criteria
						if (m_comp(*c,key)) {
							it = c + 1;
						}
						else {
							return c;
						}
					}
					diff = (std::size_t)(end - it);
				}
				while (diff-- > 0) {
					// "begin" meets the join criteria
					if (!m_comp(*it, key)) {
						return it;
					}
					++it;
				}
				// unable to join
				return this->m_end;
			} else {
				assert (!(it < m_begin));
				Iterator begin = this->m_begin;
				std::size_t diff = (std::size_t)(it - begin);
				while (diff > 2) {
					Iterator c = begin + (diff >> 1);
					// "c" meets the join criteria
					if (m_comp(*c, key)) {
						begin = c;
					} else {
						// "c" doesn't meet the join criteria
						if (m_comp(key, *c)) {
							it = c;
						} else {
							return c;
						}
					}
					diff = (std::size_t)(it - begin);
				}
				begin = it;
				while (diff-- > 0) {
					--begin;
					// "begin" meets the join criteria
					if (!m_comp(key, *begin)) {
						return begin;
					}
				}
				// unable to join
				return this->m_end;
			}
		}
		
		/**
         * NOTICE: join key value never exceeded (bound)
         * @return end if join not possible
         */
		template <class KeyT> Iterator joinBound(Iterator end, KeyT key) const 
		{
			assert (!(end < m_begin));
			Iterator begin = this->m_begin;
			std::size_t diff = (std::size_t)(end - begin);
			while (diff > 2) {
				Iterator c = begin + (diff >> 1);
				// "c" meets the join criteria
				if (m_comp(*c, key)) {
					begin = c;
				} else {
					// "c" + 1 doesn't meet the join criteria
					if (m_comp(key,*c)) {
						end = c;
						++end;
					} else {
						return c;
					}
				}
				diff = (std::size_t)(end - begin);
			}
			while (diff-- > 0) {
				// "begin" meets the join criteria
				if (!m_comp(*begin,key)) {
					return begin;
				}
				++begin;
			}
			// unable to join
			return this->m_end;
		}

		/**
         * Find first occurrence of "key"
         */
		template <class KeyT> Iterator find(KeyT key) const 
		{
			Iterator result = joinForward(m_begin,key);
			if (result == m_end) {
				return m_end;
			} else {
				// test equal
				if (m_comp(*result,key) || m_comp(key,*result)) {
					return m_end;
				} else {
					return result;
				}
			}
		}

		/**
         * @return intersection point from "this" or end if not intersecting collections
         */
		Iterator intersect(const SortedArray<T,comp_t> &other_array) const 
		{
			Iterator a_begin = this->m_begin;
			Iterator b_begin = other_array.m_begin;
			while ((a_begin!=this->m_end) && (b_begin!=other_array.m_end)) {
				a_begin = joinForward(a_begin,*b_begin);
				// test equal
				if (a_begin==this->m_end || !(m_comp(*a_begin,*b_begin) || m_comp(*b_begin,*a_begin))) {
					return a_begin;
				}
				b_begin = other_array.joinForward(b_begin,*a_begin);
				if (b_begin==other_array.m_end) {
					return this->m_end;
				}
				// test equal
				if (!(m_comp(*a_begin,*b_begin) || m_comp(*b_begin,*a_begin))) {
					return a_begin;
				}
			}
			return this->m_end;
		}

		void operator=(const std::pair<Iterator, Iterator> &buf) 
		{
			this->m_begin = buf.first;
			this->m_end = buf.second;
		}

		const T &front() const 
		{
			assert (m_begin!=m_end);
			return *m_begin;
		}

		const T &back() const 
		{
			assert (m_begin != m_end);
			return *(m_end - 1);
		}

		std::size_t size() const {
			return (m_end - m_begin);
		}

		inline ConstIteratorT begin() const {
			return m_begin;
		}

		inline ConstIteratorT end() const {
			return m_end;
		}
	};
	
} 
