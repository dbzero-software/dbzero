// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/utils/SortedArray.hpp>
#include <dbzero/core/utils/BoundCheck.hpp>
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{

	template <typename data_t, typename comp_t> class joinable_const_iterator
		: public SortedArray<data_t, comp_t>
    {
		using super_t = SortedArray<data_t, comp_t>;
	public :
		// raw iterators
		using iterator = data_t*;
		using const_iterator = const data_t*;

		/**
         * Construct as invalid
         */
		joinable_const_iterator()
			: super_t(static_cast<const data_t*>(nullptr), static_cast<const data_t*>(nullptr))			
			, m_bound_check(false)
		{
		}

		joinable_const_iterator(const data_t *begin, const data_t *end, const data_t *c, int direction)
			: super_t(begin, end)
			, m_current(c)
			, m_direction(direction)
			, m_bound_check(direction)
		{
			assert(direction != 0);
		}

		joinable_const_iterator(const joinable_const_iterator &other)
			: super_t(other.m_begin, other.m_end)
			, m_current(other.m_current)
			, m_direction(other.m_direction)
			, m_bound_check(other.m_bound_check)
		{
		}
		
		/**
         * Find exact key value within the current bound
         */
		template <class KeyT> bool joinKey(const KeyT &key) 
		{
			m_current = super_t::joinForward(m_current,key);
			if (m_current==this->m_end) {
				return false;
			} else {
				if (m_bound_check.hasBound() && !m_bound_check(key)) {
					set_end();
					return false;
				}
				// check equal
				return (!m_comp(*m_current,key) && !m_comp(key,*m_current));
			}
		}

		/**
         * Find first equal or greater value (move iterator)
         */
		template <class KeyT> bool join(const KeyT &key, int direction = -1)
		{
			if (direction > 0) {
				m_current = super_t::join(m_current, key, direction);
				if (m_current==this->m_end) {
					return false;
				}
				// check bounds, render iterator as end if out of bound
				if (!m_bound_check(*m_current)) {
					set_end();
					return false;
				}
				return true;
			} else {
				m_current = super_t::join((m_current + 1), key, direction);
				if (m_current==this->m_end) {
					return false;
				}
				// check bounds, render iterator as end if out of bound
				if (!m_bound_check(*m_current)) {
					set_end();
					return false;
				}
				return true;
			}
		}
		
		template <typename KeyT> std::pair<KeyT, bool> peek(const KeyT &key) const 
		{
			const data_t *ping_item = super_t::join((m_current + 1), key, -1);
			if (ping_item==this->m_end) {
				return std::make_pair<KeyT,bool>(KeyT(),false);
			} else {
				// check bounds
				if (!m_bound_check(*ping_item)) {
					return std::make_pair<KeyT,bool>(KeyT(),false);
				}
				return std::make_pair<KeyT,bool>((KeyT)(*ping_item),true);
			}
		}

		template <class KeyT> void joinBound(const KeyT &key) 
		{
			if (m_bound_check.hasBound()) {
				THROWF(db0::InternalException) << "Operation not permitted over bounded iterator";
			}
			const data_t *item = super_t::joinBound((m_current + 1),key);
			if (item!=(m_current + 1)) {
				m_current = item;
			}
		}

		const data_t &operator*() const {
			assert(m_current && m_current != this->m_end);
			return *m_current;
		}

		void operator++()
		{
			assert(m_current && m_current != this->m_end);
			++m_current;
			if (m_current!=this->m_end && !m_bound_check(*m_current)) {
				// render iterator as end
				set_end();
			}
		}

		void operator--()
		{
			if (m_current == this->m_begin) {
				// switch to end position
				m_current = this->m_end;
                return;
			}
            --m_current;
            if (!m_bound_check(*m_current)) {
                // render iterator as end
                set_end();
            }
		}

        /**
         * Move iterator by specific offset (forwards or backwards), index NOT validated
         * @param offset
         * @return
         */
        joinable_const_iterator &operator+=(int offset) 
		{
            if ((this->m_current - this->m_begin) + offset < 0) {
                // switch to end position
                m_current = this->m_end;
                return *this;
            }
            m_current += offset;
            if (m_current >= this->m_end || !m_bound_check(*m_current)) {
                // render iterator as end
                set_end();
            }
            return *this;
        }

		bool is_end() const {
			return (m_current==this->m_end);
		}

		bool isValid() const {
			return (m_current!=0 && m_current!=this->m_end);
		}

		/**
         * Invalidate
         */
		void reset() {
			m_current = 0;
		}
        
		void stop() {
		    m_current = this->m_end;
		}

		bool operator==(const_iterator it) const {
			return (m_current==it);
		}

		bool operator!=(const_iterator it) const {
			return (m_current!=it);
		}

		/**
         * @return current iterator position / collection size (exact)
         */
		std::pair<std::uint64_t, std::uint64_t> getPosition() const 
		{
			std::uint64_t total_size = this->size();
			std::uint64_t pos = m_current - this->m_begin;
			if (m_direction < 0) {
				pos = total_size - pos;
			}
			return std::make_pair(pos, total_size);
		}

		const data_t &operator*()
		{
			assert(m_current && m_current != this->m_end);
			return *m_current;
		}

		const data_t *operator->() 
		{
			assert(m_current && m_current != this->m_end);
			return m_current;
		}

		/**
         * @return native iterator
         */
		iterator getIterator() const 
		{
			assert(m_current && m_current != this->m_end);
			return const_cast<iterator>(m_current);
		}

		/**
         * @return native const iterator
         */
		const_iterator getConstIterator() const
		{
			assert(m_current && m_current != this->m_end);
			return m_current;
		}

		/**
         * Render iterator as end
         */
		void set_end() {
			m_current = this->m_end;
		}

		template <typename T> bool limitBy(const T &key)
		{
			m_bound_check.limitBy(key);
			if (isValid() && !is_end() && !m_bound_check(key)) {
				// render iterator as end
				set_end();
				return false;
			}
			return true;
		}

		bool hasLimit() const {
			return m_bound_check.hasBound();
		}

		const data_t &getLimit() const {
			return m_bound_check.getBound();
		}

		/**
		 * Get element index range ( current index / end index ) accounting for the direction for iteration
		 * NOTICE: end index will be -1 for the backward iterator
		 * @return current index / end index
		 */
		std::pair<int, int> getIndexRange() const
		{
            if (m_direction > 0) {
                auto end = this->m_end;
                if (m_bound_check.hasBound()) {
                    end = super_t::join(this->m_current, m_bound_check.getBound(), m_direction);
                }
                // calculate forward range
                return std::make_pair(this->m_current - this->m_begin, end - this->m_begin);
            } else {
                auto end = this->m_end;
                if (m_bound_check.hasBound()) {
                    end = super_t::join(this->m_current, m_bound_check.getBound(), m_direction);
                }
                int end_index = -1;
                if (end != this->m_end) {
                    end_index = end - this->m_begin;
                }
                // calculate backward range
                return std::make_pair(this->m_current - this->m_begin, end_index);
            }
		}
		
		bool isNextKeyDuplicated() const
		{
			assert(m_current && m_current != this->m_end);
			auto next = m_current;
			if (m_direction > 0) {
				++next;
				if (next == this->m_end) {
					return false;
				}
				return !this->m_comp(*m_current, *next) && !this->m_comp(*next, *m_current);
			} else {
				if (m_current == this->m_begin) {
					return false;
				}
				--next;
				return !this->m_comp(*m_current, *next) && !this->m_comp(*next, *m_current);
			}
		}
		
	private:
		const data_t *m_current = nullptr;
		int m_direction = -1;
		BoundCheck<data_t, comp_t> m_bound_check;
	};
	
} 
