// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <atomic>
#include "Flags.hpp"
	
namespace db0

{

    //
    // **********************************************************************************************
    // ROWO (read-only / write-only) mutex allows for single resource initialization operation only
    // it is guaranteed that "read-only" is always performed before full completion of "write-only"
    // use this utility class in the following schema:
    //
    //		ROWO_Mutex<> mutex;
    //		...
    //		while (!mutex.get()) {
    //			ROWO_Mutex::WriteOnlyLock lock(mutex);
    //			if (lock.isLocked()) {
    //				SAFE TO WRITE-ONLY NOW
    //              safe to trow exceptions
    //              lock.commit();
    //			}
    //		}
    //		SAFE TO READ-ONLY NOW
    // ***********************************************************************************************
    //

	template <class ValueT, ValueT FLAG_AVAILABLE, ValueT FLAG_SET, ValueT FLAG_LOCK>
	class ROWO_Mutex: public std::atomic<ValueT>
	{
	public :
		ROWO_Mutex() = default;

		static void __new(std::atomic<ValueT> &store)
		{
			// clear resource / lock flags
			if (store.load() & (FLAG_AVAILABLE | FLAG_SET | FLAG_LOCK)) {
				store = store.load() & ~(FLAG_AVAILABLE | FLAG_SET | FLAG_LOCK);
			}
		}

		static ROWO_Mutex &__ref(std::atomic<ValueT> &store) {
			return (ROWO_Mutex&)store;
		}

		struct WriteOnlyLock
		{
			ROWO_Mutex &m_rowo_mutex;
			bool m_locked;

			WriteOnlyLock(ROWO_Mutex &rowo_mutex)
				: m_rowo_mutex(rowo_mutex)
			{
				auto old_val = rowo_mutex.load();
				for (;;) {
					// some thread already gained write-only access
					if (old_val & FLAG_LOCK) {
						m_locked = false;
						break;
					}
					if (rowo_mutex.compare_exchange_weak(old_val, (old_val | FLAG_LOCK))) {
						// lock bit set / resource access granted
						m_locked = true;
						break;
					}
				}
			}
			
			WriteOnlyLock(std::atomic<ValueT> &store)
				: WriteOnlyLock(ROWO_Mutex::__ref(store))
			{
			}

			~WriteOnlyLock()
			{
			    if (m_locked) {
					assert(m_rowo_mutex.load() & FLAG_LOCK);
					atomicResetFlags(m_rowo_mutex, FLAG_LOCK);
			    }
			}
			
			/**
			 * Commit operation by setting the FLAG_SET flag (must be locked)
			 */
			void commit_set()
			{
				assert(m_locked);
				// content written, set flag (with CAS) and release lock
				auto old_val = m_rowo_mutex.load();
				for (;;) {
					assert(old_val & FLAG_LOCK);
					if (m_rowo_mutex.compare_exchange_weak(old_val, ((old_val | FLAG_SET) & ~FLAG_LOCK))) {
						m_locked = false;
						break;
					}
				}				
			}

			/**
			 * Commit operation by resetting the FLAG_SET flag (must be locked)
			 */
			void commit_reset()
			{
				assert(m_locked);				
				// content written, set flag (with CAS) and release lock
				auto old_val = m_rowo_mutex.load();
				for (;;) {
					assert(old_val & FLAG_LOCK);
					if (m_rowo_mutex.compare_exchange_weak(old_val, old_val & ~(FLAG_SET | FLAG_LOCK))) {
						m_locked = false;
						break;
					}
				}
			}
			
			// test for write-only resource access granted
			inline bool isLocked() const {
				return m_locked;
			}
		};
		
		/// Test for content being set / written
		/// @return true if read-only access granted (resource set)
		inline bool get() const {
			return this->load() & FLAG_AVAILABLE;
		}
		
        /// Clear all resource flags
        void reset()
		{
            if (this->load() & (FLAG_AVAILABLE | FLAG_SET | FLAG_LOCK)) {
                *this = this->load() & ~(FLAG_AVAILABLE | FLAG_SET | FLAG_LOCK);
            }
        }

	protected :		
		friend struct WriteOnlyLock;
	};

}