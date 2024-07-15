#pragma once

#include <atomic>
#include "Flags.hpp"
	
namespace db0

{

    //
    // **********************************************************************************************
    // ROWO (read-only / write-only) mutex allows for single resource initialization operation only
    // it is guaranteed that "read-only" is always performed before full completion of "write-only"
    // use this utility class in the following schema :
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
				for (;;) {
					auto old_val = rowo_mutex.load();
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
					safeResetFlags(m_rowo_mutex, FLAG_LOCK);
			    }
			}
			
			/**
			 * Commit operation by setting the FLAG_SET flag
			 */
			void commit_set()
			{
				if (m_locked) {
					// content written, set flag (with CAS) and release lock
					for (;;) {
						auto old_val = m_rowo_mutex.load();
						if (m_rowo_mutex.compare_exchange_weak(old_val, ((old_val | FLAG_SET) & ~FLAG_LOCK))) {
							break;
						}
					}
					m_locked = false;
				}
			}

			/**
			 * Commit operation by resetting the FLAG_SET flag
			 */
			void commit_reset()
			{
				if (m_locked) {
					// content written, set flag (with CAS) and release lock
					for (;;) {
						auto old_val = m_rowo_mutex.load();
						if (m_rowo_mutex.compare_exchange_weak(old_val, old_val & ~(FLAG_SET | FLAG_LOCK))) {
							break;
						}
					}
					m_locked = false;
				}
			}
			
			// test for write-only access granted
			inline bool isLocked() const {
				return m_locked;
			}
		};

		/// Test for content being set / written
		/// @return true if read-only access granted (resource set)
		bool get() const
		{
			auto value = this->load();
			return !(value & FLAG_LOCK) && (value & FLAG_AVAILABLE);
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