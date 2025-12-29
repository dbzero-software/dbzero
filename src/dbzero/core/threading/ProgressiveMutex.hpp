// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <condition_variable>
#include <mutex>
#include <atomic>
    
namespace db0

{

	class ProgressiveMutexDuration;	

	class progressive_mutex 
	{
	public :
		progressive_mutex() = default;

		struct scoped_lock 
		{			
			progressive_mutex &mx;
			std::uint32_t n_lock = 0;
			static std::uint32_t val_unique;
			static std::uint32_t val_zero;

			scoped_lock(progressive_mutex &mx)
				: mx(mx)				
			{
			}

			~scoped_lock();

			// test for unique lock owned
			bool isUniqueLocked() const {
				return (n_lock==0xffffffff);
			}

			// test for any lock owned
			bool isLocked() const {
				return n_lock;
			}

			// lock for read ( if called for the first time ) or upgrade lock to unique
			// NOTICE: temporarily unlocks to obtain the exclusive lock
			void lock();

			// try upgrade to unique lock without releasing the read lock ( if single reader )
			// @return false if failed to upgrade
			bool upgradeToUniqueLock();

			// downgrade lock
			void downgradeLock();

			// unlock all
			void release();

			void uniqueLock();
			bool uniqueLockWait(const ProgressiveMutexDuration &duration);

			void readLock();
			bool readLockWait(const ProgressiveMutexDuration &duration);
		};

		class scoped_read_lock : public scoped_lock
		{
		public :
			scoped_read_lock(progressive_mutex &mx)
				: scoped_lock(mx)
			{
				lock();
			}

			void unlock() {
				scoped_lock::release();
			}
		};

		class scoped_unique_lock : public scoped_lock
		{
		public :
			scoped_unique_lock(progressive_mutex &mx)
				: scoped_lock(mx)
			{
				scoped_lock::uniqueLock();
			}

			void lock() {
				scoped_lock::uniqueLock();
			}

			void unlock() {
				scoped_lock::release();
			}
		};

	protected:
		friend struct scoped_lock;
		std::atomic<std::uint32_t> m_num_readers = 0;
		// locked when the unique lock acquired
		std::timed_mutex mx_unique;
		std::mutex mx_event;
		std::condition_variable cv;
	};
    
} 
