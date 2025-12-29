// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <unordered_set>

namespace db0 

{

	template <class T> class unique_set: public std::unordered_set<T>
	{
	public :
		/**
		 * Verify that item has not been seen before
		 */
		bool insertUnique(const T &item)
		{
			auto it = this->find(item);
			if (it==this->end())
			{
				this->insert(item);
				return true;
			}
			// not unique
			return false;
		}
	};
    
}
