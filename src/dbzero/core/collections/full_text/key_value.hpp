// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once 

#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{

DB0_PACKED_BEGIN
	template <class key_type, class value_type> class DB0_PACKED_ATTR key_value 
    {
	public :
		key_type key;
		value_type value = value_type();

		key_value() = default;

		/**
		 * Initialize key, default value
		 */
		key_value(const key_type &key)
		    : key(key)			
		{		
		}

		key_value(const key_type &key, const value_type &value)
            : key(key)
            , value(value)
		{		
		}

		/**
		 * Key cast operator
		 */
		operator key_type() const {
			return key;
		}

		bool operator<(const key_value &other) const {
			return key < other.key;
		}

		bool operator>(const key_value &other) const {
			return key > other.key;
		}

		bool operator==(const key_value &other) const {
			return key == other.key;
		}

		bool operator!=(const key_value &other) const {
			return key != other.key;
		}

		struct comparer {
			bool operator()(const key_type &k0, const key_type &k1) const {
				return k0 < k1;
			}
			bool operator()(const key_type &k0, const key_value<key_type, value_type> &k1) const {
				return k0 < k1.key;
			}
			bool operator()(const key_value<key_type,value_type> &k0, const key_type &k1) const {
				return k0.key < k1;
			}
			bool operator()(const key_value<key_type,value_type> &k0, const key_value<key_type, value_type> &k1) const {
				return k0.key < k1.key;
			}
		};
	};
DB0_PACKED_END
    
} 

