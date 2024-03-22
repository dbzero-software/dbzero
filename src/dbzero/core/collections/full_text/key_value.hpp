#pragma once 

namespace db0

{

	template <class key_type, class value_type> class [[gnu::packed]] key_value 
    {
	public :
		key_type key;
		value_type value;

		key_value() = default;

		/**
		 * Initialize key, default value
		 */
		key_value(const key_type &key)
		    : key(key)
		{}

		key_value(const key_type &key, const value_type &value)
            : key(key)
            , value(value)
		{}

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
    
} 

