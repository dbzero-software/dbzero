#pragma once

#include <unordered_map>
#include <deque>

namespace db0

{

    // auto_map is a simple collection with ordinal auto-generated values
    // it can be used for mapping long addresses into smaller ordinal values in-memory
    // the capacity of auto_map is determined by the capacity of the ValueT type
    template <typename KeyT, typename ValueT> class auto_map
    {
    public:
        // adds a new key if it is unique and return the auto-assigned ordinal value
        ValueT addUnique(KeyT key);
        // erase key if it exists
        void erase(KeyT);

    private:
        std::unordered_map<KeyT, ValueT> m_map;
        // reusable values
        std::deque<ValueT> m_unused;
        ValueT m_next_value;
    };
        
}