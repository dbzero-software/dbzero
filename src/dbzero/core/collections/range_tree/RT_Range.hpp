#pragma once

namespace db0

{

    template <typename KeyT> struct RT_Range
    {
        std::optional<KeyT> m_min;
        bool m_min_inclusive;
        std::optional<KeyT> m_max;
        bool m_max_inclusive;

        bool contains(KeyT key) const;
    };

    template <typename KeyT>
    bool RT_Range<KeyT>::contains(KeyT key) const
    {        
        if (m_min && (key < *m_min || (!m_min_inclusive && key == *m_min))) {
            return false;
        }
        if (m_max && (key > *m_max || (!m_max_inclusive && key == *m_max))) {
            return false;
        }        
        return true;
    }
    
}
