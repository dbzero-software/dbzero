// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "XValuesVector.hpp"

namespace db0::object_model

{       

    XValuesVector::XValuesVector(unsigned int sort_threshold)
        : m_sort_threshold(sort_threshold)
    {    
        m_masks.reserve(128);
        this->reserve(128);
    }

    void XValuesVector::push_back(const XValue &xvalue, std::uint64_t mask)
    {    
        // assure mask provided only for lo-fi types
        assert(mask == 0 || getStorageFidelity(xvalue.m_type) != 0);
        std::vector<XValue>::push_back(xvalue);
        m_masks.push_back(mask);        
        // if number of unsorted values exceeds the sort threshold then sort
        if ((this->size() - m_sorted_size) >= m_sort_threshold) {
            const_cast<XValuesVector*>(this)->sortValues();
        }
    }
    
    bool XValuesVector::tryGetAt(unsigned int index, std::pair<StorageClass, Value> &result) const
    {
        bool has_value = false;
        std::uint64_t mask = 0;
        // try locating element within the unsorted items first (starting from the end)
        // NOTE: in case of lo-fi types we must merge values from both sorted and unsorted parts
        auto alt_it = end() - 1;
        auto alt_end = begin() + m_sorted_size - 1;
        auto it_mask = m_masks.end() - 1;
        while (alt_it != alt_end) {
            if (alt_it->getIndex() == index) {
                // lo-fi type values (under the same index) must be merged                
                if (*it_mask) {
                    if (has_value) {
                        assert(result.first == alt_it->m_type);
                        // NOTE: we exclude already assigned values from the mask (last write wins)
                        result.second.assign(alt_it->m_value, *it_mask & ~mask);                        
                    } else {
                        result.first = alt_it->m_type;
                        result.second = alt_it->m_value;
                        has_value = true;
                    }
                    mask |= *it_mask;
                } else {
                    // a complete value was found
                    result.first = alt_it->m_type;
                    result.second = alt_it->m_value;
                    return true;
                }
            }
            --alt_it;
            --it_mask;
        }
        
        if (m_sorted_size == 0) {
            // no sorted values
            return has_value;
        }
        
        // using bisect try locating element by index
        auto it_end = begin() + m_sorted_size;
        auto it = std::lower_bound(begin(), it_end, index);
        if (it == it_end || it->getIndex() != index) {
            // element not found in the sorted part
            return has_value;
        }

        if (has_value) {
            // merge lo-fi type value
            // NOTE: since mask is not known here we reverse the application order
            assert(result.first == it->m_type);
            auto temp = result.second;
            result.second = it->m_value;
            result.second.assign(temp, mask);
        } else {
            result.first = it->m_type;
            result.second = it->m_value;
            has_value = true;
        }

        return has_value;
    }
    
    void XValuesVector::sortValues()
    {
        assert(!empty());
        assert(!m_masks.empty());

        // by-index map (lo-fi types only)
        std::unordered_map<unsigned int, std::pair<XValue*, std::uint64_t> > merge_map;
        // first pass to merge lo-fi types in the unsorted part
        // this has to be done before sorting, or otherwise masks will not be accessible
        auto alt_it = end() - 1;
        auto alt_end = begin() + m_sorted_size - 1;
        auto it_mask = m_masks.end() - 1;
        while (alt_it != alt_end) {
            if (*it_mask) {
                auto it = merge_map.find(alt_it->getIndex());
                if (it == merge_map.end()) {
                    merge_map.emplace(alt_it->getIndex(), std::make_pair(&*alt_it, *it_mask));
                } else {
                    // merge into the conclusive result
                    it->second.first->m_value.assign(alt_it->m_value, *it_mask & ~it->second.second);
                    it->second.second |= *it_mask;
                }                
            }
            --alt_it;
            --it_mask;
        }

        // all masks can be dropped now
        m_masks.clear();        

        // also need to merge with the sorted part
        // where the masks are unknown
        alt_end = std::vector<XValue>::begin() - 1;
        while (alt_it != alt_end) {
            auto it = merge_map.find(alt_it->getIndex());
            if (it != merge_map.end()) {
                auto temp = it->second.first->m_value;
                it->second.first->m_value = alt_it->m_value;
                it->second.first->m_value.assign(temp, it->second.second);
            }
            --alt_it;
        }

        // stable-sort to preserve order of equal elements (sort by index)
        std::stable_sort(begin(), end());

        // finally, compact sorted results by removing duplicates (last write wins)
        auto it_in = begin(), _end = end();
        auto it_out = begin();
        ++it_in;
        while (it_in != _end) {
            if (it_in->getIndex() == it_out->getIndex()) {
                // overwrite with the latest write
                *it_out = *it_in;
            } else {
                ++it_out;
                if (it_in != it_out) {
                    *it_out = *it_in;
                }
            }
            ++it_in;
        }
        ++it_out;
        // it_out points to the last valid element
        this->erase(it_out, this->end());
        m_sorted_size = this->size();
    }

    void XValuesVector::sortAndMerge()
    {
        if (this->size() > m_sorted_size) {
            sortValues();
        }
    }
    
    bool XValuesVector::remove(unsigned int at, std::uint64_t mask)
    {
        sortAndMerge();
        auto it_end = begin() + m_sorted_size;
        auto it = std::lower_bound(begin(), it_end, at);
        if (it == it_end || it->getIndex() != at) {
            // element not found
            return false;
        }

        if (mask == 0) {
            // remove the entire element
            this->erase(it);
            --m_sorted_size;
        } else {
            // lo-fi type, clear bits indicated by the mask
            if ((it->m_value.m_store & mask) == 0) {
                // nothing stored under the mask
                return false;
            }
            it->m_value.m_store &= ~mask;
            // if the value is now zeroed then remove the entire element
            if (it->m_value.m_store == 0) {
                this->erase(it);
                --m_sorted_size;
            }
        }
        return true;
    }
    
    void XValuesVector::clear()
    {
        std::vector<XValue>::clear();
        m_masks.clear();
        m_sorted_size = 0;
    }

}
