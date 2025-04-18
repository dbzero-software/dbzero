#include "diff_utils.hpp"
#include <algorithm>
#include <cassert>
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{
    
    DiffRange::DiffRange(const std::vector<std::pair<std::uint16_t, std::uint16_t>> &data)
        : m_data(data)
        , m_normalized(m_data.empty())
    {
    }
    
    void DiffRange::insert(std::uint16_t begin, std::uint16_t end, std::size_t max_size)
    {
        if (m_overflow) {
            return;
        }

        if (m_data.size() >= max_size) {
            setOverflow();
            return;
        }

        m_data.push_back({ begin, end });
        m_normalized = false;
    }

    void DiffRange::clear()
    {
        m_data.clear();
        m_overflow = false;
        m_normalized = true;
    }

    bool DiffRange::isOverflow() const {
        return m_overflow;
    }
    
    DiffRangeView::DiffRangeView(const DiffRange &diff_range)
        : m_diff_ranges(&diff_range.getData())
    {
    }
    
    std::size_t DiffRangeView::size() const
    {
        if (m_diff_ranges) {
            return m_diff_ranges->size();
        }
        return 0;
    }
    
    std::pair<std::uint16_t, std::uint16_t> DiffRangeView::operator[](std::size_t index) const
    {
        if (m_diff_ranges) {
            assert(index < m_diff_ranges->size());
            return (*m_diff_ranges)[index];
        }
        return { 0, 0 };
    }

    std::uint16_t DiffRangeView::sizeOf(std::size_t index) const 
    {
        if (m_diff_ranges) {
            assert(index < m_diff_ranges->size());
            return (*m_diff_ranges)[index].second - (*m_diff_ranges)[index].first;
        }
        return 0;
    }

    bool DiffRangeView::empty() const {
        return !m_diff_ranges || m_diff_ranges->empty();
    }

    DiffRangeView::operator bool() const {
        return m_diff_ranges && !m_diff_ranges->empty();
    }

    const std::vector<std::pair<std::uint16_t, std::uint16_t>> &DiffRange::getData() const
    {
        if (m_overflow) {
            assert(false);
            THROWF(db0::InternalException) << "DiffRange overflow";
        }
        if (!m_normalized) {
            const_cast<DiffRange *>(this)->normalize();
        }
        return m_data;
    }

    void DiffRange::setOverflow()
    {
        m_overflow = true;
        m_data.clear();
    }

    void DiffRange::normalize()
    {
        // sort ranges ascending
        std::sort(m_data.begin(), m_data.end());
        // then iterate over to merge overlapping ranges
        auto it = m_data.begin(), end = m_data.end();
        if (it == end) {
            return;
        }

        auto next = it;
        ++next;
        for (; next != end; ++next) {
            if (it->second >= next->first) {
                // merge the ranges
                it->second = std::max(it->second, next->second);
            } else {
                ++it;
                if (it != next) {                 
                    *it = *next;
                }                
            }
        }
        // remove the rest of the ranges
        m_data.erase(++it, end);
        m_normalized = true;
    }

    bool getDiffs(const void *buf_1, const void *buf_2,
        std::size_t size, std::vector<std::uint16_t> &result, std::size_t max_diff, std::optional<std::size_t> max_size,
        DiffRangeView diff_ranges)
    {
        if (!max_size) {
            max_size = std::numeric_limits<std::uint16_t>::max();
        }

        assert(size <= std::numeric_limits<std::uint16_t>::max());
        if (!max_diff) {
            // by default flush as diff if less than 75% of the data differs
            max_diff = (size * 3) >> 2;
        }
        result.clear();
        const std::uint8_t *it_1 = static_cast<const std::uint8_t *>(buf_1), *it_2 = static_cast<const std::uint8_t *>(buf_2);
        auto it_base = it_1;
        auto end = it_1 + size;
        // exact number of bytes that differ
        std::uint16_t diff_bytes = 0;
        // estimated space occupied by the diff data
        std::uint16_t diff_total = 0;

        std::size_t diff_index = 0;
        const std::uint8_t *diff_start = nullptr;
        if (diff_ranges) {
            diff_start = it_base + diff_ranges[diff_index].first;
        }

        for (;;) {
            // total number of allowed diff areas exceeded
            if (result.size() >= *max_size) {
                return false;
            }
            std::uint16_t diff_len = 0;
            while (it_1 != end) {
                if (it_1 == diff_start) {
                    assert(diff_ranges);
                    auto skip = diff_ranges.sizeOf(diff_index);
                    it_1 += skip;
                    it_2 += skip;
                    diff_len += skip;
                    ++diff_index;
                    if (diff_index != diff_ranges.size()) {
                        diff_start = it_base + diff_ranges[diff_index].first;
                    } else {
                        diff_start = nullptr;
                    }
                    continue;
                }
                if (*it_1 == *it_2) {
                    break;
                }
                ++it_1;
                ++it_2;
                ++diff_len;                
            }
            
            // account for the administrative space overhead (approximate)
            diff_bytes += diff_len;
            diff_total += diff_len + sizeof(std::uint16_t);
            if (diff_total > max_diff) {
                return false;
            }
            if (diff_len || it_1 != end) {
                result.push_back(diff_len);
            }
            if (it_1 == end) {
                break;
            }
            std::uint16_t sim_len = 0;
            for (; it_1 != end && *it_1 == *it_2; ++it_1, ++it_2) {
                if (it_1 == diff_start) {
                    break;
                }
                ++sim_len;
            }
            // do not include the trailing similarity area
            if (it_1 == end) {
                break;
            }
            assert(sim_len);
            result.push_back(sim_len);
        }
        // no diffs found
        if (!diff_bytes) {
            result.clear();
        }
        return true;
    }
    
    bool getDiffs(const void *buf, std::size_t size, std::vector<std::uint16_t> &result, std::size_t max_diff,
        std::optional<std::size_t> max_size, DiffRangeView diff_ranges)
    {
        if (!max_size) {
            max_size = std::numeric_limits<std::uint16_t>::max();
        }        

        assert(size <= std::numeric_limits<std::uint16_t>::max());
        if (!max_diff) {
            // by default flush as diff if less than 75% of the data differs
            max_diff = (size * 3) >> 2;
        }
        result.clear();
        const std::uint8_t *it_base = static_cast<const std::uint8_t *>(buf);
        auto it = it_base;
        auto end = it + size;
        // estimated space occupied by the diff data
        std::uint16_t diff_total = 0;
        // include the zero-fill indicator (i.e. the 0, 0 elements)
        result.push_back(0);
        result.push_back(0);
        std::size_t diff_index = 0;
        const std::uint8_t *diff_start = nullptr;
        if (diff_ranges) {
            diff_start = it_base + diff_ranges[diff_index].first;
        }

        for (;;) {
            // total number of allowed diff areas exceeded
            if (result.size() >= *max_size) {
                return false;
            }
            std::uint16_t diff_len = 0;
            // identify non-zero bytes or forced-diff ranges
            while (it != end) {
                if (it == diff_start) {
                    assert(diff_ranges);
                    auto skip = diff_ranges.sizeOf(diff_index);
                    it += skip;
                    diff_len += skip;
                    ++diff_index;
                    if (diff_index != diff_ranges.size()) {
                        diff_start = it_base + diff_ranges[diff_index].first;
                    } else {
                        diff_start = nullptr;
                    }
                    continue;
                }
                if (!*it) {
                    break;
                }
                ++diff_len;
                ++it;
            }
            // account for the administrative space overhead (approximate)
            diff_total += diff_len + sizeof(std::uint16_t);
            if (diff_total > max_diff) {
                return false;
            }
            if (diff_len || it != end) {
                result.push_back(diff_len);
            }
            if (it == end) {
                break;
            }
            std::uint16_t sim_len = 0;
            for (; it != end && !*it; ++it) {
                if (it == diff_start) {
                    break;
                }
                ++sim_len;
            }
            // do not include the trailing similarity area
            if (it == end) {
                break;
            }
            assert(sim_len);
            result.push_back(sim_len);
        }
        return true;
    }

}
