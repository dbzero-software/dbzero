#pragma once

#include <vector>
#include <cstdint>
#include <limits>
#include <optional>

namespace db0

{
    
    class DiffRange
    {
    public:
        DiffRange() = default;
        DiffRange(const std::vector<std::pair<std::uint16_t, std::uint16_t>> &data);

        void insert(std::uint16_t begin, std::uint16_t end, std::size_t max_size);
        void clear();

        // marks the entire range as modified
        void setOverflow();
        
        // get the normalized data for reading
        const std::vector<std::pair<std::uint16_t, std::uint16_t>> &getData() const;
        
        bool isOverflow() const;
        
    private:
        std::vector<std::pair<std::uint16_t, std::uint16_t>> m_data;
        bool m_overflow = false;
        // empty = normalized
        mutable bool m_normalized = true;

        // Sort, de-duplicate and merge diff-ranges
        void normalize();
    };

    // The DiffRangeView allows selecting a sub-range of the diff ranges
    // which has applications e.g. in the WideLock / BoundaryLock implementations
    class DiffRangeView
    {
    public:
        // an empty view
        DiffRangeView() = default;
        // Create a view of the entire range
        DiffRangeView(const DiffRange &);
        
        std::size_t size() const;
        
        std::pair<std::uint16_t, std::uint16_t> operator[](std::size_t index) const;

        // retrieve the size of the diff area at the given index
        std::uint16_t sizeOf(std::size_t index) const;

        bool empty() const;

        operator bool() const;
        
    private:
        // the normalized diff ranges
        const std::vector<std::pair<std::uint16_t, std::uint16_t>> *m_diff_ranges = nullptr;        
    };
    
    // Calculate diff areas between the 2 binary buffers of the same size (e.g. DPs)
    // @param size size of the buffers
    // @param result vector to hold size of diff / size of identical areas interleaved (totalling to size)
    // NOTE that the leading elements 0, 0 indicate the 0-filled base buffer
    // @param max_diff the maximum acceptable diff in bytes (0 for default = size / 2)
    // @param max_size the maximum number of diff areas allowed to be stored in the result
    // @param diff_ranges optional forced diff-ranges collected by the ResourceLock
    // @return false if the total volume of diff areas exceeds 50% threshold
    bool getDiffs(const void *, const void *, std::size_t size, std::vector<std::uint16_t> &result, std::size_t max_diff = 0,
        std::optional<std::size_t> max_size = std::numeric_limits<std::uint16_t>::max(),
        DiffRangeView diff_ranges = {});
    
    // the getDiffs version comparing to all-zero buffer
    bool getDiffs(const void *, std::size_t size, std::vector<std::uint16_t> &result, std::size_t max_diff = 0,
        std::optional<std::size_t> max_size = std::numeric_limits<std::uint16_t>::max(),
        DiffRangeView diff_ranges = {});
    
    // Create normalized view of the entire range
    DiffRangeView getDiffs(DiffRange &);
    
}