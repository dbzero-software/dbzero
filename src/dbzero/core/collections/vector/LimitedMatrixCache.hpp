#pragma once
    
#include <vector>
#include "VLimitedMatrix.hpp"

namespace db0

{
    
    // The LimitedMatrixCache copies all contents of a specified VLimitedMatrix instance
    // into a memory and provides a "refresh" method to detect and load appended items
    // the "load" function can be customized
    // @tparam MatrixT - type of the limited matrix (VLimitedMatrix)
    // @tparam ItemT - type of a cached (in-memory) item
    template <typename MatrixT, typename ItemT>
    class LimitedMatrixCache
    {
    public:
        LimitedMatrixCache(const MatrixT &);
        
        // Set item at specified position
        void set(std::pair<std::uint32_t, std::uint32_t>, ItemT);

        // NOTE: Cache might be refreshed if item not found at first attempt
        // @return nullptr if item not set / not found
        const ItemT *tryGet(std::pair<std::uint32_t, std::uint32_t>) const;

        // Fetch appended items only (updates or deletions not reflected)
        void refresh();
        // Refresh a specific item only
        void refresh(std::pair<std::uint32_t, std::uint32_t>);
        
        // Iterator over all cached items
        struct const_iterator
        {
            const ItemT &operator*() const;
            const ItemT *operator->() const;
            const_iterator &operator++();
            
            bool operator!=(const const_iterator &) const;
        };
        
        const_iterator begin() const;
        const_iterator end() const;

    private:
        union ItemOrVector
        {
            ItemT item;
            // Dimension 2 data (if any)
            std::vector<std::optional<ItemT> > *vector_ptr;
            
            // No delete since items will be destroyed in DataItem destructor
            ~ItemOrVector() {}
        };
        
        struct DataItem
        {
            bool m_is_item;
            ItemOrVector m_data;

            ~DataItem();
        };

        // Items organized by Dimension 1
        std::vector<std::optional<DataItem > > m_dim1;
    };
    
    template <typename MatrixT, typename ItemT>
    LimitedMatrixCache<MatrixT, ItemT>::LimitedMatrixCache(const MatrixT & matrix)    
    {
        throw std::runtime_error("Not implemented");
    }
    
    template <typename MatrixT, typename ItemT>
    void LimitedMatrixCache<MatrixT, ItemT>::set(std::pair<std::uint32_t, std::uint32_t> pos, ItemT item)
    {
        throw std::runtime_error("Not implemented");
    }

    template <typename MatrixT, typename ItemT>
    const ItemT *LimitedMatrixCache<MatrixT, ItemT>::tryGet(std::pair<std::uint32_t, std::uint32_t> pos) const
    {
        throw std::runtime_error("Not implemented");
    }

    template <typename MatrixT, typename ItemT>
    void LimitedMatrixCache<MatrixT, ItemT>::refresh()
    {
        throw std::runtime_error("Not implemented");
    }

    template <typename MatrixT, typename ItemT>
    void LimitedMatrixCache<MatrixT, ItemT>::refresh(std::pair<std::uint32_t, std::uint32_t> pos)
    {
        throw std::runtime_error("Not implemented");
    }

    template <typename MatrixT, typename ItemT>
    typename LimitedMatrixCache<MatrixT, ItemT>::const_iterator LimitedMatrixCache<MatrixT, ItemT>::begin() const
    {
        throw std::runtime_error("Not implemented");
    }
    
    template <typename MatrixT, typename ItemT>
    typename LimitedMatrixCache<MatrixT, ItemT>::const_iterator LimitedMatrixCache<MatrixT, ItemT>::end() const
    {
        throw std::runtime_error("Not implemented");
    }

    template <typename MatrixT, typename ItemT>
    LimitedMatrixCache<MatrixT, ItemT>::DataItem::~DataItem()
    {
        if (m_is_item) {
            m_data.item.~ItemT();
        } else {
            delete m_data.vector_ptr;
        }
    }

    template <typename MatrixT, typename ItemT>
    const ItemT &LimitedMatrixCache<MatrixT, ItemT>::const_iterator::operator*() const
    {
        throw std::runtime_error("Not implemented");
    }

    template <typename MatrixT, typename ItemT>
    const ItemT *LimitedMatrixCache<MatrixT, ItemT>::const_iterator::operator->() const
    {
        throw std::runtime_error("Not implemented");
    }

    template <typename MatrixT, typename ItemT>
    typename LimitedMatrixCache<MatrixT, ItemT>::const_iterator &LimitedMatrixCache<MatrixT, ItemT>::const_iterator::operator++()
    {
        throw std::runtime_error("Not implemented");
    }
        
}
