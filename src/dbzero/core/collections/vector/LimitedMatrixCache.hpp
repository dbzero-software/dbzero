#pragma once
    
#include <vector>
#include "VLimitedMatrix.hpp"
#include <functional>
#include <optional>

namespace db0

{
    
    template <typename T1, typename T2>
    struct DefaultAdapter {
        T1 operator()(std::pair<std::uint32_t, std::uint32_t>, T2 v) const {
            return static_cast<T1>(v);
        }
    };

    // The LimitedMatrixCache copies all contents of a specified VLimitedMatrix instance
    // into a memory and provides a "refresh" method to detect and load appended items
    // the "load" function can be customized
    // @tparam MatrixT - type of the limited matrix (VLimitedMatrix)
    // @tparam ItemT - type of a cached (in-memory) item. Must be constructible from MatrixT::value_type
    // @tparam AdapterT - type of adapter function / functor converting MatrixT::value_type to ItemT
    template <typename MatrixT, typename ItemT, typename AdapterT = DefaultAdapter<ItemT, typename MatrixT::value_type> >
    class LimitedMatrixCache
    {
    public:
        using self_type = LimitedMatrixCache<MatrixT, ItemT, AdapterT>;
        using CallbackType = std::function<void(const ItemT &)>;

        // callback will be notified on each new item added either during initial load or refresh
        LimitedMatrixCache(const MatrixT &, AdapterT = {}, CallbackType = {});
        
        // @return number of cached items
        std::size_t size() const;

        // NOTE: Cache might be refreshed if item not found at first attempt
        // @return nullptr if item not set / not found
        const ItemT *tryGet(std::pair<std::uint32_t, std::uint32_t>) const;
        
        // Fetch appended items only (updates or deletions not reflected)
        // callback will be notified on each new item added
        bool refresh();
        // Reload / refresh a specific existing item only
        void reload(std::pair<std::uint32_t, std::uint32_t>);
        
    private:
        friend class const_iterator;
        friend class column_iterator;

        struct column_vector: public std::vector<std::optional<ItemT> >
        {
            // @return number of new items added (0 or 1)
            int set(std::uint32_t, ItemT item);
            const ItemT *tryGet(std::uint32_t pos) const;
            bool hasItem(std::uint32_t pos) const;
        };
        
        struct DataItem
        {
            std::optional<ItemT> m_item;
            // Dimension 2 data (if any)
            column_vector *m_vector_ptr = nullptr;
            
            DataItem() = default;
            // construct with a single item
            DataItem(ItemT &&);            
            DataItem(DataItem &&);

            ~DataItem();

            void operator=(DataItem &&other);
        };
        
        // Set item at specified position in cache
        void set(std::pair<std::uint32_t, std::uint32_t>, ItemT);
        const ItemT *_tryGet(std::pair<std::uint32_t, std::uint32_t>) const;
        // Check if an item exists in cache
        bool hasItem(std::pair<std::uint32_t, std::uint32_t>) const;
        
        std::reference_wrapper<const MatrixT> m_matrix;
        std::size_t m_size = 0;
        // Items organized by Dimension 1
        std::vector<DataItem> m_dim1;
        const column_vector m_null_column;
        AdapterT m_adapter;
        CallbackType m_callback;
        
        struct column_iterator
        {
            column_iterator(typename column_vector::const_iterator it,
                            typename column_vector::const_iterator end);
                        
            const ItemT &operator*() const;
            column_iterator &operator++();
            bool operator!=(const column_iterator &other) const;
            
            bool isEnd() const;
            void fix();
            
            typename column_vector::const_iterator m_it;
            typename column_vector::const_iterator m_end;
        };

        column_iterator endColumn() const;

    public:

        // Iterator over all cached items
        class const_iterator
        {
        public:
            const_iterator(const self_type &, typename std::vector<DataItem>::const_iterator it,
                typename std::vector<DataItem>::const_iterator end);
            
            const ItemT &operator*() const;
            const ItemT *operator->() const;
            const_iterator &operator++();
            
            bool operator!=(const const_iterator &) const;
            
        private:
            std::reference_wrapper<const self_type> m_cache;
            typename std::vector<DataItem>::const_iterator m_it;
            typename std::vector<DataItem>::const_iterator m_end;
            // is positioned at first item of a column
            bool m_first_item = true;
            // only valid if m_it is not at end and points to a vector
            column_iterator m_col_it;
            
            column_iterator getColumn() const;

            void fix();
        };
        
        const_iterator cbegin() const;
        const_iterator cend() const;
    };
    
    template <typename MatrixT, typename ItemT, typename AdapterT>
    std::size_t LimitedMatrixCache<MatrixT, ItemT, AdapterT>::size() const
    {
        return m_size;
    }
    
    template <typename MatrixT, typename ItemT, typename AdapterT>
    LimitedMatrixCache<MatrixT, ItemT, AdapterT>::LimitedMatrixCache(const MatrixT &matrix, AdapterT adapter, CallbackType callback)
        : m_matrix(matrix)
        , m_adapter(adapter)  
        , m_callback(callback) 
    {
        auto it = matrix.cbegin(), end = matrix.cend();
        for ( ; it != end; ++it) {
            auto item = m_adapter(it.loc(), *it);
            if (m_callback) {
                m_callback(item);
            }
            set(it.loc(), std::move(item));
        }
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    bool LimitedMatrixCache<MatrixT, ItemT, AdapterT>::hasItem(std::pair<std::uint32_t, std::uint32_t> pos) const
    {
        if (pos.first >= m_dim1.size()) {
            return false;
        }
        
        if (pos.second == 0) {
            if (!m_dim1[pos.first].m_item.has_value()) {
                return false;
            }
            return true;
        }
        
        if (!m_dim1[pos.first].m_vector_ptr) {
            return false;
        }
        return m_dim1[pos.first].m_vector_ptr->hasItem(pos.second - 1);        
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    const ItemT *LimitedMatrixCache<MatrixT, ItemT, AdapterT>::_tryGet(std::pair<std::uint32_t, std::uint32_t> pos) const
    {
        if (pos.first >= m_dim1.size()) {
            return nullptr;
        }
        
        if (pos.second == 0) {
            if (!m_dim1[pos.first].m_item.has_value()) {
                return nullptr;
            }
            return &m_dim1[pos.first].m_item.value();
        }
        
        if (!m_dim1[pos.first].m_vector_ptr) {
            return nullptr;
        }
        return m_dim1[pos.first].m_vector_ptr->tryGet(pos.second - 1);
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    const ItemT *LimitedMatrixCache<MatrixT, ItemT, AdapterT>::tryGet(std::pair<std::uint32_t, std::uint32_t> pos) const
    {
        auto item_ptr = _tryGet(pos);
        if (!item_ptr) {
            // try refreshing the cache
            if (const_cast<self_type *>(this)->refresh()) {
                item_ptr = _tryGet(pos);
            }
        }
        return item_ptr;
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    void LimitedMatrixCache<MatrixT, ItemT, AdapterT>::set(std::pair<std::uint32_t, std::uint32_t> pos, ItemT item)
    {
        if (pos.first >= m_dim1.size()) {
            m_dim1.resize(pos.first + 1);
        }
        
        if (pos.second == 0) {
            if (!m_dim1[pos.first].m_item.has_value()) {
                ++m_size;
            }
            m_dim1[pos.first].m_item = std::move(item);
        } else {
            // create a new vector if needed
            if (!m_dim1[pos.first].m_vector_ptr) {
                m_dim1[pos.first].m_vector_ptr = new column_vector();
            }
            // Set/replace item in vector
            m_size += m_dim1[pos.first].m_vector_ptr->set(pos.second - 1, std::move(item));
        }
    }
    
    template <typename MatrixT, typename ItemT, typename AdapterT>
    bool LimitedMatrixCache<MatrixT, ItemT, AdapterT>::refresh()
    {
        // prevent refreshing if item count did not change
        if (this->m_size == this->m_matrix.get().getItemCount()) {
            return false;
        }

        bool result = false;
        auto it = m_matrix.get().cbegin(), end = m_matrix.get().cend();
        for ( ; it != end; ++it) {
            // only add new items
            if (!this->hasItem(it.loc())) {
                auto item = m_adapter(it.loc(), *it);
                if (m_callback) {
                    m_callback(item);
                }
                this->set(it.loc(), std::move(item));
                result = true;
            }
        }
        return result;
    }
    
    template <typename MatrixT, typename ItemT, typename AdapterT>
    void LimitedMatrixCache<MatrixT, ItemT, AdapterT>::reload(std::pair<std::uint32_t, std::uint32_t> pos)
    {
        auto item = m_adapter(pos, m_matrix.get().get(pos));
        // NOTE: here we invoke callback even if the item already exists (it might've been updated)
        if (m_callback) {
            m_callback(item);
        }
        this->set(pos, std::move(item));
    }
    
    template <typename MatrixT, typename ItemT, typename AdapterT>
    LimitedMatrixCache<MatrixT, ItemT, AdapterT>::DataItem::DataItem(ItemT &&item)
        : m_item(std::move(item))        
    {
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    LimitedMatrixCache<MatrixT, ItemT, AdapterT>::DataItem::DataItem(DataItem &&other)
        : m_item(std::move(other.m_item))        
    {
        m_vector_ptr = other.m_vector_ptr;
        other.m_vector_ptr = nullptr;
    }
    
    template <typename MatrixT, typename ItemT, typename AdapterT>
    void LimitedMatrixCache<MatrixT, ItemT, AdapterT>::DataItem::operator=(DataItem &&other)
    {
        if (this == &other) {
            return;
        }
        if (m_vector_ptr) {
            delete m_vector_ptr;
            m_vector_ptr = nullptr;
        }
        m_item = std::move(other.m_item);
        m_vector_ptr = other.m_vector_ptr;
        other.m_vector_ptr = nullptr;
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    LimitedMatrixCache<MatrixT, ItemT, AdapterT>::DataItem::~DataItem()
    {
        if (m_vector_ptr) {
            delete m_vector_ptr;
            m_vector_ptr = nullptr;
        }
    }
    
    template <typename MatrixT, typename ItemT, typename AdapterT>
    const ItemT &LimitedMatrixCache<MatrixT, ItemT, AdapterT>::const_iterator::operator*() const
    {
        assert(m_it != m_end);
        if (this->m_first_item) {
            return m_it->m_item.value();
        } else {
            assert(!m_col_it.isEnd());
            return *m_col_it;
        }
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    const ItemT *LimitedMatrixCache<MatrixT, ItemT, AdapterT>::const_iterator::operator->() const
    {
        return &(**this);
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    int LimitedMatrixCache<MatrixT, ItemT, AdapterT>::column_vector::set(std::uint32_t pos, ItemT item)
    {
        if (pos >= this->size()) {
            this->resize(pos + 1);
            (*this)[pos] = std::move(item);
            return 1;
        } else {
            int result = (*this)[pos].has_value() ? 0 : 1;
            (*this)[pos] = std::move(item);
            return result;
        }
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    bool LimitedMatrixCache<MatrixT, ItemT, AdapterT>::column_vector::hasItem(std::uint32_t pos) const 
    {    
        return pos < this->size() && (*this)[pos].has_value();
    }
    
    template <typename MatrixT, typename ItemT, typename AdapterT>
    const ItemT *LimitedMatrixCache<MatrixT, ItemT, AdapterT>::column_vector::tryGet(std::uint32_t pos) const
    {
        if (pos >= this->size() || !(*this)[pos].has_value()) {
            return nullptr;        
        }
        return &(*this)[pos].value();
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    LimitedMatrixCache<MatrixT, ItemT, AdapterT>::column_iterator::column_iterator(typename column_vector::const_iterator it,
        typename column_vector::const_iterator end)
        : m_it(it)
        , m_end(end)
    {
        this->fix();
    }
    
    template <typename MatrixT, typename ItemT, typename AdapterT>
    const ItemT &LimitedMatrixCache<MatrixT, ItemT, AdapterT>::column_iterator::operator*() const
    {
        assert(m_it != m_end);
        return m_it->value();
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    typename LimitedMatrixCache<MatrixT, ItemT, AdapterT>::column_iterator &
    LimitedMatrixCache<MatrixT, ItemT, AdapterT>::column_iterator::operator++()
    {
        ++m_it;
        this->fix();
        return *this;
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    bool LimitedMatrixCache<MatrixT, ItemT, AdapterT>::column_iterator::isEnd() const
    {
        return m_it == m_end;
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    void LimitedMatrixCache<MatrixT, ItemT, AdapterT>::column_iterator::fix()
    {
        while (m_it != m_end && !m_it->has_value()) {
            ++m_it;
        }
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    typename LimitedMatrixCache<MatrixT, ItemT, AdapterT>::column_iterator 
    LimitedMatrixCache<MatrixT, ItemT, AdapterT>::endColumn() const
    {
        return column_iterator(m_null_column.cend(), m_null_column.cend());
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    LimitedMatrixCache<MatrixT, ItemT, AdapterT>::const_iterator::const_iterator(const self_type &cache,
        typename std::vector<DataItem>::const_iterator it,
        typename std::vector<DataItem>::const_iterator end)
        : m_cache(cache)
        , m_it(it)
        , m_end(end)
        , m_col_it(getColumn())
    {
        this->fix();
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    typename LimitedMatrixCache<MatrixT, ItemT, AdapterT>::const_iterator &
    LimitedMatrixCache<MatrixT, ItemT, AdapterT>::const_iterator::operator++()
    {
        if (this->m_first_item) {
            this->m_first_item = false;            
        } else {
            if (m_col_it.isEnd()) {
                // Move to next DataItem
                ++m_it;
                this->m_first_item = true;
                m_col_it = getColumn();
            } else {
                ++m_col_it;
            }
        }
        this->fix();
        return *this;
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    typename LimitedMatrixCache<MatrixT, ItemT, AdapterT>::column_iterator 
    LimitedMatrixCache<MatrixT, ItemT, AdapterT>::const_iterator::getColumn() const
    {
        if (m_it == m_end || !m_it->m_vector_ptr) {
            return m_cache.get().endColumn();
        }
        return column_iterator(m_it->m_vector_ptr->cbegin(), m_it->m_vector_ptr->cend());
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    void LimitedMatrixCache<MatrixT, ItemT, AdapterT>::const_iterator::fix()
    {
        while (m_it != m_end) {
            if (m_first_item) {
                if (m_it->m_item.has_value()) {
                    // Found item
                    return;
                }
                m_first_item = false;
            }

            if (!m_col_it.isEnd()) {
                return;
            }
            // Move to next DataItem
            ++m_it;
            m_first_item = true;
            m_col_it = getColumn();            
        }
    }
    
    template <typename MatrixT, typename ItemT, typename AdapterT>
    bool LimitedMatrixCache<MatrixT, ItemT, AdapterT>::const_iterator::operator!=(const const_iterator &other) const
    {
        return m_it != other.m_it || m_first_item != other.m_first_item || m_col_it != other.m_col_it;
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    bool LimitedMatrixCache<MatrixT, ItemT, AdapterT>::column_iterator::operator!=(const column_iterator &other) const
    {
        return m_it != other.m_it;
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    typename LimitedMatrixCache<MatrixT, ItemT, AdapterT>::const_iterator 
    LimitedMatrixCache<MatrixT, ItemT, AdapterT>::cbegin() const
    {
        return const_iterator(*this, m_dim1.cbegin(), m_dim1.cend());
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    typename LimitedMatrixCache<MatrixT, ItemT, AdapterT>::const_iterator 
    LimitedMatrixCache<MatrixT, ItemT, AdapterT>::cend() const
    {
        return const_iterator(*this, m_dim1.cend(), m_dim1.cend());    
    }
    
}
