#pragma once
    
#include <vector>
#include "VLimitedMatrix.hpp"
#include <functional>
#include <optional>

namespace db0

{
    
    template <typename T1, typename T2>
    struct DefaultAdapter {
        T1 operator()(T2 v) const {
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

        LimitedMatrixCache(const MatrixT &, AdapterT = {});
        
        // @return number of cached items
        std::size_t size() const;

        // NOTE: Cache might be refreshed if item not found at first attempt
        // @return nullptr if item not set / not found
        const ItemT *tryGet(std::pair<std::uint32_t, std::uint32_t>) const;

        // Fetch appended items only (updates or deletions not reflected)
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
        };
        
        union ItemOrVector
        {
            ItemT item;
            // Dimension 2 data (if any)
            column_vector *vector_ptr;
            
            // No delete since items will be destroyed in DataItem destructor
            ~ItemOrVector() {}
        };
        
        struct as_vector {};
        struct DataItem
        {
            bool m_has_value = false;
            bool m_is_item = true;
            ItemOrVector m_data;

            DataItem() = default;
            // construct as item
            DataItem(ItemT &&);
            DataItem(as_vector);
            DataItem(DataItem &&);

            ~DataItem();

            void operator=(DataItem &&other);
        };

        // Set item at specified position in cache
        void set(std::pair<std::uint32_t, std::uint32_t>, ItemT);

        std::reference_wrapper<const MatrixT> m_matrix;
        std::size_t m_size = 0;
        // Items organized by Dimension 1
        std::vector<DataItem> m_dim1;
        const column_vector m_null_column;
        AdapterT m_adapter;
        
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
    LimitedMatrixCache<MatrixT, ItemT, AdapterT>::LimitedMatrixCache(const MatrixT &matrix, AdapterT adapter)
        : m_matrix(matrix)
        , m_adapter(adapter)
    {
        auto it = matrix.cbegin(), end = matrix.cend();
        for ( ; it != end; ++it) {
            set(it.loc(), m_adapter(*it));
        }
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    const ItemT *LimitedMatrixCache<MatrixT, ItemT, AdapterT>::tryGet(std::pair<std::uint32_t, std::uint32_t> pos) const
    {
        if (pos.first >= m_dim1.size()) {
            return nullptr;
        }

        if (!m_dim1[pos.first].m_has_value) {
            return nullptr;
        }

        if (m_dim1[pos.first].m_is_item) {
            if (pos.second == 0) {
                return &m_dim1[pos.first].m_data.item;
            } else {
                return nullptr;
            }
        } else {
            return m_dim1[pos.first].m_data.vector_ptr->tryGet(pos.second);
        }
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    void LimitedMatrixCache<MatrixT, ItemT, AdapterT>::set(std::pair<std::uint32_t, std::uint32_t> pos, ItemT item)
    {
        if (pos.first >= m_dim1.size()) {
            m_dim1.resize(pos.first + 1);
        }
        
        if (pos.second == 0) {
            if (m_dim1[pos.first].m_has_value) {
                if (m_dim1[pos.first].m_is_item) {
                    // Replace existing item
                    m_dim1[pos.first].m_data.item = std::move(item);
                } else {
                    m_size += m_dim1[pos.first].m_data.vector_ptr->set(0, std::move(item));
                }
            } else {
                // New item
                DataItem di(std::move(item));
                m_dim1[pos.first] = std::move(di);
                ++m_size;
            }

        } else {
            // convert item to vector if needed
            if (m_dim1[pos.first].m_has_value) {
                if (m_dim1[pos.first].m_is_item) {
                    // Convert item to vector
                    auto vec = new column_vector();
                    vec->set(0, std::move(m_dim1[pos.first].m_data.item));
                    m_dim1[pos.first].m_data.vector_ptr = vec;
                    m_dim1[pos.first].m_is_item = false;
                }
            } else {
                // New empty vector
                DataItem di(as_vector{});
                m_dim1[pos.first] = std::move(di);
            }
            // Set/replace item in vector
            m_size += m_dim1[pos.first].m_data.vector_ptr->set(pos.second, std::move(item));
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
            if (!this->tryGet(it.loc())) {
                this->set(it.loc(), m_adapter(*it));
                result = true;
            }
        }
        return result;
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    void LimitedMatrixCache<MatrixT, ItemT, AdapterT>::reload(std::pair<std::uint32_t, std::uint32_t> pos)
    {
        this->set(pos, m_matrix.get().get(pos));
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    LimitedMatrixCache<MatrixT, ItemT, AdapterT>::DataItem::DataItem(ItemT &&item)
        : m_has_value(true)
        , m_is_item(true)
    {
        new (&m_data.item) ItemT(std::move(item));
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    LimitedMatrixCache<MatrixT, ItemT, AdapterT>::DataItem::DataItem(as_vector)
        : m_has_value(true)
        , m_is_item(false)
    {
        m_data.vector_ptr = new column_vector();
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    LimitedMatrixCache<MatrixT, ItemT, AdapterT>::DataItem::DataItem(DataItem &&other)
        : m_has_value(other.m_has_value)
        , m_is_item(other.m_is_item)
    {
        if (!m_has_value) {
            return;
        }
        if (m_is_item) {
            new (&m_data.item) ItemT(std::move(other.m_data.item));
        } else {
            m_data.vector_ptr = other.m_data.vector_ptr;
            other.m_data.vector_ptr = nullptr;
        }
        other.m_has_value = false;
    }
    
    template <typename MatrixT, typename ItemT, typename AdapterT>
    void LimitedMatrixCache<MatrixT, ItemT, AdapterT>::DataItem::operator=(DataItem &&other)
    {
        if (this == &other) {
            return;
        }
        // Destroy existing value
        this->~DataItem();
        
        m_has_value = other.m_has_value;
        m_is_item = other.m_is_item;
        if (!m_has_value) {
            return;
        }
        if (m_is_item) {
            new (&m_data.item) ItemT(std::move(other.m_data.item));
        } else {
            m_data.vector_ptr = other.m_data.vector_ptr;
            other.m_data.vector_ptr = nullptr;
        }
        other.m_has_value = false;
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    LimitedMatrixCache<MatrixT, ItemT, AdapterT>::DataItem::~DataItem()
    {
        if (!m_has_value) {
            return;
        }
        if (m_is_item) {
            m_data.item.~ItemT();
        } else {
            delete m_data.vector_ptr;
        }
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    const ItemT &LimitedMatrixCache<MatrixT, ItemT, AdapterT>::const_iterator::operator*() const
    {
        assert(m_it != m_end);
        if (m_it->m_is_item) {
            return m_it->m_data.item;
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
    typename LimitedMatrixCache<MatrixT, ItemT, AdapterT>::const_iterator &
    LimitedMatrixCache<MatrixT, ItemT, AdapterT>::const_iterator::operator++()
    {
        if (!m_col_it.isEnd()) {
            ++m_col_it;
        } else {
            ++m_it;
            m_col_it = getColumn();
        }
        this->fix();
        return *this;
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
    typename LimitedMatrixCache<MatrixT, ItemT, AdapterT>::column_iterator 
    LimitedMatrixCache<MatrixT, ItemT, AdapterT>::const_iterator::getColumn() const
    {
        if (m_it == m_end || !m_it->m_has_value || m_it->m_is_item) {
            return m_cache.get().endColumn();
        }
        return column_iterator(m_it->m_data.vector_ptr->cbegin(),
                              m_it->m_data.vector_ptr->cend());
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    void LimitedMatrixCache<MatrixT, ItemT, AdapterT>::const_iterator::fix()
    {
        while (m_it != m_end) {
            if (m_it->m_has_value) {
                if (m_it->m_is_item) {
                    // Found item
                    return;
                }
                // Vector
                if (!m_col_it.isEnd()) {
                    // Found item in vector
                    return;
                } else {
                    // Move to next column
                    ++m_it;
                    m_col_it = getColumn();
                }
            } else {
                ++m_it;
                m_col_it = getColumn();
            }
        }
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    bool LimitedMatrixCache<MatrixT, ItemT, AdapterT>::const_iterator::operator!=(const const_iterator &other) const
    {
        return m_it != other.m_it || m_col_it != other.m_col_it;
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
