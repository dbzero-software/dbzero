// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once
    
#include <vector>
#include <functional>
#include <optional>

namespace db0

{
    
    // LimitedMatrix is in-memory version of VLimitedMatrix
    // See VLimitedMatrix for details
    template <typename ItemT>
    class LimitedMatrix
    {
    public:
        using self_type = LimitedMatrix<ItemT>;
        
        LimitedMatrix() = default;
        
        // @return number of items in the matrix
        std::size_t size() const;
        
        // Set item at specified position in cache
        void set(std::pair<std::uint32_t, std::uint32_t>, ItemT);
        // @return nullptr if item not set / not found
        const ItemT *tryGet(std::pair<std::uint32_t, std::uint32_t>) const;
        // @throw std::out_of_range if item not set / not found
        const ItemT &get(std::pair<std::uint32_t, std::uint32_t>) const;
        
        // Modify an existing item (or throw)
        ItemT &modifyItem(std::pair<std::uint32_t, std::uint32_t>);

        // Check if an item was set
        bool hasItem(std::pair<std::uint32_t, std::uint32_t>) const;
        void clear();
        
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
        
        std::size_t m_size = 0;
        // Items organized by Dimension 1
        std::vector<DataItem> m_dim1;
        const column_vector m_null_column;

        struct column_iterator
        {
            column_iterator(typename column_vector::const_iterator it,
                            typename column_vector::const_iterator end);
                        
            const ItemT &operator*() const;
            column_iterator &operator++();
            bool operator!=(const column_iterator &other) const;
            bool operator==(const column_iterator &other) const;
            
            std::uint32_t loc() const {
                assert(m_it != m_end);
                return static_cast<std::uint32_t>(std::distance(m_begin, m_it));
            }

            bool isEnd() const;
            void fix();
            
            typename column_vector::const_iterator m_begin;
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
            bool operator==(const const_iterator &) const;

            std::pair<std::uint32_t, std::uint32_t> loc() const;
            
        private:
            std::reference_wrapper<const self_type> m_cache;
            typename std::vector<DataItem>::const_iterator m_begin;
            typename std::vector<DataItem>::const_iterator m_it;
            typename std::vector<DataItem>::const_iterator m_end;
            // is positioned at first item of a column
            bool m_first_item = true;
            // only valid if m_it is not at end and points to a vector
            column_iterator m_col_it;
            
            column_iterator getColumn() const;

            void fix();
        };
        
        const_iterator begin() const;
        const_iterator end() const;
    };
    
    template <typename ItemT> void LimitedMatrix<ItemT>::clear()
    {
        m_dim1.clear();
        m_size = 0;
    }

    template <typename ItemT>
    std::size_t LimitedMatrix<ItemT>::size() const {
        return m_size;
    }
    
    template <typename ItemT>
    bool LimitedMatrix<ItemT>::hasItem(std::pair<std::uint32_t, std::uint32_t> pos) const
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

    template <typename ItemT>
    const ItemT *LimitedMatrix<ItemT>::tryGet(std::pair<std::uint32_t, std::uint32_t> pos) const
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

    template <typename ItemT>
    const ItemT &LimitedMatrix<ItemT>::get(std::pair<std::uint32_t, std::uint32_t> loc) const
    {
        const ItemT *item_ptr = tryGet(loc);
        if (!item_ptr) {
            throw std::out_of_range("Item not found");
        }
        return *item_ptr;
    }

    template <typename ItemT>
    ItemT &LimitedMatrix<ItemT>::modifyItem(std::pair<std::uint32_t, std::uint32_t> loc)
    {
        const ItemT *item_ptr = tryGet(loc);
        if (!item_ptr) {
            throw std::out_of_range("Item not found");
        }
        return const_cast<ItemT&>(*item_ptr);
    }

    template <typename ItemT>
    void LimitedMatrix<ItemT>::set(std::pair<std::uint32_t, std::uint32_t> pos, ItemT item)
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

    template <typename ItemT>
    LimitedMatrix<ItemT>::DataItem::DataItem(ItemT &&item)
        : m_item(std::move(item))
    {
    }

    template <typename ItemT>
    LimitedMatrix<ItemT>::DataItem::DataItem(DataItem &&other)
        : m_item(std::move(other.m_item))        
    {
        m_vector_ptr = other.m_vector_ptr;
        other.m_vector_ptr = nullptr;
    }

    template <typename ItemT> 
    void LimitedMatrix<ItemT>::DataItem::operator=(DataItem &&other)
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

    template <typename ItemT>
    LimitedMatrix<ItemT>::DataItem::~DataItem()
    {
        if (m_vector_ptr) {
            delete m_vector_ptr;
            m_vector_ptr = nullptr;
        }
    }

    template <typename ItemT>
    const ItemT &LimitedMatrix<ItemT>::const_iterator::operator*() const
    {
        assert(m_it != m_end);
        if (this->m_first_item) {
            return m_it->m_item.value();
        } else {
            assert(!m_col_it.isEnd());
            return *m_col_it;
        }
    }

    template <typename ItemT>
    std::pair<std::uint32_t, std::uint32_t> LimitedMatrix<ItemT>::const_iterator::loc() const
    {
        assert(m_it != m_end);        
        if (this->m_first_item) {
            return { static_cast<std::uint32_t>(std::distance(m_begin, m_it)), 0 };
        } else {
            return { static_cast<std::uint32_t>(std::distance(m_begin, m_it)), m_col_it.loc() + 1 };
        }
    }
    
    template <typename ItemT>
    const ItemT *LimitedMatrix<ItemT>::const_iterator::operator->() const
    {
        return &(**this);
    }

    template <typename ItemT>
    int LimitedMatrix<ItemT>::column_vector::set(std::uint32_t pos, ItemT item)
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

    template <typename ItemT>
    bool LimitedMatrix<ItemT>::column_vector::hasItem(std::uint32_t pos) const
    {    
        return pos < this->size() && (*this)[pos].has_value();
    }

    template <typename ItemT>
    const ItemT *LimitedMatrix<ItemT>::column_vector::tryGet(std::uint32_t pos) const
    {
        if (pos >= this->size() || !(*this)[pos].has_value()) {
            return nullptr;        
        }
        return &(*this)[pos].value();
    }

    template <typename ItemT>
    LimitedMatrix<ItemT>::column_iterator::column_iterator(typename column_vector::const_iterator it,
        typename column_vector::const_iterator end)
        : m_begin(it)
        , m_it(it)
        , m_end(end)
    {
        this->fix();
    }

    template <typename ItemT>
    const ItemT &LimitedMatrix<ItemT>::column_iterator::operator*() const
    {
        assert(m_it != m_end);
        return m_it->value();
    }

    template <typename ItemT> typename LimitedMatrix<ItemT>::column_iterator &
    LimitedMatrix<ItemT>::column_iterator::operator++()
    {
        ++m_it;
        this->fix();
        return *this;
    }

    template <typename ItemT>
    bool LimitedMatrix<ItemT>::column_iterator::isEnd() const
    {
        return m_it == m_end;
    }

    template <typename ItemT>
    void LimitedMatrix<ItemT>::column_iterator::fix()
    {
        while (m_it != m_end && !m_it->has_value()) {
            ++m_it;
        }
    }

    template <typename ItemT> typename LimitedMatrix<ItemT>::column_iterator
    LimitedMatrix<ItemT>::endColumn() const
    {
        return column_iterator(m_null_column.cend(), m_null_column.cend());
    }

    template <typename ItemT>
    LimitedMatrix<ItemT>::const_iterator::const_iterator(const self_type &cache,
        typename std::vector<DataItem>::const_iterator it,
        typename std::vector<DataItem>::const_iterator end)
        : m_cache(cache)
        , m_begin(it)
        , m_it(it)
        , m_end(end)
        , m_col_it(getColumn())
    {
        this->fix();
    }

    template <typename ItemT> typename LimitedMatrix<ItemT>::const_iterator &
    LimitedMatrix<ItemT>::const_iterator::operator++()
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

    template <typename ItemT> typename LimitedMatrix<ItemT>::column_iterator 
    LimitedMatrix<ItemT>::const_iterator::getColumn() const
    {
        if (m_it == m_end || !m_it->m_vector_ptr) {
            return m_cache.get().endColumn();
        }
        return column_iterator(m_it->m_vector_ptr->cbegin(), m_it->m_vector_ptr->cend());
    }

    template <typename ItemT>
    void LimitedMatrix<ItemT>::const_iterator::fix()
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

    template <typename ItemT>
    bool LimitedMatrix<ItemT>::const_iterator::operator!=(const const_iterator &other) const
    {
        return m_it != other.m_it || m_first_item != other.m_first_item || m_col_it != other.m_col_it;
    }

    template <typename ItemT>
    bool LimitedMatrix<ItemT>::const_iterator::operator==(const const_iterator &other) const
    {
        return m_it == other.m_it && m_first_item == other.m_first_item && m_col_it == other.m_col_it;
    }

    template <typename ItemT>
    bool LimitedMatrix<ItemT>::column_iterator::operator!=(const column_iterator &other) const
    {
        return m_it != other.m_it;
    }

    template <typename ItemT>
    bool LimitedMatrix<ItemT>::column_iterator::operator==(const column_iterator &other) const
    {
        return m_it == other.m_it;
    }

    template <typename ItemT> typename LimitedMatrix<ItemT>::const_iterator 
    LimitedMatrix<ItemT>::begin() const
    {
        return const_iterator(*this, m_dim1.cbegin(), m_dim1.cend());
    }

    template <typename ItemT> typename LimitedMatrix<ItemT>::const_iterator 
    LimitedMatrix<ItemT>::end() const
    {
        return const_iterator(*this, m_dim1.cend(), m_dim1.cend());    
    }
    
}
