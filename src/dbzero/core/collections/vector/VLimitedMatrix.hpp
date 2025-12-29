// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "v_bvector.hpp"
#include "v_sorted_vector.hpp"
#include <dbzero/core/serialization/FixedVersioned.hpp>
#include <dbzero/core/memory/Address.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/core/memory/Memspace.hpp>
#include <dbzero/core/serialization/optional_item.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{
    
    template <typename PtrT>
DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_limited_matrix: public o_fixed_versioned<o_limited_matrix<PtrT> >
    {
        // Points to v_bvector representing the entire Dimension 1
        PtrT m_dim1_ptr = {};
        // Points to v_sorted_vector holding the index of Dimension 2 values
        PtrT m_index_ptr = {};
        // Points to v_bvector holding the sparse matrix with the exclusion of Dim1[0] vector
        PtrT m_sparse_matrix_ptr = {};
        // the total number of non-empty items in the matrix
        std::uint64_t m_item_count = 0;
        std::array<std::uint64_t, 2> m_reserved = {0, 0};
    };
DB0_PACKED_END
    
DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_dim2_index_item: public o_fixed<o_dim2_index_item>
    {
        // key from the Dimension 1
        std::uint32_t m_key;
        // start offset Dim2 vector (complete)
        std::uint64_t m_offset;

        o_dim2_index_item(std::uint32_t key, std::uint64_t offset)
            : m_key(key)
            , m_offset(offset)
        {
        }

        bool operator<(const o_dim2_index_item &other) const {
            return m_key < other.m_key;
        }
    };
DB0_PACKED_END

    // The LimitedMatrix is optimized for representing matrices
    // with the following properties / constraints:
    //  - Dimension 1 holds values primarily consecutively (e.g. rows in a row-major matrix)
    //  - Dimension 2 is limited to a known, small number of values
    //  - The most common values in Dimension 2 are "0"
    //  - Elements are frequently appended but rarely removed
    //  - O(1) access time is required
    // @tparam ItemT - Dim1 item type (fixed size, no pointers)
    // @tparam PtrT - type for inner pointers (V-Space)
    // @tparam Dim2 - maximum size of the Dimension 2
    template <typename ItemT, unsigned int Dim2 = 16, typename PtrT = Address>
    class VLimitedMatrix
        : public v_object<o_limited_matrix<PtrT> >
    {
    public:
        using value_type = ItemT;
        using self_t = VLimitedMatrix<ItemT, Dim2, PtrT>;
        using super_t = v_object<o_limited_matrix<PtrT> >;
        
        // as null
        VLimitedMatrix() = default;
        VLimitedMatrix(Memspace &);
        VLimitedMatrix(mptr);
        
        // add new or update already existing item
        void set(std::pair<std::uint32_t, std::uint32_t> index, const ItemT &);
        
        // get an item if exists
        std::optional<ItemT> tryGet(std::pair<std::uint32_t, std::uint32_t> index) const;
        // get or raise if not exists
        ItemT get(std::pair<std::uint32_t, std::uint32_t> index) const;
        // modify an existing item or raise if does not exist
        ItemT &modifyItem(std::pair<std::uint32_t, std::uint32_t> index);
        // check if an item exists
        bool hasItem(std::pair<std::uint32_t, std::uint32_t>) const;
        
        // @return Dim1 x Dim2 (constant)
        std::pair<std::size_t, std::uint32_t> size() const;

        // @return the total number of non-empty items stored
        std::size_t getItemCount() const;
        
        // Try locating first unassigned key at Dimension 2 for a specific Dim1 key
        std::optional<std::uint32_t> findUnassignedKey(std::uint32_t dim_1) const;

        // push back to Dimension 1
        void push_back(const ItemT &, std::uint32_t dim2_key = 0);

        void detach() const;
        void commit() const;
        
        static constexpr std::size_t maxDim2() {
            return Dim2;
        }

    private:
        v_bvector<o_optional_item<ItemT>, PtrT> m_dim1;
        v_sorted_vector<o_dim2_index_item, PtrT> m_index;
        v_bvector<o_optional_item<ItemT>, PtrT> m_sparse_matrix;

        // single column iterator
        struct column_iterator    
        {            
            using IteratorT = typename v_bvector<o_optional_item<ItemT>, PtrT>::const_iterator;
            
            column_iterator(std::uint32_t column_id, const IteratorT &begin, const IteratorT &end);

            ItemT operator*() const;            
            column_iterator &operator++();
            bool isEnd() const;
            
            std::uint32_t m_column_id = 0;            
            // Dim2 index (1..Dim2-1)
            std::uint32_t m_current_index = 1;
            IteratorT m_it;
            IteratorT m_end;

            void fix();
        };

        column_iterator beginColumn(const o_dim2_index_item &) const;
        column_iterator endColumn() const;

    public:

        class const_iterator
        {
        public:
            using MatrixT = VLimitedMatrix<ItemT, Dim2, PtrT>;
            using IteratorT = typename v_bvector<o_optional_item<ItemT>, PtrT>::const_iterator;
            using IndexIteratorT = typename v_sorted_vector<o_dim2_index_item, PtrT>::const_iterator;

            ItemT operator*() const;            
            const_iterator &operator++();
            bool operator!=(const const_iterator &) const;

            std::pair<std::uint32_t, std::uint32_t> loc() const;

        protected:
            friend MatrixT;
            const_iterator(const MatrixT &, bool is_end = false);

        private:
            std::pair<std::uint32_t, std::uint32_t> m_current_index = {0, 0};
            
            std::reference_wrapper<const MatrixT> m_matrix;
            IteratorT m_it;
            IteratorT m_end;
            IndexIteratorT m_index_it;
            IndexIteratorT m_index_end;
            // current column (only for columns with dimension 2 > 1)
            column_iterator m_col_it;
            
            void fix();
        };

        // Iterate over all non-empty items (all dimensions)
        const_iterator cbegin() const;
        const_iterator cend() const;
    };

    template <typename ItemT, unsigned int Dim2, typename PtrT>
    VLimitedMatrix<ItemT, Dim2, PtrT>::VLimitedMatrix(Memspace &memspace)
        : v_object<o_limited_matrix<PtrT> >(memspace)
        , m_dim1(memspace)
        , m_index(memspace)
        , m_sparse_matrix(memspace)
    {
        this->modify().m_dim1_ptr = m_dim1.getAddress();
        this->modify().m_index_ptr = m_index.getAddress();
        this->modify().m_sparse_matrix_ptr = m_sparse_matrix.getAddress();
    }

    template <typename ItemT, unsigned int Dim2, typename PtrT>
    VLimitedMatrix<ItemT, Dim2, PtrT>::VLimitedMatrix(mptr ptr)
        : v_object<o_limited_matrix<PtrT> >(ptr)
        , m_dim1(this->myPtr((*this)->m_dim1_ptr))
        , m_index(this->myPtr((*this)->m_index_ptr))
        , m_sparse_matrix(this->myPtr((*this)->m_sparse_matrix_ptr))
    {
    }

    template <typename ItemT, unsigned int Dim2, typename PtrT> 
    std::pair<std::size_t, std::uint32_t> VLimitedMatrix<ItemT, Dim2, PtrT>::size() const
    {
        return { m_dim1.size(), Dim2 };
    }

    template <typename ItemT, unsigned int Dim2, typename PtrT>
    void VLimitedMatrix<ItemT, Dim2, PtrT>::detach() const
    {
        m_dim1.detach();
        m_index.detach();
        m_sparse_matrix.detach();
        super_t::detach();
    }

    template <typename ItemT, unsigned int Dim2, typename PtrT>
    void VLimitedMatrix<ItemT, Dim2, PtrT>::commit() const
    {
        m_dim1.commit();
        m_index.commit();
        m_sparse_matrix.commit();
        super_t::commit();
    }
    
    template <typename ItemT, unsigned int Dim2, typename PtrT>
    void VLimitedMatrix<ItemT, Dim2, PtrT>::push_back(const ItemT &value, std::uint32_t dim2_key)
    {
        assert(dim2_key < Dim2 && "Dimension 2 key out of range");
        if (dim2_key) {
            std::uint32_t key_0 = m_dim1.size();
            m_dim1.emplace_back();
            std::uint32_t offset = m_sparse_matrix.size();
            bool addr_changed = false;
            m_index.insert(o_dim2_index_item(key_0, offset), addr_changed);
            if (addr_changed) {
                this->modify().m_index_ptr = m_index.getAddress();
            }
            m_sparse_matrix.growBy(Dim2 - 1);
            m_sparse_matrix.setItem(offset + dim2_key - 1, value);
        } else {
            m_dim1.push_back(value);            
        }
        this->modify().m_item_count += 1;
    }

    template <typename ItemT, unsigned int Dim2, typename PtrT>
    bool VLimitedMatrix<ItemT, Dim2, PtrT>::hasItem(std::pair<std::uint32_t, std::uint32_t> index) const
    {
        assert(index.second < Dim2 && "Dimension 2 index out of range");
        if (index.first >= m_dim1.size()) {
            return false;
        }
        if (index.second) {
            auto it = m_index.find(o_dim2_index_item(index.first, 0));
            if (it != m_index.end()) {
                std::uint64_t offset = it->m_offset + index.second - 1;
                return m_sparse_matrix.getItem(offset).isSet();
            } else {
                return false;
            }
        } else {
            return m_dim1[index.first].isSet();
        }
    }
    
    template <typename ItemT, unsigned int Dim2, typename PtrT>
    std::optional<ItemT> VLimitedMatrix<ItemT, Dim2, PtrT>::tryGet(std::pair<std::uint32_t, std::uint32_t> index) const
    {
        assert(index.second < Dim2 && "Dimension 2 index out of range");
        if (index.first >= m_dim1.size()) {
            return {};
        }
        if (index.second) {
            auto it = m_index.find(o_dim2_index_item(index.first, 0));
            if (it != m_index.end()) {
                std::uint64_t offset = it->m_offset + index.second - 1;
                return m_sparse_matrix.getItem(offset);
            } else {
                return {};
            }
        } else {
            return m_dim1.getItem(index.first);
        }
    }

    template <typename ItemT, unsigned int Dim2, typename PtrT>
    ItemT VLimitedMatrix<ItemT, Dim2, PtrT>::get(std::pair<std::uint32_t, std::uint32_t> index) const
    {
        auto result = tryGet(index);
        if (result) {
            return *result;
        } else {
            THROWF(db0::InternalException)
                << "Index (" << index.first << "," << index.second << ") not found" << THROWF_END;
        }
    }
    
    template <typename ItemT, unsigned int Dim2, typename PtrT>
    void VLimitedMatrix<ItemT, Dim2, PtrT>::set(std::pair<std::uint32_t, std::uint32_t> index, const ItemT &value)
    {
        assert(index.second < Dim2 && "Dimension 2 index out of range");
        if (index.second) {
            if (index.first >= m_dim1.size()) {                
                m_dim1.setItem(index.first, {});
            }
            auto it = m_index.find(o_dim2_index_item(index.first, 0));
            if (it == m_index.end()) {
                std::uint32_t key_0 = index.first;
                std::uint32_t offset = m_sparse_matrix.size();
                bool addr_changed = false;
                m_index.insert(o_dim2_index_item(key_0, offset), addr_changed);
                if (addr_changed) {
                    this->modify().m_index_ptr = m_index.getAddress();
                }
                m_sparse_matrix.growBy(Dim2 - 1);                
                m_sparse_matrix.setItem(offset + index.second - 1, value);                
                this->modify().m_item_count += 1;
            } else {
                std::uint64_t offset = it->m_offset + index.second - 1;
                bool is_set = m_sparse_matrix.getItem(offset).isSet();
                m_sparse_matrix.setItem(offset, value);
                if (!is_set) {
                    this->modify().m_item_count += 1;
                }
            }
        } else {
            bool is_set = (index.first < m_dim1.size()) && m_dim1[index.first].isSet();
            m_dim1.setItem(index.first, value);
            if (!is_set) {
                this->modify().m_item_count += 1;
            }
        }
    }

    template <typename ItemT, unsigned int Dim2, typename PtrT>
    std::optional<std::uint32_t> VLimitedMatrix<ItemT, Dim2, PtrT>::findUnassignedKey(std::uint32_t dim_1) const
    {
        if (dim_1 >= m_dim1.size()) {
            return {};
        }
        if (!m_dim1[dim_1].isSet()) {
            return 0u; // Dim2[0] unassigned
        }
        auto it = m_index.find(o_dim2_index_item(dim_1, 0));
        if (it == m_index.end()) {
            return 1u; // all keys unassigned
        } else {
            std::uint64_t offset = it->m_offset;
            for (std::uint32_t dim2_key = 1; dim2_key < Dim2; ++dim2_key) {
                auto item = m_sparse_matrix.getItem(offset + dim2_key - 1);
                if (!item.isSet()) {
                    return dim2_key;
                }
            }
            return {}; // all keys assigned
        }
    }

    template <typename ItemT, unsigned int Dim2, typename PtrT>
    ItemT &VLimitedMatrix<ItemT, Dim2, PtrT>::modifyItem(std::pair<std::uint32_t, std::uint32_t> index)
    {
        assert(index.second < Dim2 && "Dimension 2 index out of range");
        if (index.first >= m_dim1.size()) {
            THROWF(db0::InternalException)
                << "Index (" << index.first << "," << index.second << ") not found" << THROWF_END;
        }
        if (index.second) {
            auto it = m_index.find(o_dim2_index_item(index.first, 0));
            if (it == m_index.end()) {
                THROWF(db0::InternalException)
                    << "Index (" << index.first << "," << index.second << ") not found" << THROWF_END;
            }
            std::uint64_t offset = it->m_offset + index.second - 1;
            return m_sparse_matrix.modifyItem(offset).get();
        } else {
            return m_dim1.modifyItem(index.first).get();
        }
    }
    
    template <typename ItemT, unsigned int Dim2, typename PtrT>
    typename VLimitedMatrix<ItemT, Dim2, PtrT>::const_iterator VLimitedMatrix<ItemT, Dim2, PtrT>::cbegin() const {
        return const_iterator(*this, false);
    }

    template <typename ItemT, unsigned int Dim2, typename PtrT>
    typename VLimitedMatrix<ItemT, Dim2, PtrT>::const_iterator VLimitedMatrix<ItemT, Dim2, PtrT>::cend() const {
        return const_iterator(*this, true);
    }

    template <typename ItemT, unsigned int Dim2, typename PtrT>
    typename VLimitedMatrix<ItemT, Dim2, PtrT>::column_iterator 
    VLimitedMatrix<ItemT, Dim2, PtrT>::beginColumn(const o_dim2_index_item &index_item) const
    {
        return column_iterator(
            index_item.m_key,
            m_sparse_matrix.cbegin() + index_item.m_offset,
            m_sparse_matrix.cbegin() + index_item.m_offset + (Dim2 - 1)
        );
    }

    template <typename ItemT, unsigned int Dim2, typename PtrT>
    typename VLimitedMatrix<ItemT, Dim2, PtrT>::column_iterator VLimitedMatrix<ItemT, Dim2, PtrT>::endColumn() const {
        return { 0, m_sparse_matrix.cend(), m_sparse_matrix.cend() };
    }

    template <typename ItemT, unsigned int Dim2, typename PtrT>
    std::size_t VLimitedMatrix<ItemT, Dim2, PtrT>::getItemCount() const
    {
        return (*this)->m_item_count;
    }
    
    template <typename ItemT, unsigned int Dim2, typename PtrT>
    VLimitedMatrix<ItemT, Dim2, PtrT>::const_iterator::const_iterator(const self_t &matrix, bool is_end)
        : m_matrix(matrix)        
        , m_it(is_end ? matrix.m_dim1.end() : matrix.m_dim1.begin())        
        , m_end(matrix.m_dim1.end())
        , m_index_it(is_end ? matrix.m_index.end() : matrix.m_index.begin())
        , m_index_end(matrix.m_index.end())
        , m_col_it(m_matrix.get().endColumn())
    {
        // advance to first set item
        if (!is_end) {
            fix();
        }
    }
    
    template <typename ItemT, unsigned int Dim2, typename PtrT>
    void VLimitedMatrix<ItemT, Dim2, PtrT>::const_iterator::fix()
    {
        while (m_it != m_end) {
            if (m_current_index.second == 0) {
                if (m_it->isSet()) {
                    return;
                } else {
                    if (m_index_it != m_index_end && m_index_it->m_key == m_current_index.first) {
                        // start iterating over the column
                        m_col_it = m_matrix.get().beginColumn(*m_index_it);
                        if (!m_col_it.isEnd()) {
                            m_current_index.second = m_col_it.m_current_index;
                            return;
                        }
                    }
                }
            } else {
                // continue iterating over the column
                m_col_it.fix();
                if (!m_col_it.isEnd()) {
                    m_current_index.second = m_col_it.m_current_index;
                    return;
                } else {
                    ++m_index_it;
                }
            }
            ++m_it;
            ++m_current_index.first;
            m_current_index.second = 0;
        }
    }

    template <typename ItemT, unsigned int Dim2, typename PtrT>
    bool VLimitedMatrix<ItemT, Dim2, PtrT>::const_iterator::operator!=(const const_iterator &other) const
    {
        return m_it != other.m_it;
    }
    
    template <typename ItemT, unsigned int Dim2, typename PtrT>
    std::pair<std::uint32_t, std::uint32_t> VLimitedMatrix<ItemT, Dim2, PtrT>::const_iterator::loc() const
    {
        return m_current_index;
    }
    
    template <typename ItemT, unsigned int Dim2, typename PtrT>
    typename VLimitedMatrix<ItemT, Dim2, PtrT>::const_iterator &VLimitedMatrix<ItemT, Dim2, PtrT>::const_iterator::operator++()
    {
        if (m_current_index.second == 0) {
            if (m_index_it != m_index_end && m_index_it->m_key == m_current_index.first) {
                // start iterating over the column
                m_col_it = m_matrix.get().beginColumn(*m_index_it);
                m_current_index.second = m_col_it.m_current_index;
            } else {
                ++m_current_index.first;
                ++m_it;
            }
        } else {
            // continue iterating over the column
            ++m_col_it;
            ++m_current_index.second;            
        }
        fix();

        return *this;
    }
    
    template <typename ItemT, unsigned int Dim2, typename PtrT>
    ItemT VLimitedMatrix<ItemT, Dim2, PtrT>::const_iterator::operator*() const
    {        
        if (m_current_index.second == 0) {
            return m_it->m_value;
        } else {
            return *m_col_it;
        }
    }

    template <typename ItemT, unsigned int Dim2, typename PtrT>
    VLimitedMatrix<ItemT, Dim2, PtrT>::column_iterator::column_iterator(
        std::uint32_t column_id, const IteratorT &begin, const IteratorT &end)
        : m_column_id(column_id)
        , m_it(begin)
        , m_end(end)
    {
        fix();
    }

    template <typename ItemT, unsigned int Dim2, typename PtrT>
    bool VLimitedMatrix<ItemT, Dim2, PtrT>::column_iterator::column_iterator::isEnd() const {
        return m_it == m_end;        
    }

    template <typename ItemT, unsigned int Dim2, typename PtrT>
    void VLimitedMatrix<ItemT, Dim2, PtrT>::column_iterator::fix()
    {
        while (m_it != m_end) {
            if (m_it->isSet()) {
                return;
            }
            ++m_it;
            ++m_current_index;
        }
    }

    template <typename ItemT, unsigned int Dim2, typename PtrT>
    typename VLimitedMatrix<ItemT, Dim2, PtrT>::column_iterator &VLimitedMatrix<ItemT, Dim2, PtrT>::column_iterator::operator++()
    {
        assert(!isEnd() && "Incrementing past the end of the iterator");
        ++m_it;
        ++m_current_index;
        fix();
        return *this;
    }

    template <typename ItemT, unsigned int Dim2, typename PtrT>
    ItemT VLimitedMatrix<ItemT, Dim2, PtrT>::column_iterator::operator*() const
    {
        assert(!isEnd() && "Dereferencing end iterator");
        return m_it->m_value;
    }

}
