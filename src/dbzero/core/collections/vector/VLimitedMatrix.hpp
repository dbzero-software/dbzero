#pragma once

#include "v_bvector.hpp"
#include "v_sorted_vector.hpp"
#include <dbzero/core/memory/Address.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/core/memory/Memspace.hpp>
#include <dbzero/core/serialization/optional_item.hpp>

namespace db0

{
    
    template <typename PtrT>
    struct [[gnu::packed]] o_limited_matrix: public o_fixed<o_limited_matrix<PtrT> >
    {
        // Points to v_bvector representing the entire Dimension 1
        PtrT m_dim1_ptr = {};
        // Points to v_sorted_vector holding the index of Dimension 2 values
        PtrT m_index_ptr = {};
        // Points to v_bvector holding the sparse matrix with the exclusion of Dim1[0] vector
        PtrT m_sparse_matrix_ptr = {};
    };
    
    struct [[gnu::packed]] o_dim2_index_item: public o_fixed<o_dim2_index_item>
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

    // The LimitedMatrix type is a type optimized for representing matrices
    // with the following properties / constraints:
    //  - Dimension 1 holds values primarily consecutively (e.g. rows in a row-major matrix)
    //  - Dimension 2 is limited to a known, small number of values
    //  - The most common values in Dimension 2 are "0"
    //  - Elements are frequently appended but rarely removed
    //  - O(1) access time is required
    // @tparam ItemT - contained item type (fixed size, no pointers)
    // @tparam PtrT - type for inner pointers (V-Space)
    // @tparam Dim2Size - number of bits representing Dimension 2 (max 2^Dim2Pow - 1)
    template <typename ItemT, typename PtrT = Address, unsigned int Dim2Pow = 6> class VLimitedMatrix
        : public v_object<o_limited_matrix<PtrT> >
    {
    public:
        using super_t = v_object<o_limited_matrix<PtrT> >;
        VLimitedMatrix(Memspace &);
        VLimitedMatrix(mptr);

        // add new or update already existing item
        void set(std::pair<std::uint32_t, std::uint32_t> index, const ItemT &);
        
        // get an item if exists
        std::optional<ItemT> tryGet(std::pair<std::uint32_t, std::uint32_t> index) const;
        // get or raise if not exists
        ItemT get(std::pair<std::uint32_t, std::uint32_t> index) const;

        // @return Dim1 size
        std::size_t size() const;

        static constexpr std::size_t maxDim2Size() {
            return (1u << Dim2Pow) - 1;
        }

        // push back to Dimension 1
        void push_back(const ItemT &, std::uint32_t dim2_key = 0);

        void detach() const;
        void commit() const;

    private:
        v_bvector<o_optional_item<ItemT>, PtrT> m_dim1;
        v_sorted_vector<o_dim2_index_item, PtrT> m_index;
        v_bvector<o_optional_item<ItemT>, PtrT> m_sparse_matrix;
    };
    
    template <typename ItemT, typename PtrT, unsigned int Dim2Pow>
    VLimitedMatrix<ItemT, PtrT, Dim2Pow>::VLimitedMatrix(Memspace &memspace)
        : v_object<o_limited_matrix<PtrT> >(memspace)
        , m_dim1(memspace)
        , m_index(memspace)
        , m_sparse_matrix(memspace)
    {
        this->modify().m_dim1_ptr = m_dim1.getAddress();
        this->modify().m_index_ptr = m_index.getAddress();
        this->modify().m_sparse_matrix_ptr = m_sparse_matrix.getAddress();
    }

    template <typename ItemT, typename PtrT, unsigned int Dim2Pow>
    VLimitedMatrix<ItemT, PtrT, Dim2Pow>::VLimitedMatrix(mptr ptr)
        : v_object<o_limited_matrix<PtrT> >(ptr)
        , m_dim1(this->myPtr((*this)->m_dim1_ptr))
        , m_index(this->myPtr((*this)->m_index_ptr))
        , m_sparse_matrix(this->myPtr((*this)->m_sparse_matrix_ptr))
    {
    }

    template <typename ItemT, typename PtrT, unsigned int Dim2Pow>
    std::size_t VLimitedMatrix<ItemT, PtrT, Dim2Pow>::size() const
    {
        return m_dim1.size();
    }

    template <typename ItemT, typename PtrT, unsigned int Dim2Pow>
    void VLimitedMatrix<ItemT, PtrT, Dim2Pow>::detach() const
    {
        m_dim1.detach();
        m_index.detach();
        m_sparse_matrix.detach();
        super_t::detach();
    }
    
    template <typename ItemT, typename PtrT, unsigned int Dim2Pow>
    void VLimitedMatrix<ItemT, PtrT, Dim2Pow>::commit() const
    {
        m_dim1.commit();
        m_index.commit();
        m_sparse_matrix.commit();
        super_t::commit();
    }
    
    template <typename ItemT, typename PtrT, unsigned int Dim2Pow>
    void VLimitedMatrix<ItemT, PtrT, Dim2Pow>::push_back(const ItemT &value, std::uint32_t dim2_key)
    {
        assert(dim2_key < maxDim2Size() && "Dimension 2 key out of range");
        if (dim2_key) {
            std::uint32_t key_0 = m_dim1.size();
            m_dim1.emplace_back();
            std::uint32_t offset = m_sparse_matrix.size();
            bool addr_changed = false;
            m_index.insert(o_dim2_index_item(key_0, offset), addr_changed);
            if (addr_changed) {
                this->modify().m_index_ptr = m_index.getAddress();
            }
            m_sparse_matrix.growBy(this->maxDim2Size() - 1);
            m_sparse_matrix.setItem(offset + dim2_key - 1, value);
        } else {
            m_dim1.push_back(value);
        }
    }

    template <typename ItemT, typename PtrT, unsigned int Dim2Pow>
    std::optional<ItemT> VLimitedMatrix<ItemT, PtrT, Dim2Pow>::tryGet(std::pair<std::uint32_t, std::uint32_t> index) const
    {
        assert(index.second < maxDim2Size() && "Dimension 2 index out of range");
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
    
    template <typename ItemT, typename PtrT, unsigned int Dim2Pow>
    ItemT VLimitedMatrix<ItemT, PtrT, Dim2Pow>::get(std::pair<std::uint32_t, std::uint32_t> index) const
    {
        auto result = tryGet(index);
        if (result) {
            return *result;
        } else {
            THROWF(db0::InternalException)
                << "Index (" << index.first << "," << index.second << ") not found" << THROWF_END;
        }
    }
    
    template <typename ItemT, typename PtrT, unsigned int Dim2Pow>
    void VLimitedMatrix<ItemT, PtrT, Dim2Pow>::set(std::pair<std::uint32_t, std::uint32_t> index, const ItemT &value)
    {
        assert(index.second < maxDim2Size() && "Dimension 2 index out of range");
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
                m_sparse_matrix.growBy(this->maxDim2Size() - 1);
                m_sparse_matrix.setItem(offset + index.second - 1, value);
            } else {
                std::uint64_t offset = it->m_offset + index.second - 1;
                m_sparse_matrix.setItem(offset, value);
            }
        } else {
            m_dim1.setItem(index.first, value);
        }
    }

}
