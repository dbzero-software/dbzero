// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once
    
#include <vector>
#include "VLimitedMatrix.hpp"
#include "LimitedMatrix.hpp"
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
    class LimitedMatrixCache: protected LimitedMatrix<ItemT>
    {
    public:
        using self_type = LimitedMatrixCache<MatrixT, ItemT, AdapterT>;
        using super_t = LimitedMatrix<ItemT>;
        using CallbackType = std::function<void(const ItemT &)>;

        // callback will be notified on each new item added either during initial load or refresh
        LimitedMatrixCache(const MatrixT &, AdapterT = {}, CallbackType = {});
        
        std::size_t size() const {
            return super_t::size();
        }
        
        // NOTE: Cache might be refreshed if item not found at first attempt
        // @return nullptr if item not set / not found
        const ItemT *tryGet(std::pair<std::uint32_t, std::uint32_t>) const;
        
        // Fetch appended items only (updates or deletions not reflected)
        // callback will be notified on each new item added
        bool refresh();
        // Reload / refresh a specific existing item only
        void reload(std::pair<std::uint32_t, std::uint32_t>);
        
        typename super_t::const_iterator cbegin() const {
            return super_t::begin();
        }

        typename super_t::const_iterator cend() const {
            return super_t::end();
        }

    private:        
        std::reference_wrapper<const MatrixT> m_matrix;
        AdapterT m_adapter;
        CallbackType m_callback;
    };
    
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
            this->set(it.loc(), std::move(item));
        }
    }

    template <typename MatrixT, typename ItemT, typename AdapterT>
    const ItemT *LimitedMatrixCache<MatrixT, ItemT, AdapterT>::tryGet(std::pair<std::uint32_t, std::uint32_t> pos) const
    {
        auto item_ptr = super_t::tryGet(pos);
        if (!item_ptr) {
            // try refreshing the cache
            if (const_cast<self_type *>(this)->refresh()) {
                item_ptr = super_t::tryGet(pos);
            }
        }
        return item_ptr;
    }
    
    template <typename MatrixT, typename ItemT, typename AdapterT>
    bool LimitedMatrixCache<MatrixT, ItemT, AdapterT>::refresh()
    {
        // prevent refreshing if item count did not change
        if (this->size() == this->m_matrix.get().getItemCount()) {
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
        
}
