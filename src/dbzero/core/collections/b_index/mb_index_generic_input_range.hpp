// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#pragma once

#include "mb_index_def.hpp"
#include <cstring>

namespace db0::bindex

{

    template<typename IteratorT, typename DefinitionT>
    class GenericInputRange : public DefinitionT::Containers::IInputRange
    {
        using empty_t = typename DefinitionT::empty_t;
        using itty_index_t = typename DefinitionT::itty_index_t;
        using array2_t = typename DefinitionT::array2_t;
        using array3_t = typename DefinitionT::array3_t;
        using array4_t = typename DefinitionT::array4_t;
        using vector_t = typename DefinitionT::vector_t;
        using bindex_t = typename DefinitionT::bindex_t;
        using CallbackT = typename DefinitionT::CallbackT;
        using HeteromorphicResolverT = typename DefinitionT::HeteromorphicResolverT;

        IteratorT m_first, m_last;

        template<typename IndexContainer>
        std::size_t indexCountNew(const IndexContainer &index, std::size_t max_count) 
        {
            std::size_t result = 0;
            auto index_end = index.end();
            for(auto it = m_first; it != m_last; ++it) {
                if(result == max_count) {
                    break;
                }
                if(index.find(*it) == index_end) {
                    ++result;
                }
            }
            return result;
        }

        template<typename IndexContainer>
        std::size_t indexCountExisting(const IndexContainer &index, std::size_t max_count)
        {
            std::size_t result = 0;
            auto index_end = index.end();
            for (auto it = m_first; it != m_last; ++it) {
                if (result == max_count) {
                    break;
                }
                if (index.find(*it) != index_end) {
                    ++result;
                }
            }
            return result;
        }

        std::pair<std::uint32_t, std::uint32_t> arrayInsert() 
        {
            if (m_first != m_last) {
                THROWF(db0::InternalException)
                    << "Insert not supported in immutable container";
            }
            return std::make_pair(0, 0);
        }

        std::size_t arrayErase() 
        {
            if (m_first != m_last) {
                THROWF(db0::InternalException)
                    << "Erase not supported in immutable container";
            }
            return 0;
        }

        template<typename IndexContainer>
        std::pair<std::uint32_t, std::uint32_t> indexInsertHeteromorphic(IndexContainer &index,
            HeteromorphicResolverT *resolver_ptr)
        {
            using item_t = typename DefinitionT::item_t;
            std::pair<std::uint32_t, std::uint32_t> result(0, 0);
            for (auto it = m_first; it != m_last; ++it) {
                item_t incoming = *it;
                ++result.first;
                item_t old_value;
                if (index.updateExisting(incoming, &old_value)) {
                    if (resolver_ptr && std::memcmp(&old_value, &incoming, sizeof(item_t)) != 0) {
                        item_t resolved;
                        if ((*resolver_ptr)(old_value, incoming, resolved)) {
                            index.updateExisting(resolved, nullptr);
                        } else {
                            index.updateExisting(old_value, nullptr);
                        }
                    }
                    continue;
                }
                THROWF(db0::InternalException)
                    << "Insert not supported in immutable container";
            }
            return result;
        }

    public:
        GenericInputRange(const IteratorT &first, const IteratorT &last)
            : m_first(first)
            , m_last(last)
        {}
    
        virtual ~GenericInputRange() = default;

        // bindex_t
        virtual std::pair<std::uint32_t, std::uint32_t> insert(bindex_t &index, CallbackT *callback_ptr) override {
            return index.bulkInsertUnique(m_first, m_last, callback_ptr);
        }

        virtual std::pair<std::uint32_t, std::uint32_t> insertHeteromorphic(
            bindex_t &index, CallbackT *callback_ptr, HeteromorphicResolverT *resolver_ptr) override
        {
            return index.bulkInsertUniqueHeteromorphic(m_first, m_last, callback_ptr, resolver_ptr);
        }

        virtual std::size_t erase(bindex_t &index, CallbackT *callback_ptr) override
        {
            using item_t = typename DefinitionT::item_t;
            return index.bulkErase(m_first, m_last, (const item_t*)nullptr, callback_ptr);
        }

        virtual std::size_t countNew(const bindex_t &index, std::size_t max_count) override {
            return indexCountNew(index, max_count);
        }

        virtual std::size_t countExisting(const bindex_t &index, std::size_t max_count) override {
            return indexCountExisting(index, max_count);
        }

        // vector_t
        virtual std::pair<std::uint32_t, std::uint32_t> insert(vector_t &index, CallbackT *callback_ptr) override {
            std::pair<std::uint32_t, std::uint32_t> result;
            index.bulkInsertUnique(m_first, m_last, &result, callback_ptr);
            return result;
        }

        virtual std::pair<std::uint32_t, std::uint32_t> insertHeteromorphic(
            vector_t &index, CallbackT *callback_ptr, HeteromorphicResolverT *resolver_ptr) override
        {
            std::pair<std::uint32_t, std::uint32_t> result;
            index.bulkInsertUniqueHeteromorphic(m_first, m_last, &result, callback_ptr, resolver_ptr);
            return result;
        }

        virtual std::size_t erase(vector_t &index, CallbackT *callback_ptr) override {
            return index.bulkErase(m_first, m_last, callback_ptr);
        }

        virtual std::size_t countNew(const vector_t &index, std::size_t max_count) override {
            return indexCountNew(index, max_count);
        }

        virtual std::size_t countExisting(const vector_t &index, std::size_t max_count) override {
            return indexCountExisting(index, max_count);
        }

        // array4_t
        virtual std::pair<std::uint32_t, std::uint32_t> insert(array4_t&, CallbackT *) override {
            return arrayInsert();
        }

        virtual std::pair<std::uint32_t, std::uint32_t> insertHeteromorphic(
            array4_t &index, CallbackT *, HeteromorphicResolverT *resolver_ptr) override
        {
            return indexInsertHeteromorphic(index, resolver_ptr);
        }

        virtual std::size_t erase(array4_t&, CallbackT *) override {
            return arrayErase();
        }

        virtual std::size_t countNew(const array4_t &index, std::size_t max_count) override {
            return indexCountNew(index, max_count);
        }

        virtual std::size_t countExisting(const array4_t &index, std::size_t max_count) override {
            return indexCountExisting(index, max_count);
        }

        // array3_t
        virtual std::pair<std::uint32_t, std::uint32_t> insert(array3_t&, CallbackT *) override {
            return arrayInsert();
        }

        virtual std::pair<std::uint32_t, std::uint32_t> insertHeteromorphic(
            array3_t &index, CallbackT *, HeteromorphicResolverT *resolver_ptr) override
        {
            return indexInsertHeteromorphic(index, resolver_ptr);
        }

        virtual std::size_t erase(array3_t&, CallbackT *) override {
            return arrayErase();
        }

        virtual std::size_t countNew(const array3_t &index, std::size_t max_count) override {
            return indexCountNew(index, max_count);
        }

        virtual std::size_t countExisting(const array3_t &index, std::size_t max_count) override {
            return indexCountExisting(index, max_count);
        }

        // array2_t
        virtual std::pair<std::uint32_t, std::uint32_t> insert(array2_t&, CallbackT *) override {
            return arrayInsert();
        }

        virtual std::pair<std::uint32_t, std::uint32_t> insertHeteromorphic(
            array2_t &index, CallbackT *, HeteromorphicResolverT *resolver_ptr) override
        {
            return indexInsertHeteromorphic(index, resolver_ptr);
        }

        virtual std::size_t erase(array2_t&, CallbackT *) override {
            return arrayErase();
        }

        virtual std::size_t countNew(const array2_t &index, std::size_t max_count) override {
            return indexCountNew(index, max_count);
        }

        virtual std::size_t countExisting(const array2_t &index, std::size_t max_count) override {
            return indexCountExisting(index, max_count);
        }

        // itty_index_t
        virtual std::pair<std::uint32_t, std::uint32_t> insert(itty_index_t&, CallbackT *) override {
            return arrayInsert();
        }

        virtual std::pair<std::uint32_t, std::uint32_t> insertHeteromorphic(
            itty_index_t &index, CallbackT *, HeteromorphicResolverT *resolver_ptr) override
        {
            return indexInsertHeteromorphic(index, resolver_ptr);
        }

        virtual std::size_t erase(itty_index_t&, CallbackT *) override {
            return arrayErase();
        }

        virtual std::size_t countNew(const itty_index_t &index, std::size_t max_count) override 
        {
            std::size_t result = 0;
            auto value = index.getValue();
            for(auto it = m_first; it != m_last; ++it) {
                if(result == max_count) {
                    break;
                }
                if(*it != value) {
                    ++result;
                }
            }
            return result;
        }

        virtual std::size_t countExisting(const itty_index_t &index, std::size_t max_count) override 
        {
            std::size_t result = 0;
            auto value = index.getValue();
            for(auto it = m_first; it != m_last; ++it) {
                if(result == max_count) {
                    break;
                }
                if(*it == value) {
                    ++result;
                }
            }
            return result;
        }

        // empty_t
        virtual std::pair<std::uint32_t, std::uint32_t> insert(empty_t&, CallbackT *) override {
            return arrayInsert();
        }

        virtual std::pair<std::uint32_t, std::uint32_t> insertHeteromorphic(
            empty_t &index, CallbackT *callback_ptr, HeteromorphicResolverT *) override
        {
            return insert(index, callback_ptr);
        }

        virtual std::size_t erase(empty_t&, CallbackT *) override {
            return arrayErase();
        }

        virtual std::size_t countNew(const empty_t&, std::size_t max_count) override {
            return std::min((std::size_t)std::distance(m_first, m_last), max_count);
        }

        virtual std::size_t countExisting(const empty_t&, std::size_t) override {
            return 0;
        }
    
    };

}
