// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "mb_index_def.hpp"

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