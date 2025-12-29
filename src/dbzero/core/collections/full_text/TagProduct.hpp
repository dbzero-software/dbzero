// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "TP_Vector.hpp"
#include "FT_Iterator.hpp"

namespace db0

{
    
    // Join product dependent on join type
    template <typename key_t> class JoinProduct;

    // The tag-product operator combines a result iterator (yielding objects)
    // Its architecture allows multiple tag-products to be stacked together
    // with the tag iterator (yielding tags). It generates a composite key consiting of object(s) + tag
    // with tag assumed as the unique identifier
    template <typename key_t = std::uint64_t>
    class TagProduct
    {
    public:
        using KeyT = const key_t*;
        using KeyStorageT = TP_Vector<key_t>;
        using FT_IteratorT = FT_Iterator<key_t>;
        // the tag-inverted index factory
        // @return nullptr if the tag does not have an associated index
        using tag_factory_func = std::function<std::unique_ptr<FT_IteratorT>(key_t, int direction)>;

        /**
         * @param object_sets - the iterators yielding collections of objects (e.g. of a specific type)
         * @param tags - the iterator yielding all requested tags
         * @param tag_func - the factory function producing an inverted index (i.e. objects for a given tag)
         */
        TagProduct(std::vector<std::unique_ptr<FT_IteratorT> > &&object_sets, std::unique_ptr<FT_IteratorT> &&tags,
            tag_factory_func tag_func);
        TagProduct(const std::vector<std::unique_ptr<FT_IteratorT> > &object_sets, const FT_IteratorT &tags, 
            tag_factory_func tag_func);
        ~TagProduct();

		bool isEnd() const;
        
        void next(KeyStorageT * = nullptr);
        
		bool join(KeyT);
        
        // start the iteration over
        std::unique_ptr<TagProduct<key_t> > begin() const;
        
        // Get the number of underlying joined object streams
        std::size_t getDimension() const;
        
    protected:        
        tag_factory_func m_tag_func;
        std::vector<std::unique_ptr<FT_IteratorT> > m_object_sets;
        std::unique_ptr<FT_IteratorT> m_tags;
        key_t m_current_tag;
        std::unique_ptr<JoinProduct<key_t> > m_join_product;
        
        void initNextTag();
    };

    extern template class TagProduct<UniqueAddress>;
    extern template class TagProduct<std::uint64_t>;
    
}