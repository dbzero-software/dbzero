// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "TagProduct.hpp"
#include "FT_ANDIterator.hpp"

namespace db0

{
    
    template <typename key_t>
    class JoinProduct
    {
    public:
        using KeyStorageT = TP_Vector<key_t>;
        using FT_IteratorT = FT_Iterator<key_t>;

        JoinProduct();

        bool isEnd() const;
        void next(KeyStorageT *);
        
        void add(std::unique_ptr<FT_IteratorT> &&it1, std::unique_ptr<FT_IteratorT> &&it2);

    private:
        std::vector<key_t> m_keys;
        // position + size
        std::vector<std::pair<std::size_t, std::size_t> > m_ranges;
        // iterator positions
        std::vector<std::size_t> m_current_positions;
        bool m_end = true;
    };
    
    template <typename key_t>
    JoinProduct<key_t>::JoinProduct()
    {        
    }

    template <typename key_t>
    bool JoinProduct<key_t>::isEnd() const
    {
        return m_end;
    }

    template <typename key_t>
    void JoinProduct<key_t>::add(std::unique_ptr<FT_IteratorT> &&it1, std::unique_ptr<FT_IteratorT> &&it2)
    {
        if (m_ranges.empty()) {
            m_end = false;
        }
        FT_JoinANDIterator<key_t> joined(std::move(it1), std::move(it2));
        key_t key;
        auto pos = m_keys.size();
        while (!joined.isEnd()) {
            joined.next(&key);
            m_keys.push_back(key);
        }
        m_ranges.emplace_back(pos, m_keys.size() - pos);
        m_current_positions.push_back(0);
        m_end = m_end || ((m_keys.size() - pos) == 0);
    }

    template <typename key_t>
    void JoinProduct<key_t>::next(KeyStorageT *key)
    {
        assert(!isEnd());
        if (key && key->size() != m_current_positions.size()) {
            key->resize(m_current_positions.size());
        }
        
        unsigned int at = 0;
        auto range = m_ranges.begin();
        bool carry = true;
        for (auto &pos: m_current_positions) {
            if (key) {
                (*key)[at] = m_keys[range->first + pos];
            }
            if (carry) {
                ++pos;
                if (pos == range->second) {
                    pos = 0;
                } else {
                    carry = false;
                }
            }
            ++at;
            ++range;
        }
        m_end = carry;
    }
    
    template <typename key_t>
    TagProduct<key_t>::TagProduct(std::vector<std::unique_ptr<FT_IteratorT> > &&object_sets, std::unique_ptr<FT_IteratorT> &&tags,
        tag_factory_func tag_func)
        : m_tag_func(tag_func)
        , m_object_sets(std::move(object_sets))
        , m_tags(std::move(tags))        
    {
        initNextTag();
    }

    template <typename key_t>
    TagProduct<key_t>::~TagProduct()
    {        
    }

    template <typename key_t>
    void TagProduct<key_t>::initNextTag()
    {
        while (m_tags && !m_tags->isEnd()) {
            m_tags->getKey(m_current_tag);
            auto tag_index = m_tag_func(m_current_tag, -1);
            if (tag_index) {
                JoinProduct<key_t> join_product;
                for (auto &objects: m_object_sets) {
                    join_product.add(objects->beginTyped(-1), tag_index->beginTyped(-1));
                }
                // NOTE: in common case where join product size = 1 we can
                // optimize it by skipping the join product object's creation
                if (!join_product.isEnd()) {
                    m_join_product = std::make_unique<JoinProduct<key_t> >(std::move(join_product));
                    return;
                }
            }
            m_tags->next();
        }
        m_join_product = nullptr;
    }
    
    template <typename key_t>
    void TagProduct<key_t>::next(KeyStorageT *key)
    {
        assert(!isEnd());
        m_join_product->next(key);
        if (m_join_product->isEnd()) {
            m_tags->next();
            initNextTag();
        }        
    }
    
    template <typename key_t>
    bool TagProduct<key_t>::isEnd() const
    {
        return !m_join_product;
    }
    
    template <typename key_t>
    bool TagProduct<key_t>::join(KeyT) {
        throw std::runtime_error("Not implemented");
    }

    template <typename key_t>
    std::unique_ptr<TagProduct<key_t> > TagProduct<key_t>::begin() const 
    {
        std::vector<std::unique_ptr<FT_IteratorT> > object_sets;
        for (const auto &it: m_object_sets) {
            if (it) {
                object_sets.push_back(it->beginTyped(-1));
            } else {
                object_sets.push_back(nullptr);
            }
        }
        std::unique_ptr<FT_IteratorT> tags;
        if (m_tags) {
            tags = m_tags->beginTyped(-1);
        }
        return std::make_unique<TagProduct<key_t> >(std::move(object_sets), std::move(tags), m_tag_func);
    }
    
    template <typename key_t>
    std::size_t TagProduct<key_t>::getDimension() const {
        return m_object_sets.size();
    }

    template class TagProduct<std::uint64_t>;
    template class TagProduct<db0::UniqueAddress>;
    
}   
