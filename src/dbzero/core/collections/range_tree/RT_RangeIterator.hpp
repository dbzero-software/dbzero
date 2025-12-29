// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <optional>
#include "RangeTree.hpp"
#include "IndexBase.hpp"
#include <dbzero/core/collections/full_text/FT_IteratorBase.hpp>
#include <dbzero/core/collections/full_text/FT_Iterator.hpp>
#include <dbzero/core/serialization/Serializable.hpp>
#include <dbzero/core/serialization/hash.hpp>

namespace db0

{

    /**
     * The RT_RangeIterator can be used to apply a range filter on top of a standard FT_Iterator
     * The RT_RangeIterator implements the FT_IteratorBase interface only so it can't be joined with other iterators
     * @tparam KeyT type of the RangeTree key
     * @tparam ValueT type of the RangeTree value
    */
    template <typename KeyT, typename ValueT> class RT_RangeIterator: public FT_IteratorBase
    {
        using RT_TreeT = RangeTree<KeyT, ValueT>;
        using self_t = RT_RangeIterator<KeyT, ValueT>;
    public:
        // Create to range-filter results of a specific FT-iterator (e.g. tag query)
        RT_RangeIterator(const IndexBase &index, SharedPtrWrapper<RT_TreeT> tree_ptr, std::unique_ptr<FT_Iterator<ValueT> > &&it, 
            std::optional<KeyT> min, bool min_inclusive, std::optional<KeyT> max, bool max_inclusive, bool null_first)
            : RT_RangeIterator(index, this->nextUID(), tree_ptr, true, std::move(it), min, min_inclusive, max, max_inclusive, null_first)
        {
        }
        
        // Create range-only filter
        RT_RangeIterator(const IndexBase &index, SharedPtrWrapper<RT_TreeT> tree_ptr, std::optional<KeyT> min = {},
            bool min_inclusive = false, std::optional<KeyT> max = {}, bool max_inclusive = false, bool null_first = false)
            : RT_RangeIterator(index, this->nextUID(), tree_ptr, false, nullptr, min, min_inclusive, max, max_inclusive, null_first)
        {
        }

		bool isEnd() const override;

        void next(void *buf = nullptr) override;

        const std::type_info &keyTypeId() const override;

        const std::type_info &typeId() const override;
        
        std::ostream &dump(std::ostream &os) const override;

        std::unique_ptr<FT_IteratorBase> begin() const override;
        
        const FT_IteratorBase *find(std::uint64_t uid) const override;
        
        void getSignature(std::vector<std::byte> &) const override;
        
    protected:
        double compareToImpl(const FT_IteratorBase &it) const override;

        double compareTo(const RT_RangeIterator &it) const;
        
    private:
        using ItemT = typename RT_TreeT::ItemT;
        using RT_IteratorT = typename RT_TreeT::BlockT::FT_IteratorT;
        using RangeIterator = typename RT_TreeT::RangeIterator;

        IndexBase m_index;
        SharedPtrWrapper<RT_TreeT> m_tree_ptr;
        RangeIterator m_tree_it;
        const bool m_has_query;
        std::unique_ptr<FT_Iterator<ValueT> > m_query_it;
        // the iterator over null keys only
        std::unique_ptr<FT_Iterator<ValueT> > m_null_query_it;
        std::optional<KeyT> m_min;
        const bool m_min_inclusive;
        std::optional<KeyT> m_max;
        const bool m_max_inclusive;
        const bool m_null_first;

        // Create to range-filter results of a specific FT-iterator (e.g. tag query)
        RT_RangeIterator(const IndexBase &index, std::uint64_t uid, SharedPtrWrapper<RT_TreeT> tree_ptr, bool has_query, 
            std::unique_ptr<FT_Iterator<ValueT> > &&it, std::optional<KeyT> min, bool min_inclusive, std::optional<KeyT> max, 
            bool max_inclusive, bool null_first)
            : FT_IteratorBase(uid)
            , m_index(index)
            , m_tree_ptr(tree_ptr)
            , m_tree_it([&]() -> RangeIterator { if (tree_ptr) { if (min) return tree_ptr->lowerBound(*min, min_inclusive); else return tree_ptr->beginRange(); } else return RangeIterator(true); }())
            , m_has_query(has_query)
            , m_query_it(std::move(it))
            , m_min(min)
            , m_min_inclusive(min_inclusive)
            , m_max(max)
            , m_max_inclusive(max_inclusive)
            , m_null_first(null_first)
        {
            // check if also include nulls in the result
            if (inRange()) {
                m_null_query_it = beginNullBlockQuery();
            }
            if (null_first) {
                if (!initNullsQuery()) {
                    initRangeQuery();
                }
            } else {
                if (!initRangeQuery()) {
                    initNullsQuery();
                }
            }

            if (m_range_it || m_null_it) {
                m_has_lh_item = _next(m_lh_item);
            }
        }
        
        bool initNullsQuery()
        {
            if (m_null_query_it) {
                m_null_it = std::move(m_null_query_it);
                m_null_query_it = nullptr;
                if (m_null_it && m_null_it->isEnd()) {
                    m_null_it = nullptr;
                }
            }
            return m_null_it != nullptr;
        }

        bool initRangeQuery()
        {
            if (m_tree_it.isEnd()) {
                return false;
            }
            m_range_it = m_tree_it->makeIterator();
            m_native_it_ptr = reinterpret_cast<RT_IteratorT *>(m_range_it.get());
            assert(!m_range_it->isEnd());
            return true;            
        }

        // check if a null value fits into the requested range
        bool inRange() const {
            return (m_null_first && !m_min) || (!m_null_first && !m_max);
        }

        inline bool inRange(const KeyT &key) const {
            return (!m_min || (m_min_inclusive ? key >= *m_min : key > *m_min)) &&
                (!m_max || (m_max_inclusive ? key <= *m_max : key < *m_max));
        }
        
        // the range currently being iterated over
        std::unique_ptr<FT_IteratorBase> m_range_it;
        // the underlying native iterator (valid when m_range_it is not null)
        RT_IteratorT *m_native_it_ptr = nullptr;
        // the look-ahead item
        bool m_has_lh_item = false;
        ItemT m_lh_item;
        // null-key block iterator
        std::unique_ptr<FT_Iterator<ValueT> > m_null_it;
        
        bool _next(ItemT &next_item);

        std::unique_ptr<FT_Iterator<ValueT> > beginNullBlockQuery() const;
    };
    
    template <typename KeyT, typename ValueT> const std::type_info &RT_RangeIterator<KeyT, ValueT>::keyTypeId() const
    {
        // note that ValueT is the actual full-text iteration key
        return typeid(ValueT);
    }

    template <typename KeyT, typename ValueT> const std::type_info &RT_RangeIterator<KeyT, ValueT>::typeId() const {
        return typeid(self_t);
    }

    template <typename KeyT, typename ValueT> std::ostream &RT_RangeIterator<KeyT, ValueT>::dump(std::ostream &os) const {
        return os << "RT_RangeIterator";
    }
    
    template <typename KeyT, typename ValueT> bool RT_RangeIterator<KeyT, ValueT>::isEnd() const {
        return !m_has_lh_item;
    }
    
    template <typename KeyT, typename ValueT>
    const FT_IteratorBase *RT_RangeIterator<KeyT, ValueT>::find(std::uint64_t uid) const
    {
        if (this->m_uid == uid) {
            return this;
        }
        if (m_query_it) {
            return m_query_it->find(uid);
        }
        return nullptr;
    }

    template <typename KeyT, typename ValueT>
    void RT_RangeIterator<KeyT, ValueT>::next(void *buf)
    {
        assert(m_has_lh_item);
        if (buf) {
            *reinterpret_cast<ValueT *>(buf) = m_lh_item.m_value;
        }
        m_has_lh_item = _next(m_lh_item);
    }

    template <typename KeyT, typename ValueT>
    bool RT_RangeIterator<KeyT, ValueT>::_next(ItemT &item)
    {
        while (m_range_it) {
            assert(!m_range_it->isEnd());
            item = *m_native_it_ptr->asNative();
            m_range_it->next();
            if (m_range_it->isEnd()) {
                m_tree_it.next();
                if (m_tree_it.isEnd()) {
                    m_range_it = nullptr;
                    m_native_it_ptr = nullptr;
                    // initiate the null-block part if it exists
                    if (inRange() && !m_null_first) {
                        initNullsQuery();
                    }
                } else {
                    m_range_it = m_tree_it->makeIterator();
                    m_native_it_ptr = reinterpret_cast<RT_IteratorT *>(m_range_it.get());
                    assert(!m_range_it->isEnd());
                }
            }
            if (inRange(item.m_key)) {
                return true;
            }
        }
        
        // retrieve from the null-block part if it exists
        if (m_null_it) {
            assert(!m_null_it->isEnd());
            m_null_it->next(&item.m_value);
            if (m_null_it->isEnd()) {
                m_null_it = nullptr;
                if (m_null_first) {
                    initRangeQuery();
                }                
            }
            return true;
        }

        return false;
    }

    template <typename KeyT, typename ValueT>
    std::unique_ptr<FT_IteratorBase> RT_RangeIterator<KeyT, ValueT>::begin() const
    {        
        return std::unique_ptr<FT_IteratorBase>(new self_t(m_index, m_uid, m_tree_ptr, m_has_query, (m_query_it ? m_query_it->beginTyped() : nullptr),
            m_min, m_min_inclusive, m_max, m_max_inclusive, m_null_first));
    }
    
    template <typename KeyT, typename ValueT> std::unique_ptr<FT_Iterator<ValueT> >
    RT_RangeIterator<KeyT, ValueT>::beginNullBlockQuery() const
    {
        if (m_has_query && !m_query_it) {
            return nullptr;
        }

        // the underlying index is empty
        if (!m_tree_ptr) {
            return nullptr;
        }

        auto null_block = m_tree_ptr->getNullBlock();
        if (null_block) {
            FT_ANDIteratorFactory<ValueT> and_factory;
            if (m_has_query) {
                and_factory.add(m_query_it->beginTyped());
            }
            and_factory.add(null_block->makeIterator());
            auto result = and_factory.release(-1);
            if (!result->isEnd()) {
                return result;
            }
        }

        return nullptr;
    }
    
    template <typename KeyT, typename ValueT>
    double RT_RangeIterator<KeyT, ValueT>::compareToImpl(const FT_IteratorBase &it) const
    {
        if (it.typeId() == this->typeId()) {
            return compareTo(reinterpret_cast<const self_t &>(it));
        }
        return 1.0;
    }

    template <typename KeyT, typename ValueT>
    double RT_RangeIterator<KeyT, ValueT>::compareTo(const RT_RangeIterator &it) const
    {   
        double result = 0.0;             
        if (m_index.getAddress() != it.m_index.getAddress()) {
            return 1.0;
        } else if (m_has_query != it.m_has_query) {
            return 1.0;
        } else if (m_has_query) {
            result = m_query_it->compareTo(*it.m_query_it);
        }
        return result;
    }
    
    template <typename KeyT, typename ValueT>
    void RT_RangeIterator<KeyT, ValueT>::getSignature(std::vector<std::byte> &v) const
    {
        using TypeIdType = decltype(db0::serial::typeId<void>());

        // NOTE: acutal range is not affecting the signature
        std::vector<std::byte> bytes;
        db0::serial::write<TypeIdType>(bytes, db0::serial::typeId<KeyT>());
        db0::serial::write<TypeIdType>(bytes, db0::serial::typeId<ValueT>());
        db0::serial::write<std::uint64_t>(bytes, m_index.getMemspace().getUUID());
        db0::serial::write(bytes, m_index.getAddress());
        if (m_has_query) {
            m_query_it->getSignature(bytes);
        }
        db0::serial::write<bool>(bytes, m_min_inclusive);
        db0::serial::write<bool>(bytes, m_max_inclusive);
        // calculate signature as a hash from bytes
        db0::serial::sha256(bytes, v);
    }
    
}
