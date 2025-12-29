// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once 

#include <cstdint>
#include "FT_Iterator.hpp"
#include <dbzero/core/memory/Address.hpp>

namespace db0

{

    class Snapshot;
    
    /**
     * This iterator type performs difference on sets, returning elements which are
     * present in value set of first iterator, but aren't in ANY of the other iterators.
     * Example:
     * 0: 1, 2, 3, 4, 5, 6
     * 1: 2, 7, 8
     * 2: 3, 5, 8
     * Returned values: 1, 4, 6
    */

    template <typename key_t = std::uint64_t>
    class FT_ANDNOTIterator final : public FT_Iterator<key_t>
    {
    public:
        using self_t = FT_ANDNOTIterator<key_t>;
        using super_t = FT_Iterator<key_t>;
        using MutateFunction = typename FT_Iterator<key_t>::MutateFunction;

        FT_ANDNOTIterator(std::vector<std::unique_ptr<FT_Iterator<key_t>>> &&inner_iterators, int direction,
            bool lazy_init = false);
        
        std::ostream &dump(std::ostream &os) const override;

        const FT_IteratorBase *find(std::uint64_t uid) const override;

        key_t getKey() const override;

        bool isEnd() const override;

        const std::type_info &typeId() const override;
        
        void next(void *buf = nullptr) override;

        void operator++() override;

        void operator--() override;

        bool join(key_t join_key, int direction) override;
        
        void joinBound(key_t join_key) override;
        
        std::pair<key_t, bool> peek(key_t join_key) const override;
        
        bool isNextKeyDuplicated() const override;
                
        std::unique_ptr<FT_Iterator<key_t> > beginTyped(int direction = -1) const override;

        bool limitBy(key_t key) override;

        void scanQueryTree(std::function<void(const FT_Iterator<key_t> *it_ptr, int depth)> scan_function,
            int depth = 0) const override;

        virtual std::size_t getDepth() const override;

        /**
         * Get first iterator's reference (included in results)
         * @return
         */
        const FT_Iterator<key_t> &getFirst() const;

        /**
         * Get all iterators excluded from results, continue while f returns true
         * @param f
         */
        void forNot(std::function<bool(const FT_Iterator<key_t> &)> f) const;

        /**
         * Begin NOT- part of the query only
         * @return
         */
        std::unique_ptr<FT_Iterator<key_t> > beginNot(int direction) const;

        void stop() override;

        bool findBy(const std::function<bool(const FT_Iterator<key_t> &)> &f) const override;

        std::pair<bool, bool> mutateInner(const MutateFunction &f) override;

        void detach();

        FTIteratorType getSerialTypeId() const override;        
        
        void getSignature(std::vector<std::byte> &) const override;
        
        static std::unique_ptr<FT_ANDNOTIterator<key_t>> deserialize(Snapshot &workspace, 
            std::vector<std::byte>::const_iterator &iter, std::vector<std::byte>::const_iterator end);
        
    protected:
        FT_ANDNOTIterator(std::uint64_t uid, std::vector<std::unique_ptr<FT_Iterator<key_t>>> &&inner_iterators, 
            int direction, bool lazy_init = false);
        
        void serializeFTIterator(std::vector<std::byte> &) const override;
        
        double compareToImpl(const FT_IteratorBase &it) const override;
        
        double compareTo(const FT_ANDNOTIterator &) const;

    private:
        int m_direction;
        std::vector<std::unique_ptr<FT_Iterator<key_t>>> m_joinable;

        FT_Iterator<key_t> &getBaseIterator();
        const FT_Iterator<key_t> &getBaseIterator() const;

        struct HeapItem
        {
            FT_Iterator<key_t> *it;
            key_t key;

            bool operator<(const HeapItem &other) const {
                return key < other.key;
            }

            bool operator>(const HeapItem &other) const {
                return key > other.key;
            }
        };

        using ForwardHeapCompare = std::greater<typename FT_ANDNOTIterator<key_t>::HeapItem>;
        using BackwardHeapCompare = std::less<typename FT_ANDNOTIterator<key_t>::HeapItem>;

        std::vector<HeapItem> m_subtrahends_heap;

        void updateWithHeap();

        bool inResult(const key_t &key, int direction);
        
        bool next(int direction, void *buf = nullptr);
    };
    
    extern template class FT_ANDNOTIterator<std::uint64_t>;
    extern template class FT_ANDNOTIterator<UniqueAddress>;
       
}