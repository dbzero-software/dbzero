// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "FT_Iterator.hpp"
#include <dbzero/core/utils/SortedArray.hpp>
#include <dbzero/core/memory/Address.hpp>

namespace db0

{

    // The FT_FixedKeyIterator stores a fixed collection of keys in-memory
    // this is usefull in when checking if a specific object (or collection of objects) is present in query result
    template <typename KeyT> 
    class FT_FixedKeyIterator final: public FT_Iterator<KeyT>
    {
    public:
        using self_t = FT_FixedKeyIterator<KeyT>;
        FT_FixedKeyIterator(const KeyT *begin, const KeyT *end, int direction = -1, bool is_sorted = false);

        const std::type_info &typeId() const;

		bool isEnd() const override;

        void next(void *buf = nullptr) override;

		void operator++() override;

		void operator--() override;

        KeyT getKey() const override;
        
        std::unique_ptr<FT_IteratorBase> begin() const override;
        
        std::unique_ptr<FT_Iterator<KeyT> > beginTyped(int direction = -1) const override;

		bool join(KeyT join_key, int direction = -1) override;

		void joinBound(KeyT join_key) override;

		std::pair<KeyT, bool> peek(KeyT join_key) const override;
        
        bool isNextKeyDuplicated() const override;
        
		bool limitBy(KeyT key) override;
        
        void stop() override;
        
        FTIteratorType getSerialTypeId() const override;
        
        double compareToImpl(const FT_IteratorBase &it) const override;

        void getSignature(std::vector<std::byte> &) const override;

        std::ostream &dump(std::ostream &os) const override;

        std::size_t size() const;

    protected:
        void serializeFTIterator(std::vector<std::byte> &) const override;
        
    private:
        std::vector<KeyT> m_sorted_keys;
        SortedArray<KeyT> m_keys;
        const int m_direction;
        typename SortedArray<KeyT>::ConstIteratorT m_current;

        static std::vector<KeyT> getSorted(const KeyT *begin, const KeyT *end, bool is_sorted);

        double compareTo(const self_t &other) const;
    };
    
    extern template class FT_FixedKeyIterator<std::uint64_t>;
    extern template class FT_FixedKeyIterator<UniqueAddress>;
    
}
