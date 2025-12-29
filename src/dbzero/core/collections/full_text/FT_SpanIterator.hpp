// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "FT_Iterator.hpp"
#include <dbzero/core/memory/Address.hpp>

namespace db0

{

    // SpanIterator allows managing elements which represent spans instead of specific keys
    // The span is determined by the shift operator which suits it to represent e.g. entire data pages
    template <typename KeyT>
    class FT_SpanIterator final: public FT_Iterator<KeyT>
    {
    public:
        using self_t = FT_SpanIterator<KeyT>;
        // @param span_shift - the shift to be applied to the key to determine the span
        FT_SpanIterator(std::unique_ptr<FT_Iterator<KeyT> > &&, unsigned int span_shift, 
            int direction = -1);
        
        const std::type_info &typeId() const override;

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
                
        double compareToImpl(const FT_IteratorBase &it) const override;
        
        std::ostream& dump(std::ostream&) const override;

        void getSignature(std::vector<std::byte>&) const override;

        db0::FTIteratorType getSerialTypeId() const override;

        void serializeFTIterator(std::vector<std::byte>&) const override;
                        
    private:
        std::unique_ptr<FT_Iterator<KeyT> > m_inner_it;        
        const unsigned int m_span_shift;
        const unsigned int m_span_size;
        const int m_direction;
        // the last joined key
        std::optional<KeyT> m_key;

        KeyT _getKey() const;

        double compareToImpl(const self_t &) const;
    };
    
    extern template class FT_SpanIterator<std::uint64_t>;
    extern template class FT_SpanIterator<UniqueAddress>;
    
}
