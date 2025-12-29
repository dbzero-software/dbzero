// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <vector>
#include "FT_Iterator.hpp"
#include "CP_Vector.hpp"

namespace db0

{
    
    /**
     * Construsts a cartesian product of multiple iterators.
     * Which itself acts as an FT_Iterator - allowing efficient joins of cartesian products.
     * NOTE: the underlying key type represents a pointer to an array of key_t values.
     * NOTE: as a genral principle key_t* return values are pointers to internal buffers and are only valid
     * until the next member call of the same instance.
    */
    template <typename key_t = std::uint64_t>
    class CartesianProduct final: public FT_Iterator<const key_t*, CP_Vector<key_t> >
    {
	public:
        using KeyT = const key_t*;
        using KeyStorageT = CP_Vector<key_t>;

        CartesianProduct(const std::vector<std::unique_ptr<FT_Iterator<key_t>>> &, int direction = -1);
        CartesianProduct(std::vector<std::unique_ptr<FT_Iterator<key_t>>> &&, int direction = -1);

		bool isEnd() const override;

        const std::type_info &typeId() const override;

        void next(void *buf = nullptr) override;

		void operator++() override;

		void operator--() override;

        // NOTE: return value lifetime rules apply
		KeyT getKey() const override;
        
        void getKey(KeyStorageT &) const override;
        
        bool swapKey(KeyStorageT &) const override;

		bool join(KeyT, int direction = -1) override;

		void joinBound(KeyT) override;

        // NOTE: return value lifetime rules apply
		std::pair<KeyT, bool> peek(KeyT) const override;
        
        bool isNextKeyDuplicated() const override;
        
		std::unique_ptr<FT_Iterator<KeyT, KeyStorageT> > beginTyped(int direction = -1) const override;
        
		bool limitBy(KeyT) override;

		std::ostream &dump(std::ostream &os) const override;
				
        void stop() override;
        
        FTIteratorType getSerialTypeId() const override;
        
        double compareToImpl(const FT_IteratorBase &it) const override;
        
        void getSignature(std::vector<std::byte> &) const override;

    protected:
        std::vector<std::unique_ptr<FT_Iterator<key_t>>> m_components;
        const bool m_direction;
        bool m_overflow = false;
        KeyStorageT m_current_key;        
        
        void serializeFTIterator(std::vector<std::byte> &) const override;
        // @return swap key result (i.e. was the key component changed)
        bool joinAt(unsigned int at, key_t, bool reset, int direction = -1);
    };
    
    extern template class CartesianProduct<UniqueAddress>;
    extern template class CartesianProduct<std::uint64_t>;

}
