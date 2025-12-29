// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "key_value.hpp"
#include "FT_Iterator.hpp"
#include <dbzero/core/collections/b_index/v_bindex.hpp>
#include <dbzero/core/collections/b_index/mb_index.hpp>
#include <dbzero/core/serialization/Serializable.hpp>
#include <optional>

namespace db0

{
    
	/**
	 * bindex_t - some bindex derived type with key_t (e.g. std::uint64_t) derived keys (v_bindex)
	 * implements FT_Iterator interface over b-index data structure
	 */
	template <typename bindex_t, typename key_t = std::uint64_t, typename IndexKeyT = std::uint64_t>
	class FT_IndexIterator: public FT_Iterator<key_t>
	{
	public:
		using self_t = FT_IndexIterator<bindex_t, key_t, IndexKeyT>;
		using super_t = FT_Iterator<key_t>;
		using iterator = typename bindex_t::joinable_const_iterator;

		FT_IndexIterator(const bindex_t &data, int direction, std::optional<IndexKeyT> index_key = {});

		/**
         * Construct over already initialized simple iterator
         */
		FT_IndexIterator(const bindex_t &data, int direction, const iterator &it,
			std::optional<IndexKeyT> index_key = {});

        virtual ~FT_IndexIterator() = default;
        
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

		std::ostream &dump(std::ostream &os) const override;
		        
        /**
         * @return const-reference to native v_bindex iterator
         */
        const iterator &asNative() const;

        IndexKeyT getIndexKey() const;

        void detach();

	    void stop() override;
	    
		FTIteratorType getSerialTypeId() const override;

		void getSignature(std::vector<std::byte> &) const override;
			
    protected:
        bindex_t m_data;
        const int m_direction;
        // underlying native iterator (joinable_const_iterator)
        mutable iterator m_iterator;
        // key value at which the iterator has been detached
        bool m_is_detached = false;
        bool m_has_detach_key = false;
        key_t m_detach_key;
        const std::optional<IndexKeyT> m_index_key;

		FT_IndexIterator(std::uint64_t uid, const bindex_t &data, int direction, 
			std::optional<IndexKeyT> index_key = {});

        /**
         * Get valid iterator after detach
         * @return
         */
        iterator &getIterator();

        const iterator &getIterator() const;

        void _next(void *buf = nullptr);

		void serializeFTIterator(std::vector<std::byte> &) const override;

		double compareToImpl(const FT_IteratorBase &it) const override;

		double compareTo(const FT_IndexIterator &it) const;
    };
	
	template <typename bindex_t, typename key_t, typename IndexKeyT>
	FT_IndexIterator<bindex_t, key_t, IndexKeyT>::FT_IndexIterator(const bindex_t &data, int direction, 
		std::optional<IndexKeyT> index_key)
        : m_data(data)
        , m_direction(direction)
        , m_iterator(m_data.beginJoin(direction))
        , m_index_key(index_key)
    {
    }
	
	template <typename bindex_t, typename key_t, typename IndexKeyT>
	FT_IndexIterator<bindex_t, key_t, IndexKeyT>::FT_IndexIterator(const bindex_t &data, int direction, const iterator &it,
	    std::optional<IndexKeyT> index_key)
        : m_data(data)
        , m_direction(direction)
        , m_iterator(it)
        , m_index_key(index_key)
    {
    }

	template <typename bindex_t, typename key_t, typename IndexKeyT>
	FT_IndexIterator<bindex_t, key_t, IndexKeyT>::FT_IndexIterator(std::uint64_t uid, const bindex_t &data, int direction, 
		std::optional<IndexKeyT> index_key)
		: FT_Iterator<key_t>(uid)
        , m_data(data)
        , m_direction(direction)
        , m_iterator(m_data.beginJoin(direction))
        , m_index_key(index_key)
    {
    }

	template <typename bindex_t, typename key_t, typename IndexKeyT>
	key_t FT_IndexIterator<bindex_t, key_t, IndexKeyT>::getKey() const {
		// casts underlying item to long_ptr
		return *getIterator();
	}

	template <typename bindex_t, typename key_t, typename IndexKeyT>
	bool FT_IndexIterator<bindex_t, key_t, IndexKeyT>::isEnd() const {
		return getIterator().is_end();
	}

	template <typename bindex_t, typename key_t, typename IndexKeyT>
	void FT_IndexIterator<bindex_t, key_t, IndexKeyT>::operator++() {
		assert(m_direction > 0);
		++getIterator();
	}

	template <typename bindex_t, typename key_t, typename IndexKeyT>
	void FT_IndexIterator<bindex_t, key_t, IndexKeyT>::_next(void *buf) 
	{
		assert(m_direction < 0);
		assert(!this->isEnd());
		if (buf) {
			// extract key from the underlying iterator
			key_t key = *getIterator();
			std::memcpy(buf, &key, sizeof(key_t));
		}
		--getIterator();
	}

	template <typename bindex_t, typename key_t, typename IndexKeyT>
	void FT_IndexIterator<bindex_t, key_t, IndexKeyT>::operator--() {
		this->_next(nullptr);
	}

	template <typename bindex_t, typename key_t, typename IndexKeyT>
	void FT_IndexIterator<bindex_t, key_t, IndexKeyT>::next(void *buf) {
		this->_next(buf);
	}

	template <typename bindex_t, typename key_t, typename IndexKeyT>
	bool FT_IndexIterator<bindex_t, key_t, IndexKeyT>::join(key_t join_key, int direction) {
		return getIterator().join(join_key, direction);
	}

	template <typename bindex_t, typename key_t, typename IndexKeyT>
	void FT_IndexIterator<bindex_t, key_t, IndexKeyT>::joinBound(key_t join_key) {
		getIterator().joinBound(join_key);
	}

	template <typename bindex_t, typename key_t, typename IndexKeyT>
	std::pair<key_t, bool> FT_IndexIterator<bindex_t, key_t, IndexKeyT>::peek(key_t join_key) const {
		return getIterator().peek(join_key);
	}
	
	template <typename bindex_t, typename key_t, typename IndexKeyT>
	std::unique_ptr<FT_Iterator<key_t> > FT_IndexIterator<bindex_t, key_t, IndexKeyT>::beginTyped(int direction) const {
		return std::unique_ptr<FT_Iterator<key_t> >(new FT_IndexIterator(this->m_uid, m_data, direction, this->m_index_key));
	}

	template <typename bindex_t, typename key_t, typename IndexKeyT>
	bool FT_IndexIterator<bindex_t, key_t, IndexKeyT>::limitBy(key_t key) {
		// simply pass through underlying collection iterator
		return getIterator().limitBy(key);
	}

	template <typename bindex_t, typename key_t, typename IndexKeyT>
	std::ostream &FT_IndexIterator<bindex_t, key_t, IndexKeyT>::dump(std::ostream &os) const {
		return os << "FTIndex@" << this;
	}
		
	template <typename bindex_t, typename key_t, typename IndexKeyT>
	const typename FT_IndexIterator<bindex_t, key_t, IndexKeyT>::iterator &FT_IndexIterator<bindex_t, key_t, IndexKeyT>::asNative() const {
		return getIterator();
	}

	template <typename bindex_t, typename key_t, typename IndexKeyT>
	IndexKeyT FT_IndexIterator<bindex_t, key_t, IndexKeyT>::getIndexKey() const {
		return *m_index_key;
	}

	template <typename bindex_t, typename key_t, typename IndexKeyT> 
	void FT_IndexIterator<bindex_t, key_t, IndexKeyT>::stop() {
	    getIterator().stop();
	}

    template <typename bindex_t, typename key_t, typename IndexKeyT>
	void FT_IndexIterator<bindex_t, key_t, IndexKeyT>::detach()
    {
		/* FIXME: implement when needed
        if (!this->m_is_detached) {
            if (!this->m_iterator.is_end()) {
                this->m_detach_key = *(this->m_iterator);
                this->m_has_detach_key = true;
            } else {
                this->m_has_detach_key = false;
            }
            const_cast<bindex_t &>(this->m_data).detach();
            this->m_iterator.reset();
            this->m_is_detached = true;
        }
		*/
    }
	
    template <typename bindex_t, typename key_t, typename IndexKeyT>
	typename FT_IndexIterator<bindex_t, key_t, IndexKeyT>::iterator &FT_IndexIterator<bindex_t, key_t, IndexKeyT>::getIterator()
    {
        if (m_is_detached) {            
			m_iterator = m_data.beginJoin(m_direction);
			if (m_has_detach_key) {
				m_iterator.join(m_detach_key, m_direction);
			} else {
				m_iterator.stop();
			}
            m_is_detached = false;
        }
        return m_iterator;
    }

    template <typename bindex_t, typename key_t, typename IndexKeyT> 
	const typename FT_IndexIterator<bindex_t, key_t, IndexKeyT>::iterator &FT_IndexIterator<bindex_t, key_t, IndexKeyT>::getIterator() const
	{
        // forward to the non-const method
        return const_cast<FT_IndexIterator<bindex_t, key_t, IndexKeyT> &>(*this).getIterator();
    }

	template <typename bindex_t, typename key_t, typename IndexKeyT> 
	const std::type_info &FT_IndexIterator<bindex_t, key_t, IndexKeyT>::typeId() const {
		return typeid(self_t);
	}
	
	template <typename bindex_t, typename key_t, typename IndexKeyT>
	FTIteratorType FT_IndexIterator<bindex_t, key_t, IndexKeyT>::getSerialTypeId() const {
		return FTIteratorType::Index;
	}

	template <typename bindex_t, typename key_t, typename IndexKeyT>
	void FT_IndexIterator<bindex_t, key_t, IndexKeyT>::serializeFTIterator(std::vector<std::byte> &v) const
	{
		using TypeIdType = decltype(db0::serial::typeId<void>());

		if (!m_index_key) {
			THROWF(db0::InternalException) << "Index key is required for serialization" << THROWF_END;
		}

		// write underlying type IDs
		db0::serial::write<TypeIdType>(v, bindex_t::getSerialTypeId());
		db0::serial::write<TypeIdType>(v, db0::serial::typeId<key_t>());
		db0::serial::write<TypeIdType>(v, db0::serial::typeId<IndexKeyT>());
		db0::serial::write(v, m_data.getMemspace().getUUID());
		db0::serial::write<std::int8_t>(v, m_direction);		
		db0::serial::write(v, *m_index_key);
	}

	template <typename bindex_t, typename key_t, typename IndexKeyT>
	double FT_IndexIterator<bindex_t, key_t, IndexKeyT>::compareToImpl(const FT_IteratorBase &it) const
	{
		if (this->typeId() == it.typeId()) {
			return compareTo(reinterpret_cast<const self_t &>(it));
		}
		return 1.0;
	}

	template <typename bindex_t, typename key_t, typename IndexKeyT>
	double FT_IndexIterator<bindex_t, key_t, IndexKeyT>::compareTo(const FT_IndexIterator &other) const
	{
		if (m_index_key && other.m_index_key) {
			return (*m_index_key == *other.m_index_key) ? 0.0 : 1.0;
		}
		
		return (m_data.getAddress() == other.m_data.getAddress()) ? 0.0 : 1.0;
	}
	
	template <typename bindex_t, typename key_t, typename IndexKeyT>
	void FT_IndexIterator<bindex_t, key_t, IndexKeyT>::getSignature(std::vector<std::byte> &v) const {
		// get the serializable's signature
		db0::serial::getSignature(*this, v);
	}
	
	template <typename bindex_t, typename key_t, typename IndexKeyT>
	bool FT_IndexIterator<bindex_t, key_t, IndexKeyT>::isNextKeyDuplicated() const {
		return getIterator().isNextKeyDuplicated();
	}
	
} 
