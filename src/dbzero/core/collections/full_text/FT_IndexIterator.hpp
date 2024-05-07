#pragma once

#include "key_value.hpp"
#include "FT_Iterator.hpp"
#include "FT_Runnable.hpp"
#include "CloneMap.hpp"
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
	template <typename bindex_t, typename key_t = std::uint64_t> 
	class FT_IndexIterator: public FT_Iterator<key_t>
	{
	public:
		using self_t = FT_IndexIterator<bindex_t, key_t>;
		using super_t = FT_Iterator<key_t>;
		using iterator = typename bindex_t::joinable_const_iterator;

		FT_IndexIterator(const bindex_t &data, int direction, std::optional<std::uint64_t> index_key = {});

		/**
         * Construct over already initialized simple iterator
         */
		FT_IndexIterator(const bindex_t &data, int direction, const iterator &it,
			std::optional<std::uint64_t> index_key = {});

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

		std::unique_ptr<FT_Iterator<key_t> > clone(
            CloneMap<FT_Iterator<key_t> > *clone_map_ptr = nullptr) const override;
		
        std::unique_ptr<FT_Iterator<key_t> > beginTyped(int direction) const override;

		bool limitBy(key_t key) override;

		std::ostream &dump(std::ostream &os) const override;        

		bool equal(const FT_IteratorBase &it) const override;

        void scanQueryTree(std::function<void(const FT_Iterator<key_t> *it_ptr, int depth)> scan_func,
            int depth = 0) const override;
        
        /**
         * @return const-reference to native v_bindex iterator
         */
        const iterator &asNative() const;

        std::uint64_t getIndexKey() const override;

        void detach();

	    void stop() override;

	    std::size_t getDepth() const override;

		FTIteratorType getSerialTypeId() const override;

		void extractRunnable(FT_Runnable *) const override;

    protected:
        bindex_t m_data;
        const int m_direction;
        // underlying native iterator (joinable_const_iterator)
        mutable iterator m_iterator;
        // key value at which the iterator has been detached
        bool m_is_detached = false;
        bool m_has_detach_key = false;
        key_t m_detach_key;
        const std::optional<std::uint64_t> m_index_key;

		class FT_IndexIteratorRunnable: public FT_Runnable
		{
		public:
			FT_IndexIteratorRunnable(const bindex_t &data, int direction, std::uint64_t index_key)
				: m_fixture_uuid(data.getMemspace().getUUID())
				, m_direction(direction)
				, m_index_key(index_key)
			{
			}

			std::unique_ptr<FT_IteratorBase> run(db0::Snapshot &) const override {
				throw std::runtime_error("Not implemented");
			}

			FTRunnableType typeId() const override {
				return FTRunnableType::Index;
			}

			void serialize(std::vector<std::byte> &v) const override
			{
				db0::serial::write(v, db0::serial::typeId<bindex_t>());
				db0::serial::write(v, db0::serial::typeId<key_t>());				
				db0::serial::write(v, m_fixture_uuid);
				db0::serial::write<std::int8_t>(v, m_direction);
				db0::serial::write(v, m_index_key);
			}

		private:
			const std::uint64_t m_fixture_uuid;
			const int m_direction;
        	const std::uint64_t m_index_key;
		};
		
        /**
         * Get valid iterator after detach
         * @return
         */
        iterator &getIterator();

        const iterator &getIterator() const;

        void _next(void *buf = nullptr);

		void serializeFTIterator(std::vector<std::byte> &) const override;
    };
	
	template <typename bindex_t, typename key_t>
	FT_IndexIterator<bindex_t, key_t>::FT_IndexIterator(const bindex_t &data, int direction, 
		std::optional<std::uint64_t> index_key)
        : m_data(data)
        , m_direction(direction)
        , m_iterator(m_data.beginJoin(direction))
        , m_index_key(index_key)
    {
    }
	
	template <typename bindex_t, typename key_t>
	FT_IndexIterator<bindex_t, key_t>::FT_IndexIterator(const bindex_t &data, int direction, const iterator &it,
	    std::optional<std::uint64_t> index_key)
        : m_data(data)
        , m_direction(direction)
        , m_iterator(it)
        , m_index_key(index_key)
    {
    }

	template <typename bindex_t, typename key_t >
	key_t FT_IndexIterator<bindex_t, key_t>::getKey() const {
		// casts underlying item to long_ptr
		return *getIterator();
	}

	template <typename bindex_t, typename key_t >
	bool FT_IndexIterator<bindex_t, key_t>::isEnd() const {
		return getIterator().is_end();
	}

	template <typename bindex_t, typename key_t >
	void FT_IndexIterator<bindex_t, key_t>::operator++() {
		assert(m_direction > 0);
		++getIterator();
	}

	template <typename bindex_t, typename key_t >
	void FT_IndexIterator<bindex_t, key_t>::_next(void *buf) 
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

	template <typename bindex_t, typename key_t >
	void FT_IndexIterator<bindex_t, key_t>::operator--() {
		this->_next(nullptr);
	}

	template <typename bindex_t, typename key_t >
	void FT_IndexIterator<bindex_t, key_t>::next(void *buf) {
		this->_next(buf);
	}

	template <typename bindex_t, typename key_t >
	bool FT_IndexIterator<bindex_t, key_t>::join(key_t join_key, int direction) {
		return getIterator().join(join_key, direction);
	}

	template <typename bindex_t, typename key_t >
	void FT_IndexIterator<bindex_t, key_t>::joinBound(key_t join_key) {
		getIterator().joinBound(join_key);
	}

	template <typename bindex_t, typename key_t >
	std::pair<key_t, bool> FT_IndexIterator<bindex_t, key_t>::peek(key_t join_key) const {
		return getIterator().peek(join_key);
	}

	template <typename bindex_t, typename key_t >
	std::unique_ptr<FT_Iterator<key_t> > FT_IndexIterator<bindex_t, key_t>::clone(
		CloneMap<FT_Iterator<key_t> > *clone_map_ptr) const
	{
		// clone preserving state
		std::unique_ptr<FT_Iterator<key_t> > result(
            new FT_IndexIterator(m_data, m_direction, getIterator(), this->m_index_key));
		if (clone_map_ptr) {
			clone_map_ptr->insert(*result.get(), *this);
		}
		return result;
	}

	template <typename bindex_t, typename key_t >
	std::unique_ptr<FT_Iterator<key_t> > FT_IndexIterator<bindex_t, key_t>::beginTyped(int direction) const {
		return std::unique_ptr<FT_Iterator<key_t> >(new FT_IndexIterator(m_data, direction, this->m_index_key));
	}

	template <typename bindex_t, typename key_t >
	bool FT_IndexIterator<bindex_t, key_t>::limitBy(key_t key) {
		// simply pass through underlying collection iterator
		return getIterator().limitBy(key);
	}

	template <typename bindex_t, typename key_t >
	std::ostream &FT_IndexIterator<bindex_t, key_t>::dump(std::ostream &os) const {
		return os << "FTIndex@" << this;
	}

	template <typename bindex_t, typename key_t>
	bool FT_IndexIterator<bindex_t, key_t>::equal(const FT_IteratorBase &it) const 
    {
		if (this->typeId() != it.typeId()) {
			return false;
		}
		// check if iterating over the same data collection
		return m_data == reinterpret_cast<const decltype(*this)&>(it).m_data;
	}
	
	template <typename bindex_t, typename key_t>
	const typename FT_IndexIterator<bindex_t, key_t>::iterator &FT_IndexIterator<bindex_t, key_t>::asNative() const {
		return getIterator();
	}

	template <typename bindex_t, typename key_t>
	std::uint64_t FT_IndexIterator<bindex_t, key_t>::getIndexKey() const {
		return *m_index_key;
	}

	template <typename bindex_t, typename key_t>
	void FT_IndexIterator<bindex_t, key_t>::scanQueryTree(
	    std::function<void(const FT_Iterator<key_t> *it_ptr, int depth)> scan_function, int depth) const {
		scan_function(this, depth);
	}

    template <typename bindex_t, typename key_t> std::size_t FT_IndexIterator<bindex_t, key_t>::getDepth() const {
        return 1u;
    }

	template <typename bindex_t, typename key_t> void FT_IndexIterator<bindex_t, key_t>::stop() {
	    getIterator().stop();
	}

    template <typename bindex_t, typename key_t> void FT_IndexIterator<bindex_t, key_t>::detach() 
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
	
    template <typename bindex_t, typename key_t> typename FT_IndexIterator<bindex_t, key_t>::iterator &
    FT_IndexIterator<bindex_t, key_t>::getIterator() 
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

    template <typename bindex_t, typename key_t> const typename FT_IndexIterator<bindex_t, key_t>::iterator &
    FT_IndexIterator<bindex_t, key_t>::getIterator() const 
	{
        // forward to the non-const method
        return const_cast<FT_IndexIterator<bindex_t, key_t> &>(*this).getIterator();
    }

	template <typename bindex_t, typename key_t> const std::type_info &FT_IndexIterator<bindex_t, key_t>::typeId() const {
		return typeid(self_t);
	}
	
	template <typename bindex_t, typename key_t>
	FTIteratorType FT_IndexIterator<bindex_t, key_t>::getSerialTypeId() const {
		return FTIteratorType::Index;
	}

	template <typename bindex_t, typename key_t>
	void FT_IndexIterator<bindex_t, key_t>::serializeFTIterator(std::vector<std::byte> &v) const
	{
		using TypeIdType = decltype(db0::serial::typeId<void>());

		if (!m_index_key) {
			THROWF(db0::InternalException) << "Index key is required for serialization" << THROWF_END;
		}

		// write underlying type IDs
		db0::serial::write<TypeIdType>(v, bindex_t::getSerialTypeId());
		db0::serial::write<TypeIdType>(v, db0::serial::typeId<key_t>());
		db0::serial::write(v, m_data.getMemspace().getUUID());
		db0::serial::write<std::int8_t>(v, m_direction);			
		db0::serial::write(v, *m_index_key);
	}

	template <typename bindex_t, typename key_t>
	void FT_IndexIterator<bindex_t, key_t>::extractRunnable(FT_Runnable *at_ptr) const
	{
		assert(at_ptr);
		if (!m_index_key) {
			THROWF(db0::InternalException) << "Index key is required for runnable extraction" << THROWF_END;
		}
		new (at_ptr) FT_IndexIteratorRunnable(m_data, m_direction, *m_index_key);		
	}

} 
