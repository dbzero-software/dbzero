#pragma once

#include <list>
#include "FT_Iterator.hpp"
#include "FT_IteratorFactory.hpp"
#include <dbzero/core/utils/heap.hpp>
#include <dbzero/core/utils/unique_set.hpp>
#include <dbzero/core/utils/BoundCheck.hpp>
#include <dbzero/core/serialization/hash.hpp>

namespace db0

{

	class Snapshot;

    /**
     * OR / ORX (OR exclusive) - joining iterator
     */
	template <typename key_t = std::uint64_t> 
	class FT_JoinORXIterator: public FT_Iterator<key_t>
    {
	public :
		using self_t = FT_JoinORXIterator<key_t>;
		using super_t = FT_Iterator<key_t>;
        using MutateFunction = typename FT_Iterator<key_t>::MutateFunction;
        
		/**
         * @param is_orx if true, then exclusive join (ORX) is performed instead of regular OR
         * @param lazy_init if lazy init is requested the iterator is created in the state where only below methods
         * are allowed: begin, clone (this is for lazy construction of the query tree)
         */
        FT_JoinORXIterator(std::list<std::unique_ptr<FT_Iterator<key_t> > > &&inner_iterators, int direction, bool m_is_orx,
            bool lazy_init = false);
		
		virtual ~FT_JoinORXIterator();

		bool isEnd() const override;

		const std::type_info &typeId() const override;
		
		void next(void *buf = nullptr) override;

		void operator++() override;

		void operator--() override;

		key_t getKey() const override;

		bool join(key_t key, int direction) override;

		void joinBound(key_t key) override;

		std::pair<key_t, bool> peek(key_t key) const override;
		
		bool isNextKeyDuplicated() const override;

		bool limitBy(key_t key) override;
		
		std::unique_ptr<FT_Iterator<key_t> > beginTyped(int direction = -1) const override;

		std::ostream &dump(std::ostream &os) const override;

		const FT_IteratorBase *find(std::uint64_t uid) const override;

		/**
         * Calculate number of underlying iterators yielding current output (join_key)
         */
		int getJoinCount() const;

		/**
         * check for duplicate keys pending iteration (applicable with non-exclusive joins)
         * NOTICE: duplicate check is only performed for the current element
         */
		bool hasDuplicateKeys() const;

		/**
         * Get UID of the underlying iterator that yields the current row
         */
		std::uint64_t getInnerUID() const;
		
		bool isORX() const;

        void scanQueryTree(std::function<void(const FT_Iterator<key_t> *it_ptr, int depth)> scan_function,
            int depth = 0) const override;

        std::size_t getDepth() const override;

        /**
         * Iterate all composite iterators
         * @param f function to collect result iterators (should return false to break iteration)
         */
        void forAll(std::function<bool(const db0::FT_Iterator<key_t> &)> f) const;

        void stop() override;

        bool findBy(const std::function<bool(const db0::FT_Iterator<key_t> &)> &f) const override;

        /**
         * Stop current inner iterator available as getSimple
         * must NOT be called over end iterator (undefined behavior)
         * @return false if this operation renders whole iterator tree invalid/end
         */
        bool stopCurrentSimple();

        std::pair<bool, bool> mutateInner(const MutateFunction &f) override;

        void detach();

        FTIteratorType getSerialTypeId() const override;
		
		void getSignature(std::vector<std::byte> &) const override;
		
		static std::unique_ptr<FT_JoinORXIterator<key_t> > deserialize(Snapshot &workspace,
			std::vector<std::byte>::const_iterator &iter, std::vector<std::byte>::const_iterator end);
		
		// reusable signature algorithm
		template <typename IteratorT> static void getSignature(IteratorT begin, IteratorT end,
			std::vector<std::byte> &v)
		{
			std::vector<std::byte> buf;
			// get signature of the 1st simple inner query only
			for (auto it = begin; it != end; ++it) {
				if ((*it)->isSimple()) {
					(*it)->getSignature(buf);
				}
			}
			sortSignatures(buf);
			// keep the 1st simple signature only
			buf.resize(std::min(buf.size(), db0::FT_IteratorBase::SIGNATURE_SIZE));
			// append non-simple signatures next
			for (auto it = begin; it != end; ++it) {
				if (!(*it)->isSimple()) {
					(*it)->getSignature(buf);
				}
			}
			// sort again
			sortSignatures(buf);
			// calculate hash as the result signature
			db0::serial::sha256(buf, v);
		}

	protected:
        void serializeFTIterator(std::vector<std::byte> &) const override;
		
		double compareToImpl(const FT_IteratorBase &it) const override;
		
		// Compare to the same type iterator
		double compareTo(const FT_JoinORXIterator &other) const;

    private:

        FT_JoinORXIterator(std::uint64_t uid, std::list<std::unique_ptr<FT_Iterator<key_t> > > &&inner_iterators, 
			int direction, bool m_is_orx, bool lazy_init = false);
		
		struct heap_item
		{
			db0::FT_Iterator<key_t> *it = nullptr;
			key_t key;
			bool is_end = false;

			heap_item() = default;
			heap_item(FT_Iterator<key_t> &it)
                : it(&it)
                , key(it.getKey())
                , is_end(false)
			{
			}

			bool join(key_t join_key, int direction) 
            {
				if (it->join(join_key, direction)) {
					key = it->getKey();
					return true;
				} else {
					this->is_end = true;
					return false;
				}
			}

			void joinBound(key_t join_key) 
			{
				it->joinBound(join_key);
				this->key = it->getKey();
			}

			void operator++() 
			{
				++(*it);
				if (it->isEnd()) {
					this->is_end = true;
				}
				else {
					this->key = it->getKey();
				}
			}

			void operator--()
			{
				--(*it);
				if (it->isEnd()) {
					this->is_end = true;
				} else {
					this->key = it->getKey();
				}
			}

			bool operator!=(const FT_Iterator<key_t> &it) const {
				return (this->it!=&it);
			}

			bool operator==(const FT_Iterator<key_t> &it) const {
				return (this->it==&it);
			}

			/**
             * Only compare actual keys
             */
			bool operator==(const heap_item &item) const {
				return key==item.key;
			}

			FT_Iterator<key_t> &operator*() {
				return *it;
			}

			const FT_Iterator<key_t> &operator*() const {
				return *it;
			}

			FT_Iterator<key_t> *operator->() {
				return it;
			}

			const FT_Iterator<key_t> *operator->() const {
				return it;
			}

			friend std::ostream &operator<<(std::ostream &os, const heap_item &item) {
				if (item.is_end) {
					return os << "END";
				} else {
					return os << item.key;
				}
			}
		};

		template <typename iterator_t> int getJoinCount(iterator_t begin, iterator_t end) const;

		struct forward_comp_t 
		{
			bool operator()(const heap_item &item0,const heap_item &item1) const {
				return (item0.key < item1.key);
			}
		};

		struct back_comp_t 
		{
			bool operator()(const heap_item &item0,const heap_item &item1) const {
				return (item0.key > item1.key);
			}
			bool operator()(const heap_item &item0,key_t key1) const {
				return (item0.key > key1);
			}
		};
        
		int m_direction;
		std::list<std::unique_ptr<db0::FT_Iterator<key_t> > > m_joinable;
		/// iterators heap (forward join)
		heap<heap_item,forward_comp_t> m_forward_heap;
		/// backward join heap
		heap<heap_item,back_comp_t> m_back_heap;
		/// end of input reached
		bool m_end = false;
		bool m_is_orx;
		BoundCheck<key_t> m_key_bound;
		key_t m_join_key;

		void setEnd();

		void init(int direction);

		/**
         * Feed the join - heap
         */
		bool initHeap(int direction);

		void _next();
		void _next(void *buf);

		void prev();

		/**
         * Join specified inner iterator as leader
         */
		void joinLead(const FT_Iterator<key_t> &it_lead);

		void dumpJoinable(std::ostream &os) const;
	};

	template <typename key_t = std::uint64_t> class FT_OR_ORXIteratorFactory: 
	public FT_IteratorFactory<key_t>
    {
	public :
		FT_OR_ORXIteratorFactory(bool orx_join);

		virtual ~FT_OR_ORXIteratorFactory();

		/**
         * Add single, joinable BIG iterator
         */
		void add(std::unique_ptr<FT_Iterator<key_t> > &&) override;

		void clear() override;

		bool empty() const;

		/**
         * OR / ORX join all (as big iterator)
         */
		virtual std::unique_ptr<FT_Iterator<key_t> > release(int direction, bool lazy_init = false) override;

		/**
         * Release and retrieve result (only if yields OR-iterator)
         */
		std::unique_ptr<FT_Iterator<key_t> > releaseSpecial(int direction, FT_JoinORXIterator<key_t> *&result,
		    bool lazy_init = false);
		
		/**
         * Number of underlying simple joinable iterators
         */
		std::size_t size() const;

	protected :
		const bool m_orx_join;
		std::list<std::unique_ptr<FT_Iterator<key_t> > > m_joinable;
	};
    
	template <typename key_t = std::uint64_t> class FT_ORIteratorFactory : public FT_OR_ORXIteratorFactory<key_t> {
	public :
		FT_ORIteratorFactory();
	};

	template <typename key_t = std::uint64_t> class FT_ORXIteratorFactory : public FT_OR_ORXIteratorFactory<key_t> {
	public :
		FT_ORXIteratorFactory();
	};

    extern template class FT_JoinORXIterator<UniqueAddress>;
    extern template class FT_OR_ORXIteratorFactory<UniqueAddress>;
    extern template class FT_ORIteratorFactory<UniqueAddress>;
    extern template class FT_ORXIteratorFactory<UniqueAddress>;

    extern template class FT_JoinORXIterator<std::uint64_t>;
    extern template class FT_OR_ORXIteratorFactory<std::uint64_t>;
    extern template class FT_ORIteratorFactory<std::uint64_t>;
    extern template class FT_ORXIteratorFactory<std::uint64_t>;
    
}