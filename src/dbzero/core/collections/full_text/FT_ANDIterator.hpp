#pragma once

#include <list>
#include <functional>
#include "FT_Iterator.hpp"
#include "FT_IteratorBase.hpp"
#include "FT_IteratorFactory.hpp"
#include "IteratorGroup.hpp"
#include <dbzero/core/memory/Address.hpp>

namespace db0

{
    
    class Snapshot;
    
    /**
     * AND - joining iterator (join abstract full-text iterators)     
     */
    template <typename key_t = std::uint64_t, bool UniqueKeys = true>
    class FT_JoinANDIterator : public FT_Iterator<key_t>
    {
	public:
        using self_t = FT_JoinANDIterator<key_t, UniqueKeys>;
		using super_t = FT_Iterator<key_t>;
        using MutateFunction = typename FT_Iterator<key_t>::MutateFunction;

        /**
         * @param sub_it
         * @param direction
         * @param lazy_init if lazy init is requested the iterator is created in the state where only below methods
         * are allowed: beginBack, clone (this is for lazy construction of the query tree)
         */
		FT_JoinANDIterator(std::list<std::unique_ptr<FT_Iterator<key_t> > > &&inner_iterators, int direction = -1,
		    bool lazy_init = false);

		/**
         * join pair of iterators
         */
		FT_JoinANDIterator(std::unique_ptr<FT_Iterator<key_t> > &&it0, std::unique_ptr<FT_Iterator<key_t> > &&it1,
		    int direction = -1, bool lazy_init = false);

		virtual ~FT_JoinANDIterator();

		/**
         * IFT_Iterator interface members
         */
		bool isEnd() const override;

        const std::type_info &typeId() const override;

        void next(void *buf = nullptr) override;

		void operator++() override;

		void operator--() override;

		key_t getKey() const override;

        /**
         * Get underlying iterator that yields the current key
         */
        const FT_Iterator<key_t> &getSimple() const;

		bool join(key_t join_key, int direction) override;

		void joinBound(key_t key) override;

		std::pair<key_t, bool> peek(key_t join_key) const override;
        
        bool isNextKeyDuplicated() const override;
         
		std::unique_ptr<FT_Iterator<key_t> > beginTyped(int direction = -1) const override;
        
		bool limitBy(key_t key) override;

		std::ostream &dump(std::ostream &os) const override;
		
		const FT_IteratorBase *find(std::uint64_t uid) const override;

        void scanQueryTree(std::function<void(const FT_Iterator<key_t> *it_ptr, int depth)> scan_function,
            int depth = 0) const override;

        std::size_t getDepth() const override;

        /**
         * Iterate all composite iterators
         * @param f function to collect result iterators (should return false to break iteration)
         */
        void forAll(std::function<bool(const FT_Iterator<key_t> &)> f) const;

        void stop() override;

        bool findBy(const std::function<bool(const FT_Iterator<key_t> &)> &f) const override;

        std::pair<bool, bool> mutateInner(const MutateFunction &f) override;
        
        virtual void detach();

        FTIteratorType getSerialTypeId() const override;
        
        double compareToImpl(const FT_IteratorBase &it) const override;
        
        void getSignature(std::vector<std::byte> &) const override;

        static std::unique_ptr<FT_Iterator<key_t> > deserialize(Snapshot &workspace,
            std::vector<std::byte>::const_iterator &iter, std::vector<std::byte>::const_iterator end);
        
    protected:
        void serializeFTIterator(std::vector<std::byte> &) const override;
        
	private:
		const int m_direction;
		mutable IteratorGroup<key_t> m_joinable;
		bool m_end;
		key_t m_join_key;
        
		void setEnd();

        void _next();
        void _next(void*);
        void _nextUnique();
		void joinAll();

		FT_JoinANDIterator(std::uint64_t uid, std::list<std::unique_ptr<FT_Iterator<key_t> > > &&inner_iterators, 
            int direction, bool lazy_init = false);
        
        // compare to other AND iterator
        double compareTo(const FT_JoinANDIterator &it) const;
	};
    
    template <typename key_t = std::uint64_t, bool UniqueKeys = true>
    class FT_ANDIteratorFactory: public FT_IteratorFactory<key_t>
    {
	public :
		FT_ANDIteratorFactory();
		~FT_ANDIteratorFactory();

		/**
         * Add single, joinable FT_ iterator / sink
         */
		void add(std::unique_ptr<FT_Iterator<key_t> > &&) override;

		/**
         * AND-join all (as FT_ iterator)
         * @param lazy_init if lazy init is requested the iterator is created in the state where only below methods
         * are allowed: beginBack, clone (this is for lazy construction of the query tree)
         */
		std::unique_ptr<FT_Iterator<key_t> > release(int direction = -1, bool lazy_init = false) override;

        void clear() override;

		/**
         * Number of underlying simple joinable iterators
         */
		std::size_t size() const;

		bool empty() const;
        
	private:
        bool m_invalidated = false;
		std::list<std::unique_ptr<FT_Iterator<key_t> > > m_joinable;
	};

    /**
     * Debug and evaluation only member (will iterate over results and print range / count)
     */
    template <typename key_t> void iterateAll(std::unique_ptr<FT_Iterator<key_t> > &&it) 
    {
        std::uint32_t result = 0;
        // will hold first and last keys
        std::pair<key_t, key_t> keys;
        bool is_first = true;
        while (!(*it).isEnd()) {
            if (is_first) {
                keys.first = it->getKey();
                is_first = false;
            } else {
                keys.second = it->getKey();
            }
            ++result;
            --(*it);
        }
    }

    extern template class FT_JoinANDIterator<std::uint64_t, false>;
    extern template class FT_JoinANDIterator<std::uint64_t, true>;

    extern template class FT_JoinANDIterator<UniqueAddress, false>;
    extern template class FT_JoinANDIterator<UniqueAddress, true>;

    extern template class FT_ANDIteratorFactory<std::uint64_t, false>;
    extern template class FT_ANDIteratorFactory<std::uint64_t, true>;

    extern template class FT_ANDIteratorFactory<UniqueAddress, false>;
    extern template class FT_ANDIteratorFactory<UniqueAddress, true>;

}
