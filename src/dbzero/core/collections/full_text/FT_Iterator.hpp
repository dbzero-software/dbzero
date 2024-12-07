#pragma once

#include <iosfwd>
#include <atomic>
#include <functional>

#include "FT_IteratorBase.hpp"
#include <dbzero/core/serialization/Serializable.hpp>

namespace db0

{
    
    // FT_Iterator implementations should register here (for serialization)
    enum class FTIteratorType: std::uint16_t 
    {
        Invalid = 0,
        Index = 1,
        RangeTree = 2,
        JoinAnd = 3,
        JoinOr = 4,
        JoinAndNot = 5
    };
    
    using Serializable = db0::serial::Serializable;

    /**
     * Abstract DBZero inverted index iterator definition
     * NOTICE: IDs are not assigned automatically, when required need to call db0:assignUniqueIDs method
     */
	template <typename KeyT = std::uint64_t> class FT_Iterator: public FT_IteratorBase, public Serializable
	{
	public:
		using super_t = FT_IteratorBase;
	    using MutateFunction = std::function<std::pair<bool,bool>(FT_Iterator<KeyT> &)>;
		
        FT_Iterator() = default;

        /**
         * Retrieve currently iterated item's key
         * @return current item's key
         */
		virtual KeyT getKey() const = 0;
        
        const std::type_info &keyTypeId() const override;		

		/**
         * iterate single item forward
         */
		virtual void operator++() = 0;

		/**
         * iterate single item backward
         */
		virtual void operator--() = 0;
        
        /**
         * @direction should be +1 or -1
         * Note that join does not guarantee exact key match which needs to be checked separately
         * @return false if end of the iterator reached
         */        
		virtual bool join(KeyT join_key, int direction = -1) = 0;
		
		/**
         * join(direction=-1), dynamically excluding some of the sub-iterators
         * NOTICE: joinBound always iterates over some super-set of the underlying query
         * NOTICE: joinBound never iterates past the join_key (bound)
         * PERFORMANCE NOTICE: joinBound is the most efficient way of setting iterator at user requested position
         */
		virtual void joinBound(KeyT join_key) = 0;

		/**
         * NOTICE: iterator state not changed
         * @return key that would have been returned by join(direction=-1) and the result of the operation
        */
		virtual std::pair<KeyT, bool> peek(KeyT join_key) const = 0;
        
        // Look-up the next element without advancing the iterator
        // @return true if the next key exists & is identical as the current one
        virtual bool isNextKeyDuplicated() const = 0;
        
		/**
		 * Begin iteration as a typed FT_Iterator in a given direction,
         * the direction must be further preserved
		 * @param direction should be +1 or -1
		 * @return typed iterator instance
		 */
		virtual std::unique_ptr<FT_Iterator<KeyT> > beginTyped(int direction = -1) const = 0;

        std::unique_ptr<FT_IteratorBase> begin() const override;

		/**
         * limit query by the some margin key value (not to be reached)
         * NOTICE : this limit is neither cloned, nor serialized by queries
         * NOTICE : pass key = 0 to clear the existing limit
         * @return false if iterator is no longer valid within the limit specified
         */
		virtual bool limitBy(KeyT key) = 0;
        
        /**
         * Traverse query tree, run scan_function at each element (including this one)
         * @param scan_function
         * @param depth value to start from
         */
        virtual void scanQueryTree(std::function<void(const FT_Iterator<KeyT> *it_ptr, int depth)> scan_function,
            int depth = 0) const = 0;
        
        /**
         * Get depth of the query tree
         * depth = 1 means no nested queries, direct iterator
         * @return depth of the query tree spanned by this iterator
         */
        virtual std::size_t getDepth() const = 0;
        
        /**
         * Stop iteration, from this moment on the iterator will yield isEnd = true
         */
        virtual void stop() = 0;

        /**
         * Scan the whole query tree, continue until f returns false (or all nodes get iterated over)
         * The default implementation is provided and it only executes f for self
         * @return flag indicating if find was NOT stopped by the condition (i.e. f returned false)
         */
        virtual bool findBy(const std::function<bool(const FT_Iterator<KeyT> &)> &f) const {
            return f(*this);
        }

        /**
         * Mutate active inner iterator (of self), this may render the whole query tree invalid/end
         * NOTICE: can only mutate active iterator (i.e. the one which corresponds to current result)
         * f is expected to mutate only one iterator
         * f returns: was mutation performed (first) / is query iterator still valid (second)
         * @param f mutate function, returns true when iterator was mutated
         * @return was mutation performed (first) / is query tree still valid (second)
         *
         * Default implementation is provided and it mutates self
         */
        virtual std::pair<bool, bool> mutateInner(const MutateFunction &f) {
            return f(*this);
        }

        /**
         * Fetch this iterator's keys and feed them into provided function f (in ascending order)
         * Default implementation is provided using iterators, override for improved performance
         * This member can be used to copy entire contents of the iterator
         * @param f function to receive keys
         * @param batch_size suggested maximum batch size to use
         */
        virtual void fetchKeys(std::function<void(const KeyT *key_buf, std::size_t key_count)> f,
            std::size_t batch_size = 16 << 10) const;

        virtual void serialize(std::vector<std::byte> &) const override;

        virtual FTIteratorType getSerialTypeId() const = 0;

        bool isSimple() const override;
        
    protected:
        virtual void serializeFTIterator(std::vector<std::byte> &) const = 0;

        FT_Iterator(std::uint64_t uid);
	};
    
    extern template class FT_Iterator<std::uint64_t>;
    extern template class FT_Iterator<int>;
    
} // dbz namespace {
