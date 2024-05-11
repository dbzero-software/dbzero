#pragma once

#include <iosfwd>
#include <cstdint>
#include <typeinfo>
#include <limits>
#include <memory>

namespace db0

{
    
    /**
     * Base class for all derived DBZero full-text inverted index iterators
     */
    class FT_IteratorBase
    {
    public:
        virtual ~FT_IteratorBase() = default;

		/**
         * Test for end iterator
         * @return true if end of contents reached
         */
		virtual bool isEnd() const = 0;

        /**
         * Retrieve current key and move iterator's position to the next one
         * @param buf optional buffer for the item
         */        
        virtual void next(void *buf = nullptr) = 0;

        /**
         * Retrieve underlying key type id
        */
        virtual const std::type_info &keyTypeId() const = 0;

        /**
         * Type id of the concrete iterator instance
        */
        virtual const std::type_info &typeId() const = 0;

        /**
         * Dump basic iterator information, debug & evaluation member
         */
        virtual std::ostream &dump(std::ostream &os) const = 0;

        /** 
         * compare objects of the same type, return false otherwise
         */
        virtual bool equal(const FT_IteratorBase &it) const = 0;

        /**
         * Start iteration over without any direction specified
        */
        virtual std::unique_ptr<FT_IteratorBase> begin() const = 0;
        
        /**
         * Find "equal" iterator or sub-iterator
         * @return native iterator or NULL if not found
         * NOTICE: default implementation for simple iterators provided
         */
        virtual const FT_IteratorBase *find(const FT_IteratorBase &it) const;
        
        /**
         * Returns "false" by default, should be overridden by simple iterator implementations
         * such as single-tag iterators
         * @return flag indicating if this is a simple iterator (i.e. the iterator which may represent parameter value)
        */
        virtual bool isSimple() const;
    };
    
}
