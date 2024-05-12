#pragma once

#include <iosfwd>
#include <cstdint>
#include <typeinfo>
#include <limits>
#include <memory>
#include <vector>
#include <dbzero/core/serialization/Serializable.hpp>

namespace db0

{
    
    /**
     * Base class for all derived DBZero full-text inverted index iterators
     */
    class FT_IteratorBase
    {
    public:
        static constexpr std::size_t SIGNATURE_SIZE = db0::serial::Serializable::SIGNATURE_SIZE;

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
        
        /**
         * Measure similarity between the 2 query iterators
         * @retrun 0.0 if the iterators are identical, 1.0 if they are completely different
        */
        virtual double compareTo(const FT_IteratorBase &it) const;
        
        /**
         * Get (append) query iterator's signature for fast similarity lookup
         * each signature has a size of SIGNATURE_SIZE
        */
        virtual void getSignature(std::vector<std::byte> &) const = 0;

        std::vector<std::byte> getSignature() const;

    protected:
        virtual double compareToImpl(const FT_IteratorBase &it) const = 0;
    };
    
    /**
     * Sort signatures stored in a vector
    */
    void sortSignatures(std::vector<std::byte> &);
    void sortSignatures(std::byte *begin, std::byte *end);
    
}
