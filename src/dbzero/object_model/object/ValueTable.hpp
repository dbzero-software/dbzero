// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/serialization/micro_array.hpp>
#include <dbzero/core/serialization/unbound_array.hpp>
#include <dbzero/object_model/value/Value.hpp>
#include <dbzero/object_model/value/XValue.hpp>
#include <dbzero/object_model/value/StorageClass.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0::object_model

{

DB0_PACKED_BEGIN
    /**
     * Positionally-encoded value table
    */
    class DB0_PACKED_ATTR PosVT: public o_base<PosVT, 0, true>
    {
    public:

        struct Data
        {
            Data() = default;
            Data(std::size_t size);

            std::vector<StorageClass> m_types;
            std::vector<Value> m_values;

            void clear();
            bool empty() const;
            std::size_t size() const;
        };

    protected: 
        using super_t = o_base<PosVT, 0, true>;
        friend super_t;
        
        // Create empty value table
        PosVT(std::size_t size, unsigned int offset);                
        // Create fully populated value table
        PosVT(const std::vector<StorageClass> &types, const std::vector<Value> &values, unsigned int offset);
        PosVT(const Data &, unsigned int offset);
        
    public:
        using TypesArrayT = o_micro_array<StorageClass, true>;

        inline TypesArrayT &types() {
            return getDynFirst(TypesArrayT::type());
        }

        inline const TypesArrayT &types() const {
            return getDynFirst(TypesArrayT::type());
        }
        
        o_unbound_array<Value> &values();

        const o_unbound_array<Value> &values() const;

        std::size_t size() const;
        unsigned int offset() const;

        static std::size_t measure(const Data &, unsigned int offset);

        template <typename BufT> static std::size_t safeSizeOf(BufT buf)
        {
            std::size_t size = super_t::__const_ref(buf).size();
            return super_t::sizeOfMembers(buf)
                (TypesArrayT::type())
                (o_unbound_array<Value>::measure(size));
        }
        
        // Try finding element with a specific index
        // NOTE: index if the actual members number, not its position
        bool find(unsigned int index, std::pair<StorageClass, Value> &result) const;
        // Translate index to position or return false if not found / invalid index
        bool find(unsigned int index, unsigned int &pos) const;
        
        // Set or update element at a specific known position (not index !)
        void set(unsigned int pos, StorageClass, Value);

        bool operator==(const PosVT &other) const;
    };
DB0_PACKED_END

    /**
     * Indexed value table
    */
DB0_PACKED_BEGIN
    class DB0_PACKED_ATTR IndexVT: public o_base<IndexVT, 0, true>
    {
    protected:
        using super_t = o_base<IndexVT, 0, true>;
        friend super_t;

        /**
         * Create fully populated value table
        */        
        IndexVT(const XValue *begin = nullptr, const XValue *end = nullptr);
        
    public:

        inline o_micro_array<XValue> &xvalues() {
            return getDynFirst(o_micro_array<XValue>::type());
        }

        inline const o_micro_array<XValue> &xvalues() const {
            return getDynFirst(o_micro_array<XValue>::type());
        }
        
        static std::size_t measure(const XValue *begin = nullptr, const XValue *end = nullptr);
        
        template <typename BufT> static std::size_t safeSizeOf(BufT buf) {
            return super_t::sizeOfMembers(buf)
                (o_micro_array<XValue>::type());
        }
        
        /**
         * Try finding element by its index
         * @return the element's value if found
        */
        bool find(unsigned int index, std::pair<StorageClass, Value> &result) const;

        /**
         * Try finding element by its index
         * @return the element's position if found
        */
        bool find(unsigned int index, unsigned int &pos) const;
        
        /**
         * Update element at a specifc position
        */
        void set(unsigned int pos, StorageClass, Value);

        bool operator==(const IndexVT &other) const;
    };
    
DB0_PACKED_END

}