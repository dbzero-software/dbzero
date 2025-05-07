#pragma once

#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/serialization/micro_array.hpp>
#include <dbzero/core/serialization/unbound_array.hpp>
#include <dbzero/object_model/value/Value.hpp>
#include <dbzero/object_model/value/XValue.hpp>
#include <dbzero/object_model/value/StorageClass.hpp>

namespace db0::object_model

{

    /**
     * Positionally-encoded value table
    */
    class [[gnu::packed]] PosVT: public o_base<PosVT, 0, false>
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
        using super_t = o_base<PosVT, 0, false>;
        friend super_t;

        /**
         * Create empty value table
        */
        PosVT(std::size_t size);
        
        /**
         * Create fully populated value table
        */        
        PosVT(const std::vector<StorageClass> &types, const std::vector<Value> &values);
        
        PosVT(const Data &);

    public:

        inline o_micro_array<StorageClass> &types() {
            return getDynFirst(o_micro_array<StorageClass>::type());
        }

        inline const o_micro_array<StorageClass> &types() const {
            return getDynFirst(o_micro_array<StorageClass>::type());
        }
        
        o_unbound_array<Value> &values();

        const o_unbound_array<Value> &values() const;

        std::size_t size() const;
        
        std::size_t sizeOf() const;

        static std::size_t measure(const Data &);

        template <typename BufT> static std::size_t safeSizeOf(BufT buf)
        {
            auto start = buf;
            auto size = o_micro_array<StorageClass>::__const_ref(buf).size();
            buf += o_micro_array<StorageClass>::safeSizeOf(buf);
            buf += o_unbound_array<Value>::measure(size);
            return buf - start;
        }
        
        /**
         * Try finding element at a specific index
        */
        bool find(unsigned int index, std::pair<StorageClass, Value> &result) const;

        /**
         * Update element at a specific position / index
        */
        void set(unsigned int index, StorageClass, Value);

        bool operator==(const PosVT &other) const;
    };
    
    /**
     * Indexed value table
    */
    class [[gnu::packed]] IndexVT: public o_base<IndexVT, 0, false>
    {
    protected:
        using super_t = o_base<IndexVT, 0, false>;
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
            return o_micro_array<XValue>::safeSizeOf(buf);
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
    
}