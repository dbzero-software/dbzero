#pragma once

#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/object_model/value/StorageClass.hpp>
#include <dbzero/object_model/value/Value.hpp>
#include <dbzero/object_model/ObjectBase.hpp>    
#include <dbzero/core/serialization/micro_array.hpp>
#include <dbzero/object_model/item/Item.hpp>
#include <dbzero/workspace/GC0.hpp>

namespace db0
{

    class Fixture;

}

namespace db0::object_model

{

    using Fixture = db0::Fixture;
    
    struct [[gnu::packed]] o_tuple: public o_base<o_tuple, 0, false>
    {
        // common object header
        o_object_header m_header;

        o_tuple(std::size_t size)
        {
            arrangeMembers()
            (o_micro_array<o_typed_item>::type(), size).ptr();
        }

        inline o_micro_array<o_typed_item> &items()
        {
            return getDynFirst(o_micro_array<o_typed_item>::type());
        }

        inline const o_micro_array<o_typed_item> &items() const
        {
            return getDynFirst(o_micro_array<o_typed_item>::type());
        }

        std::size_t size() const {
            return items().size();
        }
        
        std::size_t sizeOf() const{
            return sizeOfMembers()
            (o_micro_array<o_typed_item>::type());
        }

        static std::size_t measure(std::size_t size){
            return measureMembers()
            (o_micro_array<o_typed_item>::measure(size));
        }

        template <typename BufT> static std::size_t safeSizeOf(BufT buf)
        {
            auto start = buf;
            auto size = o_micro_array<o_typed_item>::__const_ref(buf).size();
            buf += o_micro_array<o_typed_item>::safeSizeOf(buf);
            return buf - start;
        }
    };

    class Tuple: public db0::ObjectBase<Tuple, v_object<o_tuple >, StorageClass::DB0_TUPLE>
    {
        GC0_Declare
    public:
        using super_t = db0::ObjectBase<Tuple, v_object<o_tuple>, StorageClass::DB0_TUPLE>;
        using LangToolkit = db0::python::PyToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
        using iterator = const o_typed_item *;

        ObjectSharedPtr getItem(std::size_t i) const;
        void setItem(FixtureLock &, std::size_t i, ObjectPtr lang_value);
        
        static Tuple *makeNew(void *at_ptr, db0::swine_ptr<Fixture> &, std::size_t size);
        static Tuple *unload(void *at_ptr, db0::swine_ptr<Fixture> &, std::uint64_t address);
        size_t count(ObjectPtr lang_value);
        size_t index(ObjectPtr lang_value);
        size_t size() const;
        // operators
        bool operator==(const Tuple &) const;
        bool operator!=(const Tuple &) const;
        // drop underlying DBZero representation
        void drop();

        const o_typed_item * begin();
        
        const o_typed_item * end();
    private:
        // new Tuples can only be created via factory members
        Tuple(std::size_t size, db0::swine_ptr<Fixture> &);
        Tuple(db0::swine_ptr<Fixture> &, std::uint64_t address);    
        
    };
    
}