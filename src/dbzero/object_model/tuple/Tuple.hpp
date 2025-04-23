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
    
    class [[gnu::packed]] o_tuple: public o_base<o_tuple, 0, false>
    {
    protected:
        using super_t = o_base<o_tuple, 0, false>;
        friend super_t;

        o_tuple(std::size_t size);

    public:         
        // common object header
        o_unique_header m_header;

        inline o_micro_array<o_typed_item> &items() {
            return getDynFirst(o_micro_array<o_typed_item>::type());
        }
        
        inline const o_micro_array<o_typed_item> &items() const {
            return getDynFirst(o_micro_array<o_typed_item>::type());
        }

        std::size_t size() const;
        std::size_t sizeOf() const;

        static std::size_t measure(std::size_t size);

        template <typename BufT> static std::size_t safeSizeOf(BufT buf)
        {
            auto start = buf;
            buf += super_t::baseSize();            
            buf += o_micro_array<o_typed_item>::safeSizeOf(buf);
            return buf - start;
        }
    };
    
    class Tuple: public db0::ObjectBase<Tuple, v_object<o_tuple>, StorageClass::DB0_TUPLE>
    {
        GC0_Declare
    public:
        using super_t = db0::ObjectBase<Tuple, v_object<o_tuple>, StorageClass::DB0_TUPLE>;
        friend super_t;
        using LangToolkit = db0::python::PyToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
        using const_iterator = const o_typed_item *;

        explicit Tuple(db0::swine_ptr<Fixture> &, Address address);
        ~Tuple();

        ObjectSharedPtr getItem(std::size_t i) const;
        void setItem(FixtureLock &, std::size_t i, ObjectPtr lang_value);
        
        static Tuple *makeNew(void *at_ptr, db0::swine_ptr<Fixture> &, std::size_t size);
        static Tuple *unload(void *at_ptr, db0::swine_ptr<Fixture> &, Address);

        std::size_t count(ObjectPtr lang_value) const;
        std::size_t index(ObjectPtr lang_value) const;
        std::size_t size() const;

        // operators
        bool operator==(const Tuple &) const;
        void operator=(Tuple &&);
        bool operator!=(const Tuple &) const;
        
        void destroy() const;

        const o_typed_item *begin() const;
        const o_typed_item *end() const;

        void moveTo(db0::swine_ptr<Fixture> &);

    private:
        // new Tuples can only be created via factory members
        struct tag_new_tuple {};
        explicit Tuple(db0::swine_ptr<Fixture> &, tag_new_tuple, std::size_t size);
        explicit Tuple(tag_no_gc, db0::swine_ptr<Fixture> &, const Tuple &);
    };
    
}