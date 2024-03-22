#pragma once

#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/core/collections/vector/v_bvector.hpp>
#include <dbzero/object_model/pandas/Block.hpp>
#include <dbzero/object_model/object_header.hpp>
#include <dbzero/core/memory/Memspace.hpp>
#include <dbzero/core/vspace/db0_ptr.hpp>

namespace db0 

{

    class Fixture;

}

namespace db0::object_model::pandas

{

    using Fixture = db0::Fixture;
    class Block;
    class List;

    struct [[gnu::packed]] o_data_frame: public db0::o_fixed<o_data_frame>
    {
        db0::o_object_header m_header;
        db0::db0_ptr<db0::v_bvector<Value>> m_blocks;
        db0::db0_ptr<db0::v_bvector<Value>> m_indexes;

        inline o_data_frame(Memspace &memspace)
            : m_blocks(db0::v_bvector<Value>(memspace))
            , m_indexes(db0::v_bvector<Value>(memspace)){
        }
    };
    
    class DataFrame: public db0::ObjectBase<DataFrame, v_object<o_data_frame>, StorageClass::DB0_PANDAS_DATAFRAME>
    {
        GC0_Declare        
    public:
        using super_t = db0::ObjectBase<DataFrame, v_object<o_data_frame>, StorageClass::DB0_PANDAS_DATAFRAME>;
        using LangToolkit = db0::python::PyToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;

        DataFrame(db0::swine_ptr<Fixture> &);

        DataFrame(db0::swine_ptr<Fixture> &, std::uint64_t address);

        DataFrame::ObjectSharedPtr getBlock(std::size_t i) const;
        
        static DataFrame *makeNew(void *at_ptr, db0::swine_ptr<Fixture> &);
        void setBlock(Py_ssize_t i, ObjectPtr block_value);
        void appendBlock(ObjectPtr block_value);
        void appendIndex(ObjectPtr index_value);
        void detach();

    private:        
        db0::v_bvector<Value> m_frame_index;
        db0::v_bvector<Value> m_blocks;
    };
    
}