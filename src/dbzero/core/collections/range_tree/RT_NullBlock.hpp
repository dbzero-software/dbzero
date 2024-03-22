#pragma once

namespace db0

{

    /**
     * A range-tree block to keep null keys only
    */
    template <typename ValueT> class RT_NullBlock: public v_bindex<ValueT, std::uint64_t>
    {
        using super_t = v_bindex<ValueT, std::uint64_t>;
    public:
        using PtrT = db0_ptr<RT_NullBlock<ValueT> >;        
        using FT_IteratorT = FT_IndexIterator<super_t, ValueT>;

        RT_NullBlock() = default;
        RT_NullBlock(Memspace &memspace)
            : super_t(memspace)
        {
        }

        RT_NullBlock(mptr ptr)
            : super_t(ptr)
        {
        }

        std::unique_ptr<FT_IteratorT> makeIterator() const
        {            
            return std::make_unique<FT_IteratorT>(*this, -1);
        }
    };
    
}