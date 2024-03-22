#pragma once

#include "v_sgtree.hpp"
#include <dbzero/core/intrusive/base_traits.hpp>
#include <dbzero/core/vspace/v_ptr.hpp>
	
namespace db0 

{
	        
    /**
     * VSPACE node type compliant with intrusive containers
     * c_type - node container type
     * comp_t - node pointer comparer type
     */
    template <typename T,class comp_t_,class ptr_set_t = tree_ptr_set<std::uint64_t> > class intrusive_node
        : public v_object<T>
    {
    public :
        using super = v_object<T>;
        using c_type = T;
        using ptr_t = typename super::ptr_t;
        using comp_t = comp_t_;
        // type compliant with intrusive NodeTraits requirements
        using traits_t = base_traits_t<intrusive_node<c_type,comp_t>, ptr_t>;
        using tree_base_t = v_object<sg_tree_data<true_size_of<c_type>(), ptr_set_t> >;
        
        template <typename... Args> intrusive_node(Memspace &memspace, Args&&... args)
            : super(memspace, std::forward<Args>(args)...)
        {
        }

        intrusive_node(const ptr_t &ptr)
            : super(ptr)
        {
        }

        /**
         * Cast to pointer
         */
        inline operator ptr_t&()
        {
            return this->v_this;
        }
        
        /**
         * Cast to const-pointer
         */
        inline operator const ptr_t&() const
        {
            return this->v_this;
        }
    };

}
