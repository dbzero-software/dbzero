#pragma once

#include "v_sgtree.hpp"
#include <dbzero/core/serialization/Ext.hpp>
#include <dbzero/core/vspace/v_object.hpp>

namespace db0

{
	
    template <class data_t,class ptr_set_t = tree_ptr_set<std::uint64_t> > class [[gnu::packed]] v_sgtree_node
        : public o_ext<v_sgtree_node<data_t, ptr_set_t>, sg_node_base<ptr_set_t>, 0, false >
    {
    public :
        typedef typename data_t::Initializer Initializer;
        using has_constant_size = typename data_t::has_constant_size;
        
        // overlaid constructor by initializer
        static v_sgtree_node &__new(std::byte *buf, const Initializer &data) {
            std::byte *_buf = buf;
            buf += sg_node_base<ptr_set_t>::__new(_buf).sizeOf();
            data_t::__new(buf,data);
            return (v_sgtree_node<data_t,ptr_set_t>&)(*_buf);
        }
        
        static std::size_t measure(const Initializer &data) {
            std::size_t size = sg_node_base<ptr_set_t>::sizeOf();
            size += data_t::measure(data);
            return size;
        }

        template <typename = typename std::enable_if<has_constant_size::value>::type>
        static std::size_t measure () {
            std::size_t size = sg_node_base<ptr_set_t>::sizeOf();
            size += data_t::measure();
            return size;
        }
        
        template <class buf_t> static size_t safeSizeOf(buf_t buf) {
            std::size_t size = sg_node_base<ptr_set_t>::sizeOf();
            size += data_t::safeSizeOf(&buf[size]);
            return size;
        }
        
        void destroy(Memspace &memspace) const {
            data.destroy(memspace);
        }
        
        const data_t *operator->() const {
            return &data;
        }
                
    public :
        data_t data;
    };
        
    template <class data_t,class data_comp_t> class v_sgtree_node_traits {
    public :
        typedef typename data_t::Initializer Initializer;
        typedef typename v_object<v_sgtree_node<data_t> >::ptr_t node_ptr_t;
        
        struct comp_t
        {
            data_comp_t _comp;
            comp_t(data_comp_t _comp = data_comp_t())
                : _comp(_comp)
            {
            }

            bool operator()(const node_ptr_t &n0,const node_ptr_t &n1) const {
                return _comp(n0->data,n1->data);
            }

            bool operator()(const Initializer &d0,const node_ptr_t &n1) const {
                return _comp(d0,n1->data);
            }		

            bool operator()(const node_ptr_t &n0,const Initializer &d1) const {
                return _comp(n0->data,d1);
            }

            bool operator()(const Initializer &d0,const Initializer &d1) const {
                return _comp(d0,d1);
            }
        };
    };
    
}
	