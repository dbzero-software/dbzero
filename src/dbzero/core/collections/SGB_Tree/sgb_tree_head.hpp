#pragma once

#include <dbzero/core/serialization/Base.hpp>
#include <dbzero/core/serialization/Types.hpp>

namespace db0

{

    // SG-Tree node / head node pointers
    template <class PtrT> struct [[gnu::packed]] sgb_tree_ptr_set
    {
        using pointer_type = PtrT;
        PtrT parent = 0;
        PtrT left = 0;
        PtrT right = 0;

        sgb_tree_ptr_set() = default;

        inline PtrT getLeft() const {
            return left;
        }

        inline PtrT getRight() const {
            return right;
        }

        inline PtrT getParent() const {
            return parent;
        }
    };
    
    /**
     * The SGB_Tree head node, compatible with the general SGB_Tree node type
     * @tparam CapacityT the type of the capacity field
     * @tparam AddressT the type of the address field
     * @tparam TreeHeaderT optional user defined tree header (to store additional custom tree level data)
    */
    template <typename CapacityT, typename AddressT, typename TreeHeaderT>
    class [[gnu::packed]] o_sgb_tree_head: public o_base<o_sgb_tree_head<CapacityT, AddressT, TreeHeaderT>, 0, false>
    {
    public:
        // tree pointers (possibly relative to slab)
        sgb_tree_ptr_set<AddressT> ptr_set;
        // total number of node allocated bytes
        CapacityT m_capacity;
        // number of nodes in the SG-Tree (i.e. the number of allocated nodes)
        AddressT size = 0;
        AddressT max_tree_size = 0;
        // number of elements in the SGB-Tree
        AddressT m_sgb_size = 0;

        o_sgb_tree_head(CapacityT capacity)
            : m_capacity(capacity)
        {
            // initialize header with default arguments
            this->arrangeMembers()
                (TreeHeaderT::type());
        }

        // Notice: header stored as variable-length to allow 0-bytes type (default)
        inline TreeHeaderT &treeHeader() {
            return this->getDynFirst(TreeHeaderT::type());
        }

        inline const TreeHeaderT &treeHeader() const {
            return this->getDynFirst(TreeHeaderT::type());
        }

        static std::size_t measure(CapacityT capacity) {
            return capacity;
        }

        std::size_t sizeOf() const {
            // size of the object equals the capacity
            return m_capacity;
        }

        template <typename buf_t> static std::size_t safeSizeOf(buf_t at)
        { 
            auto buf = at;
            buf += o_sgb_tree_head::__const_ref(at).m_capacity;
            return buf - at;
        }
        
        /// The actual useful space occupied by the object
        std::size_t trueSizeOf() const {
            return sizeof(o_sgb_tree_head) + TreeHeaderT::sizeOf();
        }
    };
    
}