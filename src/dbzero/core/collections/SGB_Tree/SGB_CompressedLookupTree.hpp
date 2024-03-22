#pragma once

/**
* A compressed SGB lookup tree enhances on SGB lookup tree by allowing 
* storage of node-relative elements (compressed) to save on storage space
*/

#include "SGB_LookupTree.hpp"
#include <dbzero/core/memory/AccessOptions.hpp>

namespace db0

{

    template <
        typename ItemT,
        typename KeyItemT,
        typename CapacityT, 
        typename AddressT, 
        typename ItemCompT,         
        typename ItemEqualT, 
        typename HeaderT,
        int D = 2>
    class [[gnu::packed]] o_sgb_compressed_lookup_tree_node:
    public o_ext<
        o_sgb_compressed_lookup_tree_node<ItemT, KeyItemT, CapacityT, AddressT, ItemCompT, ItemEqualT, HeaderT, D>, 
        o_sgb_lookup_tree_node<ItemT, CapacityT, AddressT, ItemCompT, ItemEqualT, HeaderT, D>, 0, false>
    {
    protected: 
        using ext_t = o_ext<
            o_sgb_compressed_lookup_tree_node<ItemT, KeyItemT, CapacityT, AddressT, ItemCompT, ItemEqualT, HeaderT, D>, 
            o_sgb_lookup_tree_node<ItemT, CapacityT, AddressT, ItemCompT, ItemEqualT, HeaderT, D>, 0, false>;
        using super_t = o_sgb_lookup_tree_node<ItemT, CapacityT, AddressT, ItemCompT, ItemEqualT, HeaderT, D>;
        using base_t = typename super_t::super_t;

    public: 
        using iterator = typename super_t::iterator;
        using const_iterator = typename super_t::const_iterator;
        
        o_sgb_compressed_lookup_tree_node(const KeyItemT &item, CapacityT capacity)
            : ext_t(capacity)
        {
            // make sure at least 1 item can be appended
            assert(capacity >= this->measureSizeOf(1));
            // initialize header by compressing the key item (first item in the node)
            // need to append with base, otherwise is_sorted_flag would be erased
            base_t::append(this->header().compressFirst(item));
        }

        static std::size_t measure(const KeyItemT &, CapacityT capacity) {
            return capacity;
        }

        /**
         * Note that type of the key item (uncompressed) is different from other items (compressed)
         */
        KeyItemT keyItem() const {
            return this->header().uncompress(super_t::keyItem());
        }
        
        iterator append(const KeyItemT &item) {
            return super_t::append(this->header().compress(item));
        }

        /**
         * Rebalance sorted node by moving items after "at" to the other node
         * and finally removing the "at" item
        */
        void rebalance_at(const_iterator at, o_sgb_compressed_lookup_tree_node &other) 
        {
            assert(this->is_sorted());
            if (this->is_reversed()) {
                rebalance_at<ReverseOperators<const_iterator> >(at, other);
            } else {
                rebalance_at<Operators<const_iterator> >(at, other);
            }
        }
        
    protected:
        
        template <typename op> void rebalance_at(const_iterator at, o_sgb_compressed_lookup_tree_node &other) 
        {
            assert(this->is_sorted());
            auto len = op::sub(this->cend(), at);
            op::next(at);
            other.append_sorted<op>(*this, at, this->cend());
            // remove elements including "at"
            this->m_size -= len;
        }

        template <typename op_src> void append_sorted(const o_sgb_compressed_lookup_tree_node &from, 
            const_iterator begin, const_iterator end) 
        {
            if (this->is_reversed()) {
                append_sorted<op_src, ReverseOperators<iterator> >(from, begin, end);
            } else {
                append_sorted<op_src, Operators<iterator> >(from, begin, end);
            }
        }

        template <typename op_src, typename op> void append_sorted(const o_sgb_compressed_lookup_tree_node &from,
            const_iterator begin, const_iterator end) 
        {            
            if (begin == end) {
                return;
            }
            
            const auto &from_head = from.header();
            auto &this_head = this->header();
            if (!this->is_sorted() || super_t::heapComp.itemComp(this_head.compress(from_head.uncompress(*begin)), *this->find_max())) {
                // must append one-by-one
                while (begin != end) {
                    // change element's base (uncompress / compress)
                    super_t::append(this_head.compress(from_head.uncompress(*begin)));
                    op_src::next(begin);
                }
            } else {
                // copy sorted elements
                auto len = op_src::sub(end, begin);
                auto out = this->end();
                while (begin != end) {
                    // change element's base
                    *out = this_head.compress(from_head.uncompress(*begin));
                    op_src::next(begin);
                    op::next(out);
                }
                this->m_size += len;
            }
        }
    };
    
    template <
        typename ItemType, 
        typename CompressedItemType,
        typename ItemCompType, 
        typename CompressedItemCompType, 
        typename ItemEqualType,
        typename CompressedItemEqualType,
        typename CapacityType = std::uint16_t, 
        typename AddressType = std::uint32_t, 
        typename HeaderType = db0::o_null,
        typename TreeHeaderType = db0::o_null>
    class sgb_compressed_lookup_types
    {
    public :
        using ItemT = ItemType;
        using CompressedItemT = CompressedItemType;
        using ItemCompT = ItemCompType;
        using CompressedItemCompT = CompressedItemCompType;
        using ItemEqualT = ItemEqualType;
        using CompressedItemEqualT = CompressedItemEqualType;
        using CapacityT = CapacityType;
        using AddressT = AddressType;
        using HeaderT = HeaderType;
        using TreeHeaderT = TreeHeaderType;
        // Nodes hold compressed elements
        using o_sgb_node_t = o_sgb_compressed_lookup_tree_node<CompressedItemT, ItemT, CapacityT, AddressT, CompressedItemCompT, CompressedItemEqualT, HeaderT>;
        // SG-Tree compares uncompressed elements
        using node_traits = sgb_node_traits<o_sgb_node_t, ItemT, ItemCompT>;
        using ptr_set_t = sgb_tree_ptr_set<AddressT>;
        using NodeT = SGB_IntrusiveNode<o_sgb_node_t, ItemT, ItemCompT, typename node_traits::comp_t, TreeHeaderT>;
        using CompT = typename NodeT::comp_t;
        
        using SG_TreeT = v_sgtree<NodeT, intrusive::detail::h_alpha_sqrt2_t>;
    };

    template <
        typename ItemT,
        typename CompressedItemT,
        typename CompressingHeaderT,
        typename ItemCompT = std::less<ItemT> ,
        typename CompressedItemCompT = std::less<CompressedItemT> ,
        typename ItemEqualT = std::equal_to<ItemT> ,
        typename CompressedItemEqualT = std::equal_to<CompressedItemT> ,
        typename TreeHeaderT = db0::o_null,
        typename CapacityT = std::uint16_t,
        typename AddressT = std::uint32_t >
    class SGB_CompressedLookupTree: 
    protected SGB_LookupTreeBase<
        sgb_compressed_lookup_types<
            ItemT, 
            CompressedItemT, 
            ItemCompT, 
            CompressedItemCompT, 
            ItemEqualT, 
            CompressedItemEqualT, 
            CapacityT, 
            AddressT, 
            CompressingHeaderT,
            TreeHeaderT> >
    {
    protected:
        using super_t = SGB_LookupTreeBase<
            sgb_compressed_lookup_types<
                ItemT, 
                CompressedItemT, 
                ItemCompT, 
                CompressedItemCompT, 
                ItemEqualT, 
                CompressedItemEqualT, 
                CapacityT, 
                AddressT, 
                CompressingHeaderT,
                TreeHeaderT> >;
        using base_t = typename super_t::base_t;

    public:
        using sg_tree_const_iterator = typename super_t::sg_tree_const_iterator;
        using ItemIterator = typename super_t::ItemIterator;
        using ConstItemIterator = typename super_t::ConstItemIterator;
        
        SGB_CompressedLookupTree(Memspace &memspace, std::size_t node_capacity, AccessType access_type,
            unsigned int sort_threshold = 3)
            : super_t(memspace, node_capacity, access_type, sort_threshold)            
        {
        }
        
        SGB_CompressedLookupTree(mptr ptr, std::size_t node_capacity, AccessType access_type,
            unsigned int sort_threshold = 3)
            : super_t(ptr, node_capacity, access_type, sort_threshold)            
        {
        }

        void insert(const ItemT &item)
        {
            assert(this->m_access_type == AccessType::READ_WRITE);
            if (base_t::empty()) {
                super_t::emplace_to_empty(item);
                return;
            }
            
            base_t::modify().m_sgb_size++;
            // Find the node by uncompressed key / item
            auto node = base_t::lower_equal_bound(item);
            if (node == base_t::end()) {
                node = base_t::begin();
            }
            
            insert_into(node, 0, item);
        }

        AddressT getAddress() const {
            return base_t::getAddress();
        }

        sg_tree_const_iterator cbegin_nodes() const {
            return base_t::begin();
        }

        sg_tree_const_iterator cend_nodes() const {
            return base_t::end();
        }
    
        std::size_t size() const {
            return super_t::size();
        }

        /**
         * Note that return type is different from the base class
        */
        template <typename KeyT> std::optional<ItemT> lower_equal_bound(const KeyT &key) const
        {
            auto node = base_t::lower_equal_bound(key);
            if (node == base_t::end()) {
                return std::nullopt;
            }
            
            // node will be sorted if needed (only if opened as READ/WRITE)
            if (this->m_access_type == AccessType::READ_WRITE) {
                this->onNodeLookup(node);
            }
            // within the node look up by compressed key
            auto item_ptr = node->lower_equal_bound(node->header().compress(key));
            if (!item_ptr) {
                return std::nullopt;
            }
            
            // return uncompressed
            return node->header().uncompress(*item_ptr);
        }

        const TreeHeaderT &treeHeader() const {
            return base_t::getData()->treeHeader();
        }

        TreeHeaderT &modifyTreeHeader() {
            return base_t::modify().treeHeader();
        }

        /**
         * Iterate over all items, mostly useful for debugging purposes
        */
        void forAll(const std::function<void(const ItemT &)> &f) const 
        {
            for (auto node = base_t::begin(); node != base_t::end(); ++node) {
                for (auto item = node->cbegin(); item != node->cend(); ++item) {
                    f(node->header().uncompress(*item));
                }
            }
        }

    private:        

        template <typename... Args> void insert_into(sg_tree_const_iterator &node, int recursion, const ItemT &item)
        {
            // Split node if full or unable to fit item
            if (node->isFull() || ((node->size() > 2) && (recursion < 2) && !node->header().canFit(item))) {
                // erase the max element and create the new node
                auto item_ptr = node.modify().find_middle();
                auto new_node = super_t::insert_equal(node->header().uncompress(*item_ptr), this->m_node_capacity);
                // rebalance the nodes around the middle item and remove the middle item from "node"
                node.modify().rebalance_at(item_ptr, new_node.modify());
                // append to either of the nodes
                if (!ItemCompT()(item, new_node->keyItem())) {
                    insert_into(new_node, recursion + 1, item);
                    return;
                }                
            }
            
            if (!node->header().canFit(item)) {
                // must insert a new node to be able to fit the new item
                auto new_node = super_t::insert_equal(item, this->m_node_capacity);
                new_node->begin();                
            } else {                
                node.modify().append(item);            
            }
        }

    };

}