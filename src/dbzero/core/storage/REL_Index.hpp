#pragma once

#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/compiler_attributes.hpp>
#include <dbzero/core/collections/vector/v_bvector.hpp>
#include <dbzero/core/dram/DRAM_Prefix.hpp>
#include <dbzero/core/dram/DRAM_Allocator.hpp>
#include <dbzero/core/collections/SGB_Tree/SGB_CompressedLookupTree.hpp>

namespace db0

{

    struct REL_Item;
    struct REL_CompressedItem;

    // Options to additionally annotate REL_Index elements (i.e. continuous Page-IO steps)
    // this might be usefull for maintaining different classess of data (e.g. metadata vs no-cache data)
    enum class REL_Options: std::uint8_t
    {
    };

    using REL_Flags = FlagSet<REL_Options>;

    struct REL_ItemCompT
    {
        bool operator()(const REL_Item &lhs, const REL_Item &rhs) const;
        bool operator()(const REL_Item &lhs, std::uint64_t rhs) const;
        bool operator()(std::uint64_t lhs, const REL_Item &rhs) const;
    };

    struct REL_ItemEqualT
    {
        bool operator()(const REL_Item &lhs, const REL_Item &rhs) const;
        bool operator()(const REL_Item &lhs, std::uint64_t rhs) const;
        bool operator()(std::uint64_t lhs, const REL_Item &rhs) const;
    };

DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR REL_Item
    {
        using CompT = REL_ItemCompT;
        using EqualT = REL_ItemEqualT;

        // the starting relative page number
        std::uint64_t m_rel_page_num = 0;
        // the starting storage page number (absolute)
        std::uint64_t m_storage_page_num = 0;
        REL_Flags m_flags;
        
        REL_Item() = default;
        
        REL_Item(std::uint64_t rel_page_num, std::uint64_t storage_page_num, REL_Flags flags = {})
            : m_rel_page_num(rel_page_num)
            , m_storage_page_num(storage_page_num)
            , m_flags(flags)
        {                
        }
        
        bool operator==(const REL_Item &) const;
        std::string toString() const;
    };
DB0_PACKED_END

    struct REL_CompressedItemCompT
    {
        bool operator()(const REL_CompressedItem &, const REL_CompressedItem &) const;
    };
    
    struct REL_CompressedItemEqualT
    {
        bool operator()(const REL_CompressedItem &, const REL_CompressedItem &) const;
    };
    
    // Compressed items are actual in-memory representation
DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR REL_CompressedItem
    {
        using CompT = REL_CompressedItemCompT;
        using EqualT = REL_CompressedItemEqualT;
        // construct REL-compressed item relative to the specific page number - i.e. first_page_num
        REL_CompressedItem(std::uint32_t first_rel_page_num, const REL_Item &);
        REL_CompressedItem(std::uint32_t first_rel_page_num, std::uint64_t rel_page_num, 
            std::uint64_t storage_page_num, REL_Flags flags = {});
        
        std::uint32_t m_compressed_rel_page_num;
        std::uint64_t m_storage_page_num;
        REL_Flags m_flags;
        
        // uncompress relative to a specific page number
        REL_Item uncompress(std::uint32_t first_rel_page_num) const;        
        std::string toString() const;
    };
DB0_PACKED_END

    struct REL_IndexTypes
    {
    DB0_PACKED_BEGIN
        // tree-level header type (currently unused)
        struct DB0_PACKED_ATTR o_rel_index_header: o_fixed_versioned<o_rel_index_header>
        {
            // the largest registered mapping from absolute page number
            std::uint64_t m_last_storage_page_num = 0;
            // relative page number associated with the 
            std::uint64_t m_rel_page_num = 0;
            // the maximum assigned relative page number
            std::uint64_t m_max_rel_page_num = 0;
            // reserved space for future use
            std::array<std::uint64_t, 4> m_reserved = {0, 0, 0, 0};
        };
DB0_PACKED_END

        using ItemT = REL_Item;
        using CompressedItemT = REL_CompressedItem;

        struct BlockHeader
        {
            // number of the 1st page in a data block / (high order bits)
            std::uint32_t m_first_page_num = 0;

            CompressedItemT compressFirst(const ItemT &);            
            CompressedItemT compress(const ItemT &) const;
            // compress for comparison only
            CompressedItemT compress(std::uint64_t rel_page_num) const;

            ItemT uncompress(const CompressedItemT &) const;

            // From a compressed item, retrieve the (relative) page number only
            std::uint64_t getRelPageNum(const CompressedItemT &) const;
            
            bool canFit(const ItemT &) const;
            bool canFit(std::uint64_t rel_page_num) const;

            std::string toString(const CompressedItemT &) const;
            std::string toString() const;
        };

        // DRAM space deployed REL-index (in-memory)
        using IndexT = SGB_CompressedLookupTree<
            REL_Item, REL_CompressedItem, BlockHeader,
            REL_ItemCompT, REL_CompressedItemCompT, REL_ItemEqualT, REL_CompressedItemEqualT,
            o_rel_index_header>;
        
        using ConstNodeIterator = typename IndexT::sg_tree_const_iterator;
        using ConstItemIterator = typename IndexT::ConstItemIterator;
    };

    // REL_Index holds a complete mapping from relative to absolute Page IO addresses
    // (aka storage page numbers)
    // it only holds the location of the entire ranges of blocks, assuming consecutive following numbers
    class REL_Index: protected REL_IndexTypes::IndexT
    {
    public:
        using super_t = REL_IndexTypes::IndexT;
        
        // as null
        REL_Index() = default;
        REL_Index(const REL_Index &) = delete;
        REL_Index(Memspace &, std::size_t node_capacity, AccessType);
        REL_Index(mptr, std::size_t node_capacity, AccessType);
        
        // Assign (append) a mapping from an absolute to relative page number
        // NOTE: the mapping needs to be persisted for each "first_in_step" page
        std::uint64_t assignRelative(std::uint64_t storage_page_num, bool is_first_in_step);
        
        // Retrieve storage (absolute) page num for a given relative page num
        std::uint64_t getAbsolute(std::uint64_t rel_page_num) const;
        
        db0::Address getAddress() const;
        
        // Registers a new mapping rel_page_num -> storage_page_num
        // exception raised if unable to add the mapping
        // the method is used by copy_prefix
        void addMapping(std::uint64_t storage_page_num, std::uint64_t rel_page_num);
        
        void detach() const;
        void commit() const;
        
        void refresh();

        std::uint64_t size() const;

    private:
        // values maintained in-sync with the tree
        std::uint64_t m_last_storage_page_num = 0;            
        std::uint64_t m_rel_page_num = 0;        
        std::uint64_t m_max_rel_page_num = 0;
    };
    
}

namespace std

{

    ostream &operator<<(ostream &, const db0::REL_Item &);

}