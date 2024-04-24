#pragma once

#include <optional>
#include "RangeTree.hpp"
#include <dbzero/core/collections/full_text/IteratorFactory.hpp>
#include <dbzero/workspace/Snapshot.hpp>
#include "RT_RangeIterator.hpp"
#include "RT_Range.hpp"
#include "RT_FTIterator.hpp"

namespace db0

{

    /**
     * RangeTree ItertorFactory specialization
    */
    template <typename KeyT, typename ValueT>
    class RangeIteratorFactory: public IteratorFactory<ValueT>
    {
    public:
        using RT_TreeT = RangeTree<KeyT, ValueT>;

        // deserialization constructor
        RangeIteratorFactory(Snapshot &snapshot, std::vector<std::byte>::const_iterator &iter, 
            std::vector<std::byte>::const_iterator end)
            : m_tree(getRTTree(snapshot, iter, end))
            , m_range(iter, end)
            , m_nulls_first(db0::serial::read<decltype(m_nulls_first)>(iter, end))
        {
        }
        
        RangeIteratorFactory(const RT_TreeT &tree, std::optional<KeyT> min = {},
            bool min_inclusive = false, std::optional<KeyT> max = {}, bool max_inclusive = false, bool nulls_first = false)
            : m_tree(tree)
            , m_range { min, min_inclusive, max, max_inclusive }
            , m_nulls_first(nulls_first)
        {
        }

        RangeIteratorFactory(const RT_TreeT &tree, const RT_Range<KeyT> &range, bool nulls_first)
            : m_tree(tree)
            , m_range(range)
            , m_nulls_first(nulls_first)
        {
        }

        std::unique_ptr<FT_IteratorBase> createBaseIterator() override;

        std::unique_ptr<FT_Iterator<ValueT> > createFTIterator() override;

        IteratorFactoryTypeId getSerialTypeId() const override;

    protected:
        void serializeImpl(std::vector<std::byte> &) const override;
        
    private:
        RT_TreeT m_tree;
        const RT_Range<KeyT> m_range;
        const bool m_nulls_first;

        static RT_TreeT getRTTree(Snapshot &, std::vector<std::byte>::const_iterator &, std::vector<std::byte>::const_iterator end);
    };

    template <typename KeyT, typename ValueT> std::unique_ptr<FT_IteratorBase>
    RangeIteratorFactory<KeyT, ValueT>::createBaseIterator()
    {
        return std::make_unique<RT_RangeIterator<KeyT, ValueT>>(m_tree, m_range.m_min, m_range.m_min_inclusive,
            m_range.m_max, m_range.m_max_inclusive, m_nulls_first);
    }

    template <typename KeyT, typename ValueT> std::unique_ptr<FT_Iterator<ValueT> >
    RangeIteratorFactory<KeyT, ValueT>::createFTIterator()
    {        
        return std::make_unique<RT_FTIterator<KeyT, ValueT>>(m_tree, m_range.m_min, m_range.m_min_inclusive,
            m_range.m_max, m_range.m_max_inclusive, m_nulls_first);
    }
    
    template <typename KeyT, typename ValueT> IteratorFactoryTypeId
    RangeIteratorFactory<KeyT, ValueT>::getSerialTypeId() const
    {
        return IteratorFactoryTypeId::Range;
    }

    template <typename KeyT, typename ValueT> void
    RangeIteratorFactory<KeyT, ValueT>::serializeImpl(std::vector<std::byte> &v) const
    {
        // store underlying typeId-s
        db0::serial::write(v, db0::serial::typeId<KeyT>());
        db0::serial::write(v, db0::serial::typeId<ValueT>());
        db0::serial::write(v, m_tree.getMemspace().getUUID());
        db0::serial::write(v, m_tree.getAddress());
        m_range.serialize(v);
        db0::serial::write<bool>(v, m_nulls_first);
    }
    
    template <typename KeyT, typename ValueT> std::unique_ptr<RangeIteratorFactory<KeyT, ValueT> >
    deserializeRangeIteratorFactory(Snapshot &workspace, std::vector<std::byte>::const_iterator &iter,
        std::vector<std::byte>::const_iterator end)
    {
        using TypeIdType = decltype(db0::serial::typeId<void>());
        auto key_type_id = db0::serial::read<TypeIdType>(iter, end);        
        if (key_type_id != db0::serial::typeId<KeyT>()) {
            THROWF(db0::InternalException) << "Key type mismatch: " << key_type_id << " != " 
                << db0::serial::typeId<KeyT>() << THROWF_END;
        }
        auto value_type_id = db0::serial::read<TypeIdType>(iter, end);
        if (value_type_id != db0::serial::typeId<ValueT>()) {
            THROWF(db0::InternalException) << "Value type mismatch: " << value_type_id << " != " 
                << db0::serial::typeId<ValueT>() << THROWF_END;
        }
        auto fixture_uuid = db0::serial::read<std::uint64_t>(iter, end);
        auto fixture = workspace.getFixture(fixture_uuid);
        auto addr = db0::serial::read<std::uint64_t>(iter, end);
        auto range = RT_Range<KeyT>::deserialize(iter, end);
        auto nulls_first = db0::serial::read<bool>(iter, end);

        using RT_TreeT = RangeTree<KeyT, ValueT>;
        return std::make_unique<RangeIteratorFactory<KeyT, ValueT>>(RT_TreeT { fixture->myPtr(addr) }, range, nulls_first);
    }

}
