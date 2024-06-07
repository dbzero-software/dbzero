#pragma once

#include <cstdint>
#include <dbzero/object_model/ObjectBase.hpp>
#include <dbzero/core/vspace/db0_ptr.hpp>
#include <dbzero/core/collections/range_tree/RangeTree.hpp>
#include <dbzero/core/collections/range_tree/RT_SortIterator.hpp>
#include <dbzero/core/collections/range_tree/RT_RangeIterator.hpp>
#include <dbzero/core/collections/range_tree/RangeIteratorFactory.hpp>
#include <dbzero/core/utils/shared_void.hpp>
#include <dbzero/workspace/GC0.hpp>
#include <dbzero/core/exception/AbstractException.hpp>
#include <dbzero/object_model/object_header.hpp>
#include "IndexBuilder.hpp"

namespace db0::object_model

{

    // range-tree based index
    using RT_IndexInt = db0::RangeTree<std::int64_t, std::uint64_t>;
    class ObjectIterator;

    // known index implementations
    enum class IndexType: std::uint16_t
    {
        Unknown = 0,
        RangeTree = 1
    };

    enum class IndexDataType: std::uint16_t
    {
        Unknown = 0,
        // type will be auto-assigned on first non-null element added
        Auto = 1,
        Int64 = 2,
        UInt64 = 3
    };

    struct [[gnu::packed]] o_index: public o_fixed<o_index>
    {
        // common object header
        o_object_header m_header;
        const std::uint32_t m_instance_id;
        IndexType m_type;
        IndexDataType m_data_type;
        // address of the actual index instance
        std::uint64_t m_index_addr = 0;
        std::array<std::uint64_t, 2> m_reserved;
        
        o_index(std::uint32_t instance_id, IndexType type, IndexDataType data_type)
            : m_instance_id(instance_id)
            , m_type(type)
            , m_data_type(data_type)
        {
        }
    };
    
    class Index: public db0::ObjectBase<Index, db0::v_object<o_index>, StorageClass::DB0_INDEX>
    {
        GC0_Declare
        using super_t = db0::ObjectBase<Index, db0::v_object<o_index>, StorageClass::DB0_INDEX>;
    public:
        using LangToolkit = db0::python::PyToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
        using TypeId = db0::bindings::TypeId;
        using IteratorFactory = db0::IteratorFactory<std::uint64_t>;
                
        Index(db0::swine_ptr<Fixture> &, std::uint64_t address);

        static Index *makeNew(void *at_ptr, db0::swine_ptr<Fixture> &);
        static Index *unload(void *at_ptr, db0::swine_ptr<Fixture> &, std::uint64_t address);
        
        std::size_t size() const;
        void add(ObjectPtr key, ObjectPtr value);
        void remove(ObjectPtr key, ObjectPtr value);
        
        /**
         * Sort results of a specific object iterator from the same fixture
         * @param iter object iterator        
         */        
        std::unique_ptr<db0::SortedIterator<std::uint64_t> > sort(const ObjectIterator &iter) const;

        /**
         * Construct a range filtering query        
         * @param min optional lower bound
         * @param max optional upper bound
         */
        std::unique_ptr<IteratorFactory>
        range(ObjectPtr min, ObjectPtr max, bool nulls_first = false) const;

        static PreCommitFunction getPreCommitFunction() {
            return preCommitOp;
        }

    protected:

        void preCommit();
        static void preCommitOp(void *);

    private:        
        // the default/provisional type
        using DefaultT = std::int64_t;

        mutable std::shared_ptr<void> m_index_builder;
        mutable IndexDataType m_data_type;
        // actual index instance (must be cast to a specific type)
        mutable std::shared_ptr<void> m_index;
        
        Index(db0::swine_ptr<Fixture> &);
        
        template <typename T> IndexDataType getDataType() const
        {
            if constexpr (std::is_same_v<T, std::int64_t>) {
                return IndexDataType::Int64;
            } else if constexpr (std::is_same_v<T, std::uint64_t>) {
                return IndexDataType::UInt64;
            } else {
                return IndexDataType::Unknown;
            }
        }

        // get existing or create new range-tree builder
        template <typename T> IndexBuilder<T> &getIndexBuilder(bool as_auto = false)
        {        
            if (!m_index_builder) {
                m_index_builder = db0::make_shared_void<IndexBuilder<T> >();
                if (as_auto) {
                    m_data_type = IndexDataType::Auto;
                } else {
                    m_data_type = getDataType<T>();               
                }
            }
            assert(m_data_type == getDataType<T>() || (as_auto && m_data_type == IndexDataType::Auto));
            return *static_cast<IndexBuilder<T>*>(m_index_builder.get());
        }
        
        // update index builder instance to a concrete type
        IndexDataType updateIndexBuilder(TypeId);
        
        template <typename FromType, typename ToType> void updateIndexBuilder()
        {
            if (!m_index_builder) {
                return;
            }
            
            if (!std::is_same_v<FromType, ToType>) {
                m_index_builder = db0::make_shared_void<IndexBuilder<ToType> >(
                    getIndexBuilder<FromType>(true).releaseRemoveNullItems(),
                    getIndexBuilder<FromType>(true).releaseAddNullItems(),
                    getIndexBuilder<FromType>(true).releaseObjectCache()
                );
            }
            // set or update the data type
            m_data_type = getDataType<ToType>();
        }
        
        // get existing or create a new range tree of a specific type
        template <typename T> typename db0::RangeTree<T, std::uint64_t> &getRangeTree()
        {
            using RangeTreeT = db0::RangeTree<T, std::uint64_t>;
            if (!m_index) {
                if ((*this)->m_index_addr) {
                    // pull existing range tree
                    m_index = db0::make_shared_void<RangeTreeT>(this->myPtr((*this)->m_index_addr));                    
                } else {
                    // create a new range tree instance
                    m_index = db0::make_shared_void<RangeTreeT>(this->getMemspace());
                    this->modify().m_index_addr = static_cast<const RangeTreeT*>(m_index.get())->getAddress();
                }
                m_data_type = getDataType<T>();
            }
            assert(m_data_type == getDataType<T>());
            return *static_cast<RangeTreeT*>(m_index.get());
        }

        template <typename T> const typename db0::RangeTree<T, std::uint64_t> &getRangeTree() const
        {
            return const_cast<Index*>(this)->getRangeTree<T>();
        }

        /**
         * Construct sorted query iterator from an unsorted full-text query iterator
        */
        template <typename T> std::unique_ptr<RT_SortIterator<T, std::uint64_t> >
        sortQuery(std::unique_ptr<db0::FT_Iterator<std::uint64_t> > &&query_iterator, bool asc = true) const
        {
            return std::make_unique<RT_SortIterator<T, std::uint64_t>>(getRangeTree<T>(), std::move(query_iterator), asc);
        }

        template <typename T> std::unique_ptr<RT_SortIterator<T, std::uint64_t> >
        sortSortedQuery(std::unique_ptr<db0::SortedIterator<std::uint64_t> > &&sorted_iterator, bool asc = true) const
        {
            return std::make_unique<RT_SortIterator<T, std::uint64_t>>(getRangeTree<T>(), std::move(sorted_iterator), asc);
        }
        
        template <typename T> std::unique_ptr<IteratorFactory>
        rangeQuery(ObjectPtr min, bool min_inclusive, ObjectPtr max, bool max_inclusive, bool nulls_first) const
        {
            // FIXME: make inclusive flags configurable
            // we need to handle all-null case separately because provisional data type and range type may differ
            auto &range_tree = getRangeTree<T>();
            if (range_tree.hasAnyNonNull()) {
                return std::make_unique<RangeIteratorFactory<T, std::uint64_t>>(range_tree, extractOptionalValue<T>(min),
                    min_inclusive, extractOptionalValue<T>(max), max_inclusive, nulls_first);
            } else {
                // FIXME: handle null-first policy
                auto &type_manager = LangToolkit::getTypeManager();
                if ((nulls_first && type_manager.isNull(min)) || (!nulls_first && type_manager.isNull(max))) {
                    // return all null elements
                    return std::make_unique<RangeIteratorFactory<T, std::uint64_t>>(range_tree, true);
                }
                // no results
                return nullptr;
            }
        }

        void flush();

        // adds to with a null key, compatible with all types
        void addNull(ObjectPtr);
        void removeNull(ObjectPtr);

        template <typename T> std::optional<T> extractOptionalValue(ObjectPtr value) const;
    };

    // extract optional value specializations
    template <> std::optional<std::int64_t> Index::extractOptionalValue<std::int64_t>(ObjectPtr value) const;
    template <> std::optional<std::uint64_t> Index::extractOptionalValue<std::uint64_t>(ObjectPtr value) const;

}
