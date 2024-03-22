#pragma once

#include <cstdint>
#include <dbzero/object_model/ObjectBase.hpp>
#include <dbzero/core/vspace/db0_ptr.hpp>
#include <dbzero/core/collections/range_tree/RangeTree.hpp>
#include <dbzero/core/collections/range_tree/RT_SortIterator.hpp>
#include <dbzero/core/collections/range_tree/RT_RangeIterator.hpp>
#include <dbzero/core/utils/shared_void.hpp>
#include <dbzero/workspace/GC0.hpp>
#include <dbzero/core/exception/AbstractException.hpp>
#include <dbzero/object_model/object_header.hpp>

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
        // type will be auto-assigned on first use
        Auto = 1,
        Int64 = 2
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
                
        Index(db0::swine_ptr<Fixture> &, std::uint64_t address);

        static Index *makeNew(void *at_ptr, db0::swine_ptr<Fixture> &);
        static Index *unload(void *at_ptr, db0::swine_ptr<Fixture> &, std::uint64_t address);
        
        std::size_t size() const;
        void add(ObjectPtr key, ObjectPtr value);

        void detach();
        
        /**
         * Sort results of a specific object iterator from the same fixture
         * @param iter object iterator
         * @param at_ptr memory location for the resulting iterator (must be uninitialized ObjectIterator)
         */        
        void sort(const ObjectIterator &iter, ObjectIterator *at_ptr) const;

        /**
         * Construct a range filtering query         
         * @param at_ptr memory location for the resulting iterator (must be uninitialized ObjectIterator)
         * @param min optional lower bound
         * @param max optional upper bound
         */        
        void range(ObjectIterator *at_ptr, ObjectPtr min, ObjectPtr max) const;

    private:
        mutable std::shared_ptr<void> m_index_builder;
        mutable IndexDataType m_data_type = IndexDataType::Auto;
        // actual index instance (must be cast to a specific type)
        mutable std::shared_ptr<void> m_index;
        // A cache of language objects held until flush/close is called
        // it's required to prevent unreferenced objects from being collected by GC
        // and to handle callbacks from the range-tree index
        mutable std::unordered_map<std::uint64_t, ObjectSharedPtr> m_object_cache;

        Index(db0::swine_ptr<Fixture> &);
        
        template <typename T> IndexDataType getDataType() const
        {
            if constexpr (std::is_same_v<T, std::int64_t>) {
                return IndexDataType::Int64;
            } else {
                return IndexDataType::Unknown;
            }
        }

        // get existing or create new range-tree builder
        template <typename T> typename db0::RangeTree<T, std::uint64_t>::Builder &getRangeTreeBuilder()
        {
            using BuilderT = typename db0::RangeTree<T, std::uint64_t>::Builder;
            if (!m_index_builder)
            {
                m_index_builder = db0::make_shared_void<BuilderT>();
                m_data_type = getDataType<T>();
            }
            assert(m_data_type == getDataType<T>());
            return *static_cast<BuilderT*>(m_index_builder.get());
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
        
        template <typename T> std::unique_ptr<RT_RangeIterator<T, std::uint64_t> >
        rangeQuery(ObjectPtr min, ObjectPtr max) const
        {
            // FIXME: make inclusive flags configurable
            return std::make_unique<RT_RangeIterator<T, std::uint64_t>>(getRangeTree<T>(), extractOptionalValue<T>(min),
                false, extractOptionalValue<T>(max), false);
        }

        void flush();

        // adds to with a null key, compatible with all types
        void addNull(std::uint64_t value);

        template <typename T> std::optional<T> extractOptionalValue(ObjectPtr value) const;
    };

    // extract optional value specializations
    template <> std::optional<std::int64_t> Index::extractOptionalValue<std::int64_t>(ObjectPtr value) const;    

}
