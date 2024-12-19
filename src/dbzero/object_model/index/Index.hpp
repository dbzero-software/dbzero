#pragma once

#include <cstdint>
#include <dbzero/object_model/ObjectBase.hpp>
#include <dbzero/core/vspace/db0_ptr.hpp>
#include <dbzero/core/collections/range_tree/IndexBase.hpp>
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
    class ObjectIterable;
    
    class Index: public db0::ObjectBase<Index, db0::v_object<o_index>, StorageClass::DB0_INDEX>
    {
        GC0_Declare
        using super_t = db0::ObjectBase<Index, db0::v_object<o_index>, StorageClass::DB0_INDEX>;
        friend class db0::ObjectBase<Index, db0::v_object<o_index>, StorageClass::DB0_INDEX>;      
    public:
        using LangToolkit = db0::python::PyToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
        using TypeId = db0::bindings::TypeId;
        using IteratorFactory = db0::IteratorFactory<std::uint64_t>;
        
        Index(db0::swine_ptr<Fixture> &, std::uint64_t address);
        Index(const Index &) = delete;
        ~Index();
        
        static Index *makeNew(void *at_ptr, db0::swine_ptr<Fixture> &);
        // make null instance (e.g. after destroying the original one)
        static Index *makeNull(void *at_ptr);
        static Index *unload(void *at_ptr, db0::swine_ptr<Fixture> &, std::uint64_t address);
        
        std::size_t size() const;
        void add(ObjectPtr key, ObjectPtr value);
        void remove(ObjectPtr key, ObjectPtr value);
        
        /**
         * Sort results of a specific object iterator from the same fixture
         * @param iter object iterator        
         */        
        std::unique_ptr<db0::SortedIterator<std::uint64_t> > sort(const ObjectIterable &iter,
            bool asc, bool null_first) const;
        
        /**
         * Construct a range filtering query        
         * @param min optional lower bound
         * @param max optional upper bound
         */
        std::unique_ptr<IteratorFactory>
        range(ObjectPtr min, ObjectPtr max, bool null_first = false) const;
        
        static PreCommitFunction getPreCommitFunction() {
            return preCommitOp;
        }
        
        void moveTo(db0::swine_ptr<Fixture> &);

        void flush(FixtureLock &);
        
        void commit() const;

        void detach() const;

        void destroy() const;

        // remove any cached updates / revert
        void rollback();

        void operator=(const Index &) = delete;
        
    protected:
        // the default / provisional type
        using DefaultT = std::int64_t;
        friend struct Builder;
        void preCommit(bool revert);
        static void preCommitOp(void *, bool revert);
        
        template <typename T> static constexpr IndexDataType dataTypeOf()
        {
            if constexpr (std::is_same_v<T, std::int64_t>) {
                return IndexDataType::Int64;
            } else if constexpr (std::is_same_v<T, std::uint64_t>) {
                return IndexDataType::UInt64;
            } else {
                return IndexDataType::Unknown;
            }
        }
        
        struct Builder
        {
            using IndexDataType = db0::IndexDataType;
            Index &m_index;
            // concrete data type to be assigned (only allowed to update from Auto)
            IndexDataType m_initial_type;
            IndexDataType m_new_type;
            mutable std::shared_ptr<void> m_index_builder;    

            Builder(Index &);
            
            void flush();

            void rollback();

            bool empty() const;

            inline IndexDataType getDataType() const {
                return m_new_type;
            }

            IndexBuilder<DefaultT> &getAuto()
            {
                if (!m_index_builder) {
                    m_index_builder = db0::make_shared_void<IndexBuilder<DefaultT> >();
                    m_new_type = IndexDataType::Auto;
                }
                return *static_cast<IndexBuilder<DefaultT>*>(m_index_builder.get());                
            }

            template <typename T> IndexBuilder<T> &get()
            {
                if (!m_index_builder) {
                    m_index_builder = db0::make_shared_void<IndexBuilder<T> >();
                    m_new_type = Index::dataTypeOf<T>();
                }
                return *static_cast<IndexBuilder<T>*>(m_index_builder.get());                
            }
            
            template <typename T> IndexBuilder<T> &getExisting() const
            {
                assert(m_index_builder);
                return *static_cast<IndexBuilder<T>*>(m_index_builder.get());                
            }

            // Update to a concrete data type
            void update(TypeId);
            
            // Update to a concrete data type (ToType must not be Auto)
            template <typename FromType, typename ToType> void update()
            {
                if (!m_index_builder) {
                    return;
                }
                
                if (!std::is_same_v<FromType, ToType>) {
                    m_index_builder = db0::make_shared_void<IndexBuilder<ToType> >(
                        get<FromType>().releaseRemoveNullItems(),
                        get<FromType>().releaseAddNullItems(),
                        get<FromType>().releaseObjectCache()
                    );
                    m_new_type = Index::dataTypeOf<ToType>();
                }
            }

        };
        
        Builder m_builder;

        // check if there's any unflushed data in the internal buffers
        bool isDirty() const;
        
        void _flush();

    private: 
        // actual index instance (must be cast to a specific type)
        mutable std::shared_ptr<void> m_index;
        
        // null instance constructor
        Index();
        Index(db0::swine_ptr<Fixture> &);
        Index(tag_no_gc, db0::swine_ptr<Fixture> &, const Index &);
        
        void operator=(Index &&);

        bool hasRangeTree() const {
            return (*this)->m_index_addr != 0;
        }
        
        template <typename T> std::shared_ptr<void> getRangeTreeRawPtr()
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
            }
            assert(hasRangeTree());
            return m_index;
        }

        // Get existing or create a new range tree of a specific type
        template <typename T> SharedPtrWrapper<typename db0::RangeTree<T, std::uint64_t> > getRangeTreePtr() {
            return getRangeTreeRawPtr<T>();
        }
        
        template <typename T> typename db0::RangeTree<T, std::uint64_t> &getRangeTree() {
            return *getRangeTreePtr<T>();
        }

        // Construct range tree as a copy of an other one
        template <typename T> void makeRangeTree(const typename  db0::RangeTree<T, std::uint64_t> &other)
        {            
            using RangeTreeT = db0::RangeTree<T, std::uint64_t>;
            assert(!m_index);
            assert(!(*this)->m_index_addr);
            if (m_index || (*this)->m_index_addr) {
                return;
            }

            RangeTreeT new_range_tree(this->getMemspace(), other);
            this->modify().m_index_addr = new_range_tree.getAddress();            
        }
        
        template <typename T> const typename db0::RangeTree<T, std::uint64_t> &getExistingRangeTree() const
        {
            assert(hasRangeTree());
            return const_cast<Index*>(this)->getRangeTree<T>();
        }

        template <typename T> SharedPtrWrapper<typename db0::RangeTree<T, std::uint64_t> > tryGetRangeTree() const
        {
            if (hasRangeTree()) {
                return const_cast<Index*>(this)->getRangeTreeRawPtr<T>();
            }
            return {};
        }

        /**
         * Construct sorted query iterator from an unsorted full-text query iterator
        */
        template <typename T> std::unique_ptr<RT_SortIterator<T, std::uint64_t> >
        sortQuery(std::unique_ptr<db0::FT_Iterator<std::uint64_t> > &&query_iterator, bool asc, bool null_first) const 
        {
            return std::make_unique<RT_SortIterator<T, std::uint64_t>>(
                *this, tryGetRangeTree<T>(), std::move(query_iterator), asc, null_first
            );
        }
        
        template <typename T> std::unique_ptr<RT_SortIterator<T, std::uint64_t> >
        sortSortedQuery(std::unique_ptr<db0::SortedIterator<std::uint64_t> > &&sorted_iterator, bool asc, bool null_first) const 
        {
            return std::make_unique<RT_SortIterator<T, std::uint64_t>>(
                *this, tryGetRangeTree<T>(), std::move(sorted_iterator), asc, null_first
            );
        }
        
        template <typename T> std::unique_ptr<IteratorFactory>
        rangeQuery(ObjectPtr min, bool min_inclusive, ObjectPtr max, bool max_inclusive, bool null_first) const
        {
            // FIXME: make inclusive flags configurable
            // we need to handle all-null case separately because provisional data type and range type may differ
            auto range_tree_ptr = tryGetRangeTree<T>();
            if (range_tree_ptr && range_tree_ptr->hasAnyNonNull()) {
                return std::make_unique<RangeIteratorFactory<T, std::uint64_t>>(*this, range_tree_ptr, extractOptionalValue<T>(min),
                    min_inclusive, extractOptionalValue<T>(max), max_inclusive, null_first);
            } else {
                auto &type_manager = LangToolkit::getTypeManager();
                if ((null_first && type_manager.isNull(min)) || (!null_first && type_manager.isNull(max))) {
                    // return all null elements
                    return std::make_unique<RangeIteratorFactory<T, std::uint64_t>>(*this, range_tree_ptr, RT_Range<T> {}, true);
                }
                // no results
                return nullptr;
            }
        }

        // adds to with a null key, compatible with all types
        void addNull(ObjectPtr);
        void removeNull(ObjectPtr);

        template <typename T> std::optional<T> extractOptionalValue(ObjectPtr value) const;
    };

    // extract optional value specializations
    template <> std::optional<std::int64_t> Index::extractOptionalValue<std::int64_t>(ObjectPtr value) const;
    template <> std::optional<std::uint64_t> Index::extractOptionalValue<std::uint64_t>(ObjectPtr value) const;

}
