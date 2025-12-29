// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "Index.hpp"
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/tags/ObjectIterable.hpp>
#include <dbzero/core/collections/full_text/SortedIterator.hpp>
#include <dbzero/core/utils/uuid.hpp>
#include <dbzero/bindings/TypeId.hpp>
#include <dbzero/object_model/value/Member.hpp>

namespace db0::object_model

{

    GC0_Define(Index)
    using TypeId = db0::bindings::TypeId;
    
    Index::Index()
        : m_builder(*this)
    {
    }
    
    Index::Index(db0::swine_ptr<Fixture> &fixture, AccessFlags access_mode)
        : super_t(fixture, IndexType::RangeTree, IndexDataType::Auto, access_mode)
        , m_builder(*this)
        // NOTE: register the mutation handler for supporting "locked" sections
        , m_mutation_log(fixture->addMutationHandler())
    {
    }
    
    Index::Index(db0::swine_ptr<Fixture> &fixture, Address address, AccessFlags access_mode)
        : super_t(super_t::tag_from_address(), fixture, address, access_mode)
        , m_builder(*this)
        , m_mutation_log(fixture->addMutationHandler())
    {        
    }
    
    Index::Index(tag_no_gc, db0::swine_ptr<Fixture> &fixture, const Index &other)
        : super_t(tag_no_gc(), fixture, *other.getData())
        , m_builder(*this)
        , m_mutation_log(fixture->addMutationHandler())
    {
        if (other.hasRangeTree()) {
            switch ((*this)->m_data_type) {
                case IndexDataType::Int64: {
                    makeRangeTree(other.getExistingRangeTree<std::int64_t>());
                    break;
                }

                case IndexDataType::UInt64: {
                    makeRangeTree(other.getExistingRangeTree<std::uint64_t>());
                    break;
                }

                // flush using default / provisional data type
                case IndexDataType::Auto: {
                    makeRangeTree(other.getExistingRangeTree<DefaultT>());
                    break;
                }

                default:
                    THROWF(db0::InputException) 
                        << "Unsupported index data type: " 
                        << static_cast<std::uint16_t>((*this)->m_data_type);
            }
        }        
    }
    
    Index::~Index()
    {
        // in case of index we need to unregister first because otherwise
        // it may trigger discard of unflushed data (which has to be performed before destruction of 'builder')
        unregister();
        
        // after unregister object might still have unflushed data, we need to flush them
        if (hasInstance() && isDirty()) {
            _flush();
        }
    }
    
    Index::Builder::Builder(Index &index)
        : m_index(index)
        , m_initial_type(index->m_data_type)
        , m_new_type(m_initial_type)
    {
    }
    
    bool Index::Builder::empty() const
    {
        if (!m_index_builder) {
            return true;
        }

        switch (m_new_type) {
            case IndexDataType::Int64: {
                return getExisting<std::int64_t>().empty();
                break;
            }

            case IndexDataType::UInt64: {
                return getExisting<std::uint64_t>().empty();
                break;
            }

            case IndexDataType::Auto: {
                return getExisting<DefaultT>().empty();
                break;
            }

            default:
                THROWF(db0::InputException) 
                    << "Unsupported index data type: " 
                    << static_cast<std::uint16_t>(m_new_type) << THROWF_END;
        }
    }
    
    void Index::Builder::flush()
    {
        if (!m_index_builder) {
            return;
        }

        switch (m_new_type) {
            case IndexDataType::Int64: {
                auto &ib = getExisting<std::int64_t>();
                if (!ib.empty()) {
                    ib.flush(m_index.getRangeTree<std::int64_t>());
                }
                break;
            }

            case IndexDataType::UInt64: {
                auto &ib = getExisting<std::uint64_t>();
                if (!ib.empty()) {
                    ib.flush(m_index.getRangeTree<std::uint64_t>());
                }
                break;
            }
            
            // flush using default / provisional data type
            case IndexDataType::Auto: {
                auto &ib = getExisting<DefaultT>();
                if (!ib.empty()) {
                    ib.flush(m_index.getRangeTree<DefaultT>());
                }
                break;
            }

            default:
                THROWF(db0::InputException) 
                    << "Unsupported index data type: " 
                    << static_cast<std::uint16_t>(m_new_type);
        }
        // reflect modified data type with the underlying index
        if (m_new_type != m_initial_type) {
            m_initial_type = m_new_type;
            m_index.modify().m_data_type = m_new_type;            
        }
    }
    
    void Index::Builder::update(TypeId type_id)
    {
        auto new_type = getIndexDataType(type_id);        
        if (m_new_type == IndexDataType::Auto) {
            // convert from the provisional to a concrete data type
            switch (new_type) {
                case IndexDataType::Int64: {
                    update<DefaultT, std::int64_t>();
                    break;
                }    

                case IndexDataType::UInt64: {
                    update<DefaultT, std::uint64_t>();
                    break;
                }

                default:
                    THROWF(db0::InputException) 
                        << "Unsupported index key type: " 
                        << static_cast<std::uint16_t>(type_id) << THROWF_END;
            }            
        }

        m_new_type = new_type;
    }
    
    void Index::flush(FixtureLock &) {
        _flush();
    }

    bool Index::isDirty() const {
        return !m_builder.empty();
    }
    
    void Index::_flush()
    {
        // no instance due to move
        if (!hasInstance()) {
            return;
        }    
        m_builder.flush();
    }
    
    void Index::rollback() {
        m_builder.rollback();
    }
    
    void Index::Builder::rollback()
    {
        if (!m_index_builder) {
            return;
        }
        switch (m_new_type) {
            case IndexDataType::Int64: {
                getExisting<std::int64_t>().close();
                break;
            }

            case IndexDataType::UInt64: {
                getExisting<std::uint64_t>().close();
                break;
            }
                        
            case IndexDataType::Auto: {
                getExisting<DefaultT>().close();
                break;
            }

            default:
                THROWF(db0::InputException) 
                    << "Unsupported index data type: " 
                    << static_cast<std::uint16_t>(m_new_type);
        }
        m_new_type = m_initial_type;
        m_index_builder = nullptr;
    }
    
    std::size_t Index::size() const
    {
        if (isDirty()) {
            FixtureLock lock(this->getFixture());
            const_cast<Index*>(this)->flush(lock);
        }
        if (!hasRangeTree()) {
            return 0;
        }
        
        switch ((*this)->m_data_type) {
            case IndexDataType::Int64: {
                return getExistingRangeTree<std::int64_t>().size();
                break;
            }

            case IndexDataType::UInt64: {
                return getExistingRangeTree<std::uint64_t>().size();
                break;
            }

            case IndexDataType::Auto: {
                return getExistingRangeTree<DefaultT>().size();
                break;
            }

            default:
                THROWF(db0::InputException)
                    << "Unsupported index data type: " 
                    << static_cast<std::uint16_t>((*this)->m_data_type) << THROWF_END;
        }
    }

    void Index::setDirty(bool dirty)
    {
        if (dirty) {
            getMemspace().collectForFlush(this);            
        }
        if (m_dirty_callback) {
            m_dirty_callback(dirty);
        }
    }

    void Index::add(ObjectPtr key, ObjectPtr value)
    {        
        assert(hasInstance());
        auto &type_manager = LangToolkit::getTypeManager();
        // special handling of null / None values
        if (type_manager.isNull(key)) {
            addNull(value);
            return;
        }

        if (m_builder.getDataType() == IndexDataType::Auto) {
            // update builder to a concrete data type
            m_builder.update(type_manager.getTypeId(key));
        }

        // subscribe for flush operation
        if (!isDirty()) {
            setDirty(true);
        }

        switch (m_builder.getDataType()) {
            case IndexDataType::Int64: {
                m_builder.get<std::int64_t>().add(type_manager.extractInt64(key), value); 
                break;
            }

            case IndexDataType::UInt64: {
                m_builder.get<std::uint64_t>().add(type_manager.extractUInt64(key), value);
                break;
            }

            default:
                THROWF(db0::InputException) << "Index of type " 
                    << static_cast<std::uint16_t>(m_builder.getDataType())
                    << " does not allow adding key type: " 
                    << LangToolkit::getTypeName(key) << THROWF_END;
        }
        m_mutation_log->onDirty();
    }
    
    void Index::remove(ObjectPtr key, ObjectPtr value)
    {
        assert(hasInstance());
        auto &type_manager = LangToolkit::getTypeManager();
        // special handling of null / None values
        if (type_manager.isNull(key)) {
            removeNull(value);
            return;
        }

        if (m_builder.getDataType() == IndexDataType::Auto) {
            // update to a concrete data type
            m_builder.update(type_manager.getTypeId(key));
        }

        // subscribe for flush operation
        if (!isDirty()) {
            setDirty(true);            
        }

        switch (m_builder.getDataType()) {
            case IndexDataType::Int64: {
                m_builder.get<std::int64_t>().remove(type_manager.extractInt64(key), value); 
                break;
            }

            case IndexDataType::UInt64: {
                m_builder.get<std::uint64_t>().remove(type_manager.extractUInt64(key), value);
                break;
            }

            default:
                THROWF(db0::InputException) << "Index of type " 
                    << static_cast<std::uint16_t>(m_builder.getDataType())
                    << " does not allow keys of type: " 
                    << LangToolkit::getTypeName(key) << THROWF_END;
        }
        m_mutation_log->onDirty();
    }
    
    std::unique_ptr<Index::IteratorFactory> Index::range(ObjectPtr min, ObjectPtr max, bool null_first) const
    {
        assert(hasInstance());
        if (isDirty()) {
            FixtureLock lock(this->getFixture());
            const_cast<Index*>(this)->flush(lock);
        }
        
        switch ((*this)->m_data_type) {
            case IndexDataType::Int64: {
                return rangeQuery<std::int64_t>(min, true, max, true, null_first);
            }
            break;            

            case IndexDataType::UInt64: {
                return rangeQuery<std::uint64_t>(min, true, max, true, null_first);
            }
            break;

            case IndexDataType::Auto: {
                return rangeQuery<DefaultT>(min, true, max, true, null_first);
            }
            break;

            default:
                THROWF(db0::InputException) 
                    << "Unsupported index data type: " 
                    << static_cast<std::uint16_t>((*this)->m_data_type) << THROWF_END;
        }
    }
    
    std::unique_ptr<db0::SortedIterator<UniqueAddress> >
    Index::sort(const ObjectIterable &iter, bool asc, bool null_first) const
    {
        assert(hasInstance());
        if (isDirty()) {
            FixtureLock lock(this->getFixture());
            const_cast<Index*>(this)->flush(lock);
        }

        std::unique_ptr<db0::SortedIterator<UniqueAddress> > sort_iter;
        if (iter.isSorted()) {
            // sort by additional criteria
            switch ((*this)->m_data_type) {
                case IndexDataType::Int64: {
                    return sortSortedQuery<std::int64_t>(iter.beginSorted(), asc, null_first);
                }
                break;

                case IndexDataType::UInt64: {
                    return sortSortedQuery<std::uint64_t>(iter.beginSorted(), asc, null_first);
                }
                break;

                case IndexDataType::Auto: {
                    return sortSortedQuery<DefaultT>(iter.beginSorted(), asc, null_first);
                }
                break;

                default:
                    THROWF(db0::InputException) 
                        << "Unsupported index data type: " 
                        << static_cast<std::uint16_t>((*this)->m_data_type) << THROWF_END;
            }
        } else {
            // sort a full-text query
            // FIXME: incorporate observers in the sorted iterator
            std::vector<std::unique_ptr<QueryObserver> > observers;
            switch ((*this)->m_data_type) {
                case IndexDataType::Int64: {
                    return sortQuery<std::int64_t>(iter.beginFTQuery(observers), asc, null_first);
                }
                break;

                case IndexDataType::UInt64: {
                    return sortQuery<std::uint64_t>(iter.beginFTQuery(observers), asc, null_first);
                }
                break;

                case IndexDataType::Auto: {
                    return sortQuery<DefaultT>(iter.beginFTQuery(observers), asc, null_first);
                }
                break;

                default:
                    THROWF(db0::InputException) 
                        << "Unsupported index data type: " 
                        << static_cast<std::uint16_t>((*this)->m_data_type) << THROWF_END;
            }
        }
    }
    
    void Index::addNull(ObjectPtr obj_ptr)
    {
        assert(hasInstance());
        // subscribe for flush operation
        if (!isDirty()) {
            setDirty(true);
        }
        
        switch (m_builder.getDataType()) {
            // use provisional data type for Auto
            case IndexDataType::Auto: {
                m_builder.getAuto().addNull(obj_ptr);
                break;
            }

            case IndexDataType::Int64: {
                m_builder.get<std::int64_t>().addNull(obj_ptr);
                break;
            }

            case IndexDataType::UInt64: {
                m_builder.get<std::uint64_t>().addNull(obj_ptr);
                break;
            }

            default:
                THROWF(db0::InputException) 
                    << "Unsupported index data type: " 
                    << static_cast<std::uint16_t>(m_builder.getDataType()) << THROWF_END;
        }
        m_mutation_log->onDirty();
    }
    
    // extract optional value
    template <> std::optional<std::int64_t> Index::extractOptionalValue<std::int64_t>(ObjectPtr value) const
    {
        auto &type_manager = LangToolkit::getTypeManager();
        if (type_manager.isNull(value)) {
            return std::nullopt;    
        }
        return type_manager.extractInt64(value);
    }

    template <> std::optional<std::uint64_t> Index::extractOptionalValue<std::uint64_t>(ObjectPtr value) const
    {
        auto &type_manager = LangToolkit::getTypeManager();
        if (type_manager.isNull(value)) {
            return std::nullopt;    
        }
        return type_manager.extractUInt64(type_manager.getTypeId(value), value);
    }
    
    void Index::flush(bool revert)
    {
        if (revert) {
            rollback();
        } else {
            _flush();
        }
        setDirty(false);
    }

    void Index::flushOp(void *ptr, bool revert) {
        static_cast<Index*>(ptr)->flush(revert);
    }

    void Index::removeNull(ObjectPtr obj_ptr)
    {
        if (!isDirty()) {
            setDirty(true);
        }

        switch (m_builder.getDataType()) {
            // use provisional data type for Auto
            case IndexDataType::Auto: {
                m_builder.getAuto().removeNull(obj_ptr);
                break;
            }

            case IndexDataType::Int64: {
                m_builder.get<std::int64_t>().removeNull(obj_ptr);
                break;
            }

            case IndexDataType::UInt64: {
                m_builder.get<std::uint64_t>().removeNull(obj_ptr);
                break;
            }

            default:
                THROWF(db0::InputException) 
                    << "Unsupported index data type: " 
                    << static_cast<std::uint16_t>(m_builder.getDataType()) << THROWF_END;
        }
        m_mutation_log->onDirty();
    }
    
    void Index::moveTo(db0::swine_ptr<Fixture> &fixture)
    {        
        m_mutation_log = fixture->addMutationHandler();
        assert(hasInstance());
        this->_flush();
        super_t::moveTo(fixture);
    }
    
    void Index::commit() const
    {
        // if m_index exists then also must have a range tree
        assert(!m_index || hasRangeTree());
        const_cast<Index*>(this)->_flush();
        // commit the underlying range tree if it exists
        if (m_index) {
            switch ((*this)->m_data_type) {
                case IndexDataType::Int64: {
                    getExistingRangeTree<std::int64_t>().commit();
                    break;
                }

                case IndexDataType::UInt64: {
                    getExistingRangeTree<std::uint64_t>().commit();
                    break;
                }

                // flush using default / provisional data type
                case IndexDataType::Auto: {
                    getExistingRangeTree<DefaultT>().commit();
                    break;
                }

                default:
                    THROWF(db0::InputException)
                        << "Unsupported index data type: " 
                        << static_cast<std::uint16_t>((*this)->m_data_type);
            }
        }
        super_t::commit();
    }
    
    void Index::detach() const
    {
        // if m_index exists then also must have a range tree
        assert(!m_index || hasRangeTree());
        // invalidate cached index instance since its type might've been updated
        m_index = nullptr;
        super_t::detach();
    }
    
    void Index::destroy()
    {
        m_mutation_log = nullptr;
        // discard any pending changes
        const_cast<Builder&>(m_builder).rollback();
        if (hasRangeTree()) {
            auto fixture = this->getFixture();
            auto unref_func = [&fixture](Address obj_addr) {
                unrefMember<StorageClass::OBJECT_REF, LangToolkit>(fixture, obj_addr);
            };
            switch ((*this)->m_data_type) {
                case IndexDataType::Int64: {
                    // unreference all elements
                    getExistingRangeTree<std::int64_t>().forAll(unref_func);
                    getExistingRangeTree<std::int64_t>().destroy();
                    break;
                }

                case IndexDataType::UInt64: {
                    // unreference all elements
                    getExistingRangeTree<std::uint64_t>().forAll(unref_func);
                    getExistingRangeTree<std::uint64_t>().destroy();
                    break;
                }
                
                case IndexDataType::Auto: {
                    // unreference all elements
                    getExistingRangeTree<DefaultT>().forAll(unref_func);
                    getExistingRangeTree<DefaultT>().destroy();
                    break;
                }

                default:
                    THROWF(db0::InputException)
                        << "Unsupported index data type: " 
                        << static_cast<std::uint16_t>((*this)->m_data_type);
            }           
        }
        super_t::destroy();
    }
    
}