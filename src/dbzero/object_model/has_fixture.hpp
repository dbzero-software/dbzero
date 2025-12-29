// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/memory/swine_ptr.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/workspace/Fixture.hpp>

namespace db0

{

    /**
     * v_object extensions which takes weak ref (using swine_ptr) of the underlying Fixture
     * this allows safely accessing objects even after destroying the underlying Fixture
     * and storing weak ref without any additional memory footprint
     * @tparam BaseT must be some v_object or derived class
    */
    template <typename BaseT> class has_fixture: public BaseT
    {
    public:        
        has_fixture() = default;

        // create new instance
        template <typename... Args> has_fixture(db0::swine_ptr<Fixture> &fixture, Args &&... args)
            : BaseT(*fixture, std::forward<Args>(args)...)
        {
            // take weak ref of the Fixture
            fixture.take_weak();
        }
        
        // Open an existing instance
        // NOTE: we use tag_verified to avoid registering unverified instance with GC0
        struct tag_from_address {};
        has_fixture(tag_from_address, db0::swine_ptr<Fixture> &fixture, Address address, std::size_t size_of = 0, AccessFlags access_mode = {})
            : BaseT(db0::tag_verified(), mptr(*fixture, address), size_of, access_mode)
        {
            // take weak ref of the Fixture
            fixture.take_weak();
        }
        
        // move existing instance / stem
        struct tag_from_stem {};
        has_fixture(tag_from_stem, db0::swine_ptr<Fixture> &fixture, BaseT &&stem)
            : BaseT(std::move(stem))
        {
            // take weak ref of the Fixture
            fixture.take_weak();
        }
        
        template <typename... Args> void init(db0::swine_ptr<Fixture> &fixture, Args &&... args)
        {
            // must release existing weak ref
            Fixture *raw_ptr = reinterpret_cast<Fixture*>(this->getMemspacePtr());
            if (raw_ptr) {
                // release weak ref of the Fixture
                db0::swine_ptr<Fixture>::release_weak(raw_ptr);                
            }
            BaseT::init(*fixture, std::forward<Args>(args)...);
            // take weak ref of the Fixture
            fixture.take_weak();
        }

        template <typename... Args> 
        std::uint16_t initUnique(db0::swine_ptr<Fixture> &fixture, Args &&... args)
        {
            // must release existing weak ref
            Fixture *raw_ptr = reinterpret_cast<Fixture*>(this->getMemspacePtr());
            if (raw_ptr) {
                // release weak ref of the Fixture
                db0::swine_ptr<Fixture>::release_weak(raw_ptr);                
            }
            auto result = BaseT::initUnique(*fixture, std::forward<Args>(args)...);
            // take weak ref of the Fixture
            fixture.take_weak();
            return result;
        }
        
        ~has_fixture()
        {
            Fixture *raw_ptr = reinterpret_cast<Fixture*>(this->getMemspacePtr());
            if (raw_ptr) {
                // release weak ref of the Fixture
                db0::swine_ptr<Fixture>::release_weak(raw_ptr);                
            }
        }
        
        db0::swine_ptr<Fixture> tryGetFixture() const
        {
            Fixture *raw_ptr = reinterpret_cast<Fixture*>(this->getMemspacePtr());
            if (raw_ptr) {
                // construct swine_ptr from raw ptr
                return db0::swine_ptr<Fixture>::lock_weak(raw_ptr);
            } else {
                return {};
            }
        }
        
        db0::swine_ptr<Fixture> getFixture() const
        {
            auto fixture = tryGetFixture();
            if (!fixture) {
                // this happens when the underlying prefix / fixture has been closed / destroyed
                THROWF(db0::InternalException) << "Object instance inaccessible";
            }
            return fixture;
        }
        
        void operator=(const has_fixture &other)
        {
            // must release existing weak ref and take from the copied object
            Fixture *raw_ptr = reinterpret_cast<Fixture*>(this->getMemspacePtr());
            if (raw_ptr) {
                // release weak ref of the Fixture
                db0::swine_ptr<Fixture>::release_weak(raw_ptr);                
            }
            auto other_fixture = other.getFixture();
            BaseT::operator=(other);
            // take weak ref of the Fixture
            other_fixture.take_weak();
        }
        
        void operator=(has_fixture &&other)
        {
            // must release existing weak ref and take from the copied object
            Fixture *raw_ptr = reinterpret_cast<Fixture*>(this->getMemspacePtr());
            if (raw_ptr) {
                // release weak ref of the Fixture
                db0::swine_ptr<Fixture>::release_weak(raw_ptr);                
            }
            auto other_fixture = other.getFixture();
            BaseT::operator=(std::move(other));
            // take weak ref of the Fixture
            other_fixture.take_weak();
        }
        
        // gets the underlying fixture UUID (or throw)
        std::uint64_t getFixtureUUID() const {
            return this->getFixture()->getUUID();
        }
    };
    
}
