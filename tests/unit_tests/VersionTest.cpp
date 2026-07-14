// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <dbzero/core/serialization/Base.hpp>
#include <dbzero/core/serialization/Ext.hpp>
#include <dbzero/core/serialization/string.hpp>
#include <utils/TestBase.hpp>

#include <gtest/gtest.h>
#include <sstream>
#include <numeric>

using namespace db0;

namespace tests {

DB0_PACKED_BEGIN

class DB0_PACKED_ATTR VersionNone : public o_base<VersionNone, 0, false>{
    friend o_base<VersionNone, 0, false>;
protected:
    VersionNone(uint32_t i, const std::string &s1, const std::string &s2)
        : ii(i)
    {
        if(fehler == 0){
            arrangeMembers()
                    (o_string::type(), s1)
                    (o_string::type(), s2);
            return ;
        }
        if(fehler == 1){
            arrangeMembers()
                    (o_string::type(), s1)
                    [1]
                    (o_string::type(), s2);
            return ;
        }
        if(fehler == 2){
            auto arr = arrangeMembers()
                    (o_string::type(), s1);

            arr = arr[2]
                    (o_string::type(), s2);
            return ;
        }
        THROW(InputException, "Wrong fehler!");
    }

public:
    uint32_t ii;

    static unsigned int fehler;

    static size_t measure (uint32_t /* i */, const std::string &s1, const std::string &s2){
        if(fehler==0)
            return measureMembers()
                (o_string::type(), s1)
                (o_string::type(), s2);
        if(fehler==1)
            return measureMembers()
                (o_string::type(), s1)
                [1]
                (o_string::type(), s2);
        if(fehler==2){
            Meter met = measureMembers()
                (o_string::type(), s1);

            met = met[1]
                (o_string::type(), s2);
            return met;
        }
        THROW(InputException, "Wrong fehler!");
    }

    template <class buf_t> static size_t safeSizeOf (buf_t buf){
        if(fehler==0)
            return sizeOfMembers(buf)
                (o_string::type())
                (o_string::type());
        if(fehler==1)
            return sizeOfMembers(buf)
                (o_string::type())
                [1]
                (o_string::type());
        if(fehler==2){
            SafeSize<buf_t> ssi = sizeOfMembers(buf)
                (o_string::type());

            ssi = ssi[2]
                (o_string::type());
            return ssi;
        }
        THROW(InputException, "Wrong fehler!");
    }

    const o_string& getS1() const {
        return getDynFirst(o_string::type());
    }
    const o_string& getS2() const {
        return getDynAfter(getS1(), o_string::type());
    }
};

unsigned int VersionNone::fehler=0;

//note - 0'th version without declaring it, but this is versioned object!
class DB0_PACKED_ATTR VersionZero : public o_base<VersionZero>{
    friend o_base<VersionZero>;
protected:
    VersionZero(uint32_t i, const std::string &s1)
        : ii(i)
    {
        std::string s2("Zosia");

        if(fehler == 0){
            arrangeMembers()
                    (o_string::type(), s1);
            return ;
        }
        if(fehler == 1){
            arrangeMembers()
                    (o_string::type(), s1)
                    [1]
                    (o_string::type(), s2);
        }
        if(fehler == 2){
            auto arr = arrangeMembers()
                    (o_string::type(), s1);
            arr = arr[2]
                    (o_string::type(), s2);
        }
        THROW(InputException, "Wrong fehler!");
    }

public:
    uint32_t ii;

    static unsigned int fehler;

    static size_t measure (uint32_t /* i */, const std::string &s1){

        std::string s2("Zosia");

        if(fehler==0)
            return measureMembers()
                (o_string::type(), s1);
        if(fehler==1)
            return measureMembers()
                (o_string::type(), s1)
                [1]
                (o_string::type(), s2);
        if(fehler==2){
            Meter met = measureMembers()
                (o_string::type(), s1);

            met = met[1]
                (o_string::type(), s2);
            return met;
        }
        THROW(InputException, "Wrong fehler!");
    }

    template <class buf_t> static size_t safeSizeOf (buf_t buf){
        if(fehler==0)
            return sizeOfMembers(buf)
                (o_string::type());
        if(fehler==1)
            return sizeOfMembers(buf)
                (o_string::type())
                [1]
                (o_string::type());
        if(fehler==2){
            SafeSize<buf_t> ssi = sizeOfMembers(buf)
                (o_string::type());

            ssi = ssi[1]
                (o_string::type());
            return ssi;
        }
        THROW(InputException, "Wrong fehler!");
    }

    const o_string& getS1() const {
        return getDynFirst(o_string::type());
    }
    const o_string& getS1Throw() const {         //insufficient version to get, and no alternative...
        return getDynFirst(o_string::type(), 1);
    }
};

unsigned int VersionZero::fehler=0;

//note - 1'st version
//layout identical to VersionNull, so its memory compatible type

class DB0_PACKED_ATTR VersionOne : public o_base<VersionOne, 1>{
    friend o_base<VersionOne, 1>;
protected:
    VersionOne(uint32_t i, const std::string &s1, const std::string& s2)
        : ii(i)
    {
        //another way of constructing the object
        arrangeMembers()
                (o_string::type(), s1)
                [1]
                (o_string::type(), s2);

        //another way of constructing the object
        auto arr = arrangeMembers()
                    (o_string::type(), s1);
        arr = arr[1]
                    (o_string::type(), s2);

        if(fehler==1){
            arrangeMembers()
                (o_string::type(), s1)
                (o_string::type(), s2);
        }
        if(fehler==2){
            auto arr = arrangeMembers()
                        (o_string::type(), s1);
            arr = arr
                        (o_string::type(), s2);
        }
    }

public:
    uint32_t ii;

    static unsigned int fehler;

    static size_t measure (uint32_t /* i */, const std::string &s1, const std::string& s2){

        size_t res1 = measureMembers()
                (o_string::type(), s1)
                [1]
                (o_string::type(), s2);

        Meter met = measureMembers()
            (o_string::type(), s1)[1];

        met = met
            (o_string::type(), s2);

        size_t res2 = met;

        if(res1 != res2)  //sa(i)nity check
            THROW(InputException, "Impossible");

        if(fehler==0)
            return met;

        if(fehler==1){
            return measureMembers()
                    (o_string::type(), s1)
                    (o_string::type(), s2);
        }

        if(fehler==2){
            Meter met = measureMembers()
                (o_string::type(), s1);

            met
                (o_string::type(), s2);
            return met;
        }
        THROW(InputException, "Wrong fehler!");
    }

    template <class buf_t> static size_t safeSizeOf (buf_t buf){
        size_t res1 = sizeOfMembers(buf)
                (o_string::type())
                [1]
                (o_string::type());

        SafeSize<buf_t> ssi = sizeOfMembers(buf)
                (o_string::type())[1];
        ssi = ssi
                (o_string::type());

        size_t res2 = ssi;

        if(res1 != res2)  //sa(i)nity check
            THROW(InputException, "Impossible");

        if(fehler==0)
            return ssi;
        if(fehler==1)
            return sizeOfMembers(buf)
                (o_string::type())
                (o_string::type());
        if(fehler==2){
            SafeSize<buf_t> ssi = sizeOfMembers(buf)
                (o_string::type());

            ssi = ssi
                (o_string::type());
            return ssi;
        }
        THROW(InputException, "Wrong fehler!");
    }

    const o_string& getS1() const {
        return getDynFirst(o_string::type());
    }
    const o_string& getS2Throw() const {         //insufficient version to get, and no alternative...
        return getDynAfter(getS1(), o_string::type(), 1);
    }
    const o_string& getS2RedirS1() const {         //insufficient version to get, and alternative is S1...
        return getDynAfter(getS1(), o_string::type(), getS1(), 1);
    }
};

unsigned int VersionOne::fehler=0;

// //inheritance after non-versioned

class DB0_PACKED_ATTR VerNoneExt : public o_ext<VerNoneExt, VersionNone, 0, false>{
    friend o_ext<VerNoneExt, VersionNone, 0, false>;
    typedef o_ext<VerNoneExt, VersionNone, 0, false> Super;
protected:
    VerNoneExt(const std::string &ex1, uint32_t i, const std::string &s1, const std::string &s2)
        : Super(i, s1, s2)
    {
        arrangeMembers()
                    (o_string::type(), ex1);
    }

public:
    static unsigned int fehler;

    static size_t measure (const std::string &ex1, uint32_t i, const std::string &s1, const std::string &s2){
            return measureMembersFromBase(i, s1, s2)
                (o_string::type(), ex1);
    }

    template <class buf_t> static size_t safeSizeOf (buf_t buf){
            return sizeOfMembers(buf)
                (o_string::type());
    }

    const o_string &getEx1() const {
        return getDynFirst(o_string::type());
    }
};


class DB0_PACKED_ATTR VerNoneExt0 : public o_ext<VerNoneExt0, VersionNone, 0>{
    friend o_ext<VerNoneExt0, VersionNone, 0>;
    typedef o_ext<VerNoneExt0, VersionNone, 0> Super;
protected:
    VerNoneExt0(const std::string &ex1, uint32_t i, const std::string &s1, const std::string &s2)
        : Super(i, s1, s2)
    {
        arrangeMembers()
                    (o_string::type(), ex1);
    }

public:
    static unsigned int fehler;

    static size_t measure (const std::string &ex1, uint32_t i, const std::string &s1, const std::string &s2){
            return measureMembersFromBase(i, s1, s2)
                (o_string::type(), ex1);
    }

    template <class buf_t> static size_t safeSizeOf (buf_t buf){
            return sizeOfMembers(buf)
                (o_string::type());
    }

    const o_string &getEx1() const {
        return getDynFirst(o_string::type());
    }
};

class DB0_PACKED_ATTR VerNoneExt1 : public o_ext<VerNoneExt1, VersionNone, 1>{
    friend o_ext<VerNoneExt1, VersionNone, 1>;
    typedef o_ext<VerNoneExt1, VersionNone, 1> Super;
protected:
    VerNoneExt1(const std::string &ex2, const std::string &ex1, uint32_t i, const std::string &s1, const std::string &s2)
        : Super(i, s1, s2)
    {
        arrangeMembers()
            (o_string::type(), ex1)
            [1]
            (o_string::type(), ex2);
    }

public:
    static unsigned int fehler;

    static size_t measure (const std::string &ex2, const std::string &ex1, uint32_t i, const std::string &s1, const std::string &s2){
            return measureMembersFromBase(i, s1, s2)
                (o_string::type(), ex1)
                [1]
                (o_string::type(), ex2);
    }

    template <class buf_t> static size_t safeSizeOf (buf_t buf){
            return sizeOfMembers(buf)
                (o_string::type())
                [1]
                (o_string::type());
    }

    const o_string &getEx1() const {
        return getDynFirst(o_string::type());
    }

    const o_string &getEx2Throw() const {
        return getDynAfter(getEx1(), o_string::type(), 1);
    }

    const o_string &getEx2Redir() const {
        return getDynAfter(getEx1(), o_string::type(), getEx1(), 1);
    }
};


// no versioned inheritance after versioned

class DB0_PACKED_ATTR Ver0ExtNone : public o_ext<Ver0ExtNone, VersionZero, 0, false>{
    friend o_ext<Ver0ExtNone, VersionZero, 0, false>;
    typedef o_ext<Ver0ExtNone, VersionZero, 0, false> Super;
protected:
    Ver0ExtNone(const std::string &ex1, uint32_t i, const std::string &s1)
        : Super(i, s1)
    {
        arrangeMembers()
            (o_string::type(), ex1);
    }

public:
    static size_t measure (const std::string &ex1, uint32_t i, const std::string &s1){
            return measureMembersFromBase(i, s1)
                (o_string::type(), ex1);
    }

    template <class buf_t> static size_t safeSizeOf (buf_t buf){
            return sizeOfMembers(buf)
                (o_string::type());
    }

    const o_string &getEx1() const {
        return getDynFirst(o_string::type());
    }
};

class DB0_PACKED_ATTR Ver1ExtNone : public o_ext<Ver1ExtNone, VersionOne, 0, false>{
    friend o_ext<Ver1ExtNone, VersionOne, 0, false>;
    typedef o_ext<Ver1ExtNone, VersionOne, 0, false> Super;
protected:
    Ver1ExtNone(const std::string &ex1, uint32_t i, const std::string &s1, const std::string& s2)
        : Super(i, s1, s2)
    {
        arrangeMembers()
            (o_string::type(), ex1);
    }

public:
    static size_t measure (const std::string &ex1, uint32_t i, const std::string &s1, const std::string& s2){
            return measureMembersFromBase(i, s1, s2)
                (o_string::type(), ex1);
    }

    template <class buf_t> static size_t safeSizeOf (buf_t buf){
            return sizeOfMembers(buf)
                (o_string::type());
    }

    const o_string &getEx1() const {
        return getDynFirst(o_string::type());
    }
};

//versioned from versioned obj
//quick tip
//ver 00 <- can be read from 00 01 10 11
//ver 01 <- can be read from 01 11
//ver 10 <- can be read from 10 11
//ver 11 <- can be read from 11

class DB0_PACKED_ATTR Ver0Ext0 : public o_ext<Ver0Ext0, VersionZero>{
    friend o_ext<Ver0Ext0, VersionZero>;
    typedef o_ext<Ver0Ext0, VersionZero> Super;
protected:
    Ver0Ext0(const std::string &ex1, uint32_t i, const std::string &s1)
        : Super(i, s1)
    {
        arrangeMembers()
            (o_string::type(), ex1);
    }

public:
    static size_t measure (const std::string &ex1, uint32_t i, const std::string &s1){
            return measureMembersFromBase(i, s1)
                (o_string::type(), ex1);
    }

    template <class buf_t> static size_t safeSizeOf (buf_t buf){
            return sizeOfMembers(buf)
                (o_string::type());
    }

    const o_string &getEx1() const {
        return getDynFirst(o_string::type());
    }
};

class DB0_PACKED_ATTR Ver1Ext0 : public o_ext<Ver1Ext0, VersionOne>{
    friend o_ext<Ver1Ext0, VersionOne>;
    typedef o_ext<Ver1Ext0, VersionOne> Super;
protected:
    Ver1Ext0(const std::string &ex1, uint32_t i, const std::string &s1, const std::string &s2)
        : Super(i, s1, s2)
    {
        arrangeMembers()
            (o_string::type(), ex1);
    }

public:
    static size_t measure (const std::string &ex1, uint32_t i, const std::string &s1, const std::string &s2){
            return measureMembersFromBase(i, s1, s2)
                (o_string::type(), ex1);
    }

    template <class buf_t> static size_t safeSizeOf (buf_t buf){
            return sizeOfMembers(buf)
                (o_string::type());
    }

    const o_string &getEx1() const {
        return getDynFirst(o_string::type());
    }
};

class DB0_PACKED_ATTR Ver0Ext1 : public o_ext<Ver0Ext1, VersionZero, 1>{
    friend o_ext<Ver0Ext1, VersionZero, 1>;
    typedef o_ext<Ver0Ext1, VersionZero, 1> Super;
protected:
    Ver0Ext1(const std::string &ex2, const std::string &ex1, uint32_t i, const std::string &s1)
        : Super(i, s1)
    {
        arrangeMembers()
            (o_string::type(), ex1)
            [1]
            (o_string::type(), ex2);
    }

public:
    static size_t measure (const std::string &ex2, const std::string &ex1, uint32_t i, const std::string &s1){
        return measureMembersFromBase(i, s1)
                (o_string::type(), ex1)
                [1]
                (o_string::type(), ex2);
    }

    template <class buf_t> static size_t safeSizeOf (buf_t buf){
            return sizeOfMembers(buf)
                (o_string::type())
                [1]
                (o_string::type());
    }

    const o_string &getEx1() const {
        return getDynFirst(o_string::type());
    }
    const o_string &getEx2Throw() const {
        return getDynAfter(getEx1(), o_string::type(), 1);
    }
    const o_string &getEx2RedirS1() const {
        return getDynAfter(getEx1(), o_string::type(), getEx1(), 1);
    }
};

class DB0_PACKED_ATTR Ver1Ext1 : public o_ext<Ver1Ext1, VersionOne, 1>{
    friend o_ext<Ver1Ext1, VersionOne, 1>;
    typedef o_ext<Ver1Ext1, VersionOne, 1> Super;
protected:
    Ver1Ext1(const std::string &ex2, const std::string &ex1, uint32_t i, const std::string &s1, const std::string &s2)
        : Super(i, s1, s2)
    {
        arrangeMembers()
            (o_string::type(), ex1)
            [1]
            (o_string::type(), ex2);
    }

public:
    static size_t measure (const std::string &ex2, const std::string &ex1, uint32_t i, const std::string &s1, const std::string &s2){
        return measureMembersFromBase(i, s1, s2)
                    (o_string::type(), ex1)
                    [1]
                    (o_string::type(), ex2);
    }

    template <class buf_t> static size_t safeSizeOf (buf_t buf){
        return sizeOfMembers(buf)
                (o_string::type())
                [1]
                (o_string::type());
    }

    const o_string &getEx1() const {
        return getDynFirst(o_string::type());
    }
    const o_string &getEx2Throw() const {
        return getDynAfter(getEx1(), o_string::type(), 1);
    }
    const o_string &getEx2RedirS1() const {
        return getDynAfter(getEx1(), o_string::type(), getEx1(), 1);
    }
};

class DB0_PACKED_ATTR o_base_type : public o_fixed<o_base_type> {
    int a;
    int b;
    double d;

public:
    o_base_type (int a = 0, int b = 0, double d = 0)
        : a(a)
        , b(b)
        , d(d)
    {
    }

    std::string toString () const {
        std::stringstream _str;
        _str << a << "." << b;
        return _str.str();
    }

    std::string sumString () const {
        std::stringstream _str;
        _str << sum();
        return _str.str();
    }

    double sum () const {
        return a + b;
    }

    // returns number
    int getNumber (int num) const {
        return num;
    }

    // this is lexical_cast requirement
    friend std::ostream& operator<<(std::ostream& os, const o_base_type & /* value */) {
        return os << "instance of o_base_type";
    }
};

class DB0_PACKED_ATTR o_derived_type : public o_ext<o_derived_type, o_base_type, 0> {
    using super_t = o_ext<o_derived_type, o_base_type, 0>;
    int c;
public:
    struct Initializer{
        int a, b, c;
    };

    o_derived_type (const Initializer& i)
        : o_derived_type(i.a, i.b, i.c)
    {}

    o_derived_type (int a, int b, int c)
        : super_t(a, b)
        , c(c)
    {
    }

    std::string toString () const {
        std::stringstream _str;
        _str << o_base_type::toString() << "." << c;
        return _str.str();
    }
};

class VersionTest : public MemspaceTestBase {
};

TEST_F( VersionTest, testNonVerObjects) {
    ///////////////////////////////
    //test access of nonversioned object
    //
    std::vector<std::byte> mem_mgr(65536);

    VersionNone::fehler=0;

    ASSERT_NO_THROW(VersionNone::__new(mem_mgr.data(), 44, "Ala", "Zosia"));
    ASSERT_NO_THROW(VersionNone::measure(44, "Ala", "Zosia"));
    ASSERT_NO_THROW(VersionNone::safeSizeOf(mem_mgr.data()));

    ASSERT_EQ(VersionNone::getIsVerStored(), false);
    ASSERT_EQ(VersionNone::__ref(mem_mgr.data()).getObjVer(), 0);

    VersionNone::fehler=1;
    //versioning is forbidden for non-versioned type
    //in VERY FULL DEBUG MODE
#ifdef VERY_FULL_DEBUG
    ASSERT_ANY_THROW(VersionNone::measure(44, "Ala", "Zosia"));
    ASSERT_ANY_THROW(VersionNone::safeSizeOf(mem_mgr.data()));

    VersionNone::fehler=2;
    //versioning is forbidden for non-versioned type
    //in VERY FULL DEBUG MODE
    ASSERT_ANY_THROW(VersionNone::measure(44, "Ala", "Zosia"));
    ASSERT_ANY_THROW(VersionNone::safeSizeOf(mem_mgr.data()));
#endif
}

TEST_F( VersionTest, testVerObject){
    ///////////////////////////////
    //test acces of lower version obj from misc code...
    //
    std::vector<std::byte> mem_mgr(65536);

    VersionZero::fehler=0;
    VersionOne::fehler=0;

    ASSERT_NO_THROW(VersionZero::__new(mem_mgr.data(), 44, "Ala"));
    ASSERT_NO_THROW(VersionZero::measure(44, "Ala"));
    ASSERT_NO_THROW(VersionZero::safeSizeOf(mem_mgr.data()));

    ASSERT_EQ      (VersionZero::getIsVerStored(), true);
    ASSERT_EQ      (VersionOne ::getIsVerStored(), true);
    ASSERT_NO_THROW(VersionZero::__ref(mem_mgr.data()));
    ASSERT_NO_THROW(VersionOne ::__ref(mem_mgr.data()));
    ASSERT_EQ      (VersionZero::__ref(mem_mgr.data()).getObjVer(), 0);
    ASSERT_EQ      (VersionOne ::__ref(mem_mgr.data()).getObjVer(), 0);
    ASSERT_EQ      (VersionZero::safeSizeOf(mem_mgr.data()), VersionOne::safeSizeOf(mem_mgr.data()));
#ifdef VERY_FULL_DEBUG
    ASSERT_ANY_THROW(VersionOne::__ref(mem_mgr.data()).getS2Throw());
#endif
    ASSERT_NO_THROW (VersionOne::__ref(mem_mgr.data()).getS2RedirS1());
    ASSERT_EQ       (VersionOne::__ref(mem_mgr.data()).getS2RedirS1().toString(), VersionOne ::__ref(mem_mgr.data()).getS1().toString());
    ASSERT_EQ       (VersionOne::__ref(mem_mgr.data()).getS2RedirS1().toString(), VersionZero::__ref(mem_mgr.data()).getS1().toString());

    ASSERT_EQ(VersionOne::__ref(mem_mgr.data()).getS2RedirS1().toString(), "Ala");

#ifdef VERY_FULL_DEBUG
    VersionZero::fehler=1;
    VersionOne::fehler=1;
    //no required version information given
    ASSERT_ANY_THROW(VersionZero::measure(44, "Ala"));
    ASSERT_ANY_THROW(VersionOne::measure(44, "Ala", "Zosia"));
    ASSERT_ANY_THROW(VersionZero::safeSizeOf(mem_mgr.data()));
    ASSERT_ANY_THROW(VersionOne::safeSizeOf(mem_mgr.data()));


    VersionZero::fehler=2;
    VersionOne::fehler=2;
    //no required version information given
    ASSERT_ANY_THROW(VersionZero::measure(44, "Ala"));
    ASSERT_ANY_THROW(VersionOne::measure(44, "Ala", "Zosia"));
    ASSERT_ANY_THROW(VersionZero::safeSizeOf(mem_mgr.data()));
    ASSERT_ANY_THROW(VersionOne::safeSizeOf(mem_mgr.data()));
#endif

    /////////////////////////////////////
    //test access higher ver obj from misc code, note: downversion is not supported by design!
    //

    VersionOne::fehler=0;
    auto *ptr = mem_mgr.data() + VersionOne::safeSizeOf(mem_mgr.data());

    VersionZero::fehler=0;
    VersionOne::fehler=0;

    ASSERT_NO_THROW (VersionOne::__new(ptr, 44, "Ala", "Zosia"));
    ASSERT_EQ       (VersionOne::__ref(ptr).getObjVer(), 1);
    ASSERT_NO_THROW (VersionOne::__ref(ptr).getS2Throw());
    ASSERT_EQ       (VersionOne::__ref(ptr).getS2RedirS1().toString(), "Zosia");
    ASSERT_EQ       (VersionOne::__ref(ptr).getS2Throw().toString(), "Zosia");

#ifdef VERY_FULL_DEBUG
    ASSERT_ANY_THROW(VersionZero::__ref(ptr));
#endif
}

TEST_F( VersionTest, testNonVerExtFromNonVer){
    std::vector<std::byte> mem_mgr(65536);

    VersionNone::fehler=0;

    std::byte* ptr = mem_mgr.data();

    ASSERT_NO_THROW(VerNoneExt::__new(ptr, "Ala", 11, "Zosia", "Kasia"));
    ASSERT_NO_THROW(VerNoneExt::__ref(ptr));
    ASSERT_EQ      (VerNoneExt::__ref(ptr).getIsVerStored(), false);
    ASSERT_EQ      (VerNoneExt::__ref(ptr).getSuper().getIsVerStored(), false);
    ASSERT_EQ      (VerNoneExt::__ref(ptr).getObjVer(), 0);
    ASSERT_EQ      (VerNoneExt::__ref(ptr).getSuper().getObjVer(), 0);

    ASSERT_EQ      (VerNoneExt::__ref(ptr).getEx1().toString(), "Ala");
    ASSERT_EQ      (VerNoneExt::__ref(ptr).getS2().toString(), "Kasia");
}

TEST_F( VersionTest, testVerExtFromNonVerObject) {

    std::vector<std::byte> mem_mgr(65536);

    VersionNone::fehler=0;

    std::byte* ptr2 = mem_mgr.data();

    ASSERT_NO_THROW(VerNoneExt0::__new(ptr2, "Ala", 11, "Zosia", "Kasia"));
    ASSERT_NO_THROW(VerNoneExt0::__ref(ptr2));
    ASSERT_EQ      (VerNoneExt0::__ref(ptr2).getIsVerStored(), true);
    ASSERT_EQ      (VerNoneExt0::__ref(ptr2).getSuper().getIsVerStored(), false);
    ASSERT_EQ      (VerNoneExt0::__ref(ptr2).getObjVer(), 0);
    ASSERT_EQ      (VerNoneExt0::__ref(ptr2).getSuper().getObjVer(), 0);

    std::byte* ptr3 = ptr2 + VerNoneExt0::__ref(ptr2).sizeOf();

    ASSERT_NO_THROW(VerNoneExt1::__new(ptr3, "Basia", "Ala", 11, "Zosia", "Kasia"));
    ASSERT_NO_THROW(VerNoneExt1::__ref(ptr3));
    ASSERT_EQ      (VerNoneExt1::__ref(ptr3).getIsVerStored(), true);
    ASSERT_EQ      (VerNoneExt1::__ref(ptr3).getSuper().getIsVerStored(), false);
    ASSERT_EQ      (VerNoneExt1::__ref(ptr3).getObjVer(), 1);
    ASSERT_EQ      (VerNoneExt1::__ref(ptr3).getSuper().getObjVer(), 0);

    ASSERT_NO_THROW (VerNoneExt1::__ref(ptr2));

#ifdef VERY_FULL_DEBUG
    ASSERT_ANY_THROW(VerNoneExt0::__ref(ptr3));
#endif

    ASSERT_EQ       (VerNoneExt1::__ref(ptr2).sizeOf(), VerNoneExt0::__ref(ptr2).sizeOf());

#ifdef VERY_FULL_DEBUG
    ASSERT_ANY_THROW(VerNoneExt1::__ref(ptr2).getEx2Throw());
#endif

    ASSERT_NO_THROW (VerNoneExt1::__ref(ptr3).getEx2Throw());

    ASSERT_EQ      (VerNoneExt1::__ref(ptr2).getEx2Redir().toString(), VerNoneExt1::__ref(ptr2).getEx1().toString());
    ASSERT_EQ      (VerNoneExt1::__ref(ptr2).getEx2Redir().toString(), VerNoneExt0::__ref(ptr2).getEx1().toString());

    ASSERT_EQ      (VerNoneExt1::__ref(ptr2).getEx2Redir().toString(), "Ala");
    ASSERT_EQ      (VerNoneExt1::__ref(ptr3).getEx2Redir().toString(), "Basia");

    ASSERT_EQ      (VerNoneExt1::__ref(ptr2).getS2().toString(), "Kasia");
}

TEST_F( VersionTest, testNonVerExtFromVer) {
    std::vector<std::byte> mem_mgr(65536);

    VersionNone::fehler=0;

    std::byte* ptr = mem_mgr.data();

    ASSERT_NO_THROW(Ver0ExtNone::__new(ptr, "Ala", 11, "Zosia"));
    ASSERT_NO_THROW(Ver0ExtNone::__ref(ptr));

    ASSERT_EQ(Ver0ExtNone::__ref(ptr).getIsVerStored(), false);
    ASSERT_EQ(Ver0ExtNone::__ref(ptr).getSuper().getIsVerStored(), true);
    ASSERT_EQ(Ver0ExtNone::__ref(ptr).getObjVer(), 0);
    ASSERT_EQ(Ver0ExtNone::__ref(ptr).getSuper().getObjVer(), 0);
    ASSERT_EQ(Ver0ExtNone::__ref(ptr).getS1().toString(), "Zosia");

    std::byte* ptr2 = ptr + Ver0ExtNone::__ref(ptr).sizeOf();

    ASSERT_NO_THROW(Ver1ExtNone::__new(ptr2, "Ala", 11, "Zosia", "Kasia"));
    ASSERT_NO_THROW(Ver1ExtNone::__ref(ptr2));

    ASSERT_EQ(Ver1ExtNone::__ref(ptr2).getIsVerStored(), false);
    ASSERT_EQ(Ver1ExtNone::__ref(ptr2).getSuper().getIsVerStored(), true);
    ASSERT_EQ(Ver1ExtNone::__ref(ptr2).getObjVer(), 0);
    ASSERT_EQ(Ver1ExtNone::__ref(ptr2).getSuper().getObjVer(), 1);
    ASSERT_EQ(Ver1ExtNone::__ref(ptr2).getS2Throw().toString(), "Kasia");

    ASSERT_NO_THROW (Ver1ExtNone::__ref(ptr));

#ifdef VERY_FULL_DEBUG
    ASSERT_ANY_THROW(Ver1ExtNone::__ref(ptr).getS2Throw());
#endif
    ASSERT_EQ(Ver1ExtNone::__ref(ptr).getS2RedirS1().toString(), "Zosia");
}

//versioned from versioned obj
//quick tip
//ver 00 <- can be read from 00 01 10 11
//ver 01 <- can be read from 01 11
//ver 10 <- can be read from 10 11
//ver 11 <- can be read from 11

TEST_F( VersionTest, testVerExtFromVer) {
    std::vector<std::byte> mem_mgr(65536);

    VersionNone::fehler=0;

    std::byte* ptr = mem_mgr.data();

    ASSERT_NO_THROW(Ver0Ext0::__new(ptr, "Ala", 11, "Zosia"));
    ASSERT_NO_THROW(Ver0Ext0::__ref(ptr));

    ASSERT_EQ(Ver0Ext0::__ref(ptr).getIsVerStored(), true);
    ASSERT_EQ(Ver0Ext0::__ref(ptr).getSuper().getIsVerStored(), true);
    ASSERT_EQ(Ver0Ext0::__ref(ptr).getObjVer(), 0);
    ASSERT_EQ(Ver0Ext0::__ref(ptr).getSuper().getObjVer(), 0);
    ASSERT_EQ(Ver0Ext0::__ref(ptr).getS1().toString(), "Zosia");

    std::byte* ptr2 = ptr + Ver0Ext0::__ref(ptr).sizeOf();

    ASSERT_NO_THROW(Ver1Ext0::__new(ptr2, "Ala", 11, "Zosia", "Kasia"));
    ASSERT_NO_THROW(Ver1Ext0::__ref(ptr2));

    ASSERT_EQ(Ver1Ext0::__ref(ptr2).getIsVerStored(), true);
    ASSERT_EQ(Ver1Ext0::__ref(ptr2).getSuper().getIsVerStored(), true);
    ASSERT_EQ(Ver1Ext0::__ref(ptr2).getObjVer(), 0);
    ASSERT_EQ(Ver1Ext0::__ref(ptr2).getSuper().getObjVer(), 1);
    ASSERT_EQ(Ver1Ext0::__ref(ptr2).getS1().toString(), "Zosia");

    std::byte* ptr3 = ptr2 + Ver1Ext0::__ref(ptr2).sizeOf();

    ASSERT_NO_THROW(Ver0Ext1::__new(ptr3, "Basia", "Ala", 11, "Zosia"));
    ASSERT_NO_THROW(Ver0Ext1::__ref(ptr3));

    ASSERT_EQ(Ver0Ext1::__ref(ptr3).getIsVerStored(), true);
    ASSERT_EQ(Ver0Ext1::__ref(ptr3).getSuper().getIsVerStored(), true);
    ASSERT_EQ(Ver0Ext1::__ref(ptr3).getObjVer(), 1);
    ASSERT_EQ(Ver0Ext1::__ref(ptr3).getSuper().getObjVer(), 0);
    ASSERT_EQ(Ver0Ext1::__ref(ptr3).getEx1().toString(), "Ala");

    std::byte* ptr4 = ptr3 + Ver0Ext1::__ref(ptr3).sizeOf();

    ASSERT_NO_THROW(Ver1Ext1::__new(ptr4, "Basia", "Ala", 11, "Zosia", "Kasia"));
    ASSERT_NO_THROW(Ver1Ext1::__ref(ptr4));

    ASSERT_EQ(Ver1Ext1::__ref(ptr4).getIsVerStored(), true);
    ASSERT_EQ(Ver1Ext1::__ref(ptr4).getSuper().getIsVerStored(), true);
    ASSERT_EQ(Ver1Ext1::__ref(ptr4).getObjVer(), 1);
    ASSERT_EQ(Ver1Ext1::__ref(ptr4).getSuper().getObjVer(), 1);
    ASSERT_EQ(Ver1Ext1::__ref(ptr4).getEx1().toString(), "Ala");

    //downgrade checks....
    ASSERT_NO_THROW(Ver1Ext0::__ref(ptr));
    ASSERT_EQ(Ver1Ext0::__ref(ptr).sizeOf(), Ver0Ext0::__ref(ptr).sizeOf());
    ASSERT_NO_THROW(Ver0Ext1::__ref(ptr));
    ASSERT_EQ(Ver0Ext1::__ref(ptr).sizeOf(), Ver0Ext0::__ref(ptr).sizeOf());
    ASSERT_NO_THROW(Ver1Ext1::__ref(ptr));
    ASSERT_EQ(Ver1Ext1::__ref(ptr).sizeOf(), Ver0Ext0::__ref(ptr).sizeOf());

#ifdef VERY_FULL_DEBUG
    ASSERT_ANY_THROW(Ver0Ext0::__ref(ptr3)); //get ext[0]base[0] from ext[1]base[0]
    ASSERT_ANY_THROW(Ver1Ext0::__ref(ptr3)); //get ext[0]base[1] from ext[1]base[0]
#endif

    ASSERT_NO_THROW (Ver1Ext1::__ref(ptr3)); //get ext[1]base[1] from ext[1]base[0]
    ASSERT_EQ(Ver1Ext1::__ref(ptr3).sizeOf(), Ver0Ext1::__ref(ptr3).sizeOf());

#ifdef VERY_FULL_DEBUG
    ASSERT_ANY_THROW(Ver0Ext0::__ref(ptr2)); //get ext[0]base[0] from ext[0]base[1]
    ASSERT_ANY_THROW(Ver0Ext1::__ref(ptr2)); //get ext[1]base[0] from ext[0]base[1]
#endif
    ASSERT_NO_THROW (Ver1Ext1::__ref(ptr2)); //get ext[1]base[1] from ext[0]base[1]
    ASSERT_EQ(Ver1Ext1::__ref(ptr2).sizeOf(), Ver1Ext0::__ref(ptr2).sizeOf());

#ifdef VERY_FULL_DEBUG
    ASSERT_ANY_THROW(Ver0Ext0::__ref(ptr4)); //get ext[0]base[0] from ext[0]base[1]
    ASSERT_ANY_THROW(Ver0Ext1::__ref(ptr4)); //get ext[1]base[0] from ext[0]base[1]
    ASSERT_ANY_THROW(Ver1Ext0::__ref(ptr4)); //get ext[1]base[1] from ext[0]base[1]
#endif
}

class DB0_PACKED_ATTR Ver0Ext0Wrong : public o_ext<Ver0Ext0Wrong, VersionZero>{
    friend o_ext<Ver0Ext0Wrong, VersionZero>;
    typedef o_ext<Ver0Ext0Wrong, VersionZero> Super;
protected:
    Ver0Ext0Wrong(uint32_t i, const std::string &s1)
        : Super(i, s1)
    {
        arrangeMembers();
    }

public:
    unsigned int fehler_macher;  //well, this is simply wrong!

    static size_t measure (uint32_t i, const std::string &s1){
        return measureMembersFromBase(i, s1);
    }

    template <class buf_t> static size_t safeSizeOf (buf_t buf){
        return sizeOfMembers(buf);
    }
};

TEST_F( VersionTest, testExtendedFromDynamicBaseButWithStaticMember) {
    std::vector<std::byte> mem_mgr(65536);

    VersionNone::fehler=0;

    std::byte* ptr = mem_mgr.data();
    //it will throw and this is what it should be - CRITICAL ERROR!!
#ifdef VERY_FULL_DEBUG
    ASSERT_ANY_THROW(Ver0Ext0Wrong::__new(ptr, 12, "Ala"));
#else
    (void)ptr;
#endif
}

class DB0_PACKED_ATTR Base : public o_base<Base>{
public:
    std::uint8_t i1 = 1;

    Base(){
        arrangeMembers();
    }
    static size_t measure (){
        return measureMembers();
    }
    template <class buf_t> static size_t safeSizeOf (buf_t buf){
        return sizeOfMembers(buf);
    }
};

class DB0_PACKED_ATTR ExtNoVer : public o_ext<ExtNoVer, Base, 0, false>{
public:
    std::uint8_t i2 = 2;

    ExtNoVer(){
        arrangeMembers();
    }
    static size_t measure (){
        return measureMembersFromBase();
    }
    template <class buf_t> static size_t safeSizeOf (buf_t buf){
        return sizeOfMembers(buf);
    }
};

class DB0_PACKED_ATTR ExtVer : public o_ext<ExtVer, Base>{
public:
    std::uint8_t i2 = 3;

    ExtVer(){
        arrangeMembers();
    }
    static size_t measure (){
        return measureMembersFromBase();
    }
    template <class buf_t> static size_t safeSizeOf (buf_t buf){
        return sizeOfMembers(buf);
    }
};

class DB0_PACKED_ATTR ExtExtNoVer : public o_ext<ExtExtNoVer, ExtNoVer>{
public:
    std::uint8_t i3 = 4;

    ExtExtNoVer(){
        arrangeMembers();
    }
    static size_t measure (){
        return measureMembersFromBase();
    }
    template <class buf_t> static size_t safeSizeOf (buf_t buf){
        return sizeOfMembers(buf);
    }
};
/* triggers compilation error, because of dynamic versioning
class DB0_PACKED_ATTR ExtExtVer : public o_ext<ExtExtVer, ExtVer>{
public:
    int i3 = 0;

    ExtExtVer(){
        arrangeMembers();
    }
    static size_t measure (){
        return measureMembersFromBase();
    }
    template <class buf_t> static size_t safeSizeOf (buf_t buf){
        return sizeOfMembers(buf);
    }
};
*/
class DB0_PACKED_ATTR ExtDynExtVer : public o_ext<ExtDynExtVer, ExtVer>{
public:

    ExtDynExtVer(){
        arrangeMembers();
    }
    static size_t measure (){
        return measureMembersFromBase();
    }
    template <class buf_t> static size_t safeSizeOf (buf_t buf){
        return sizeOfMembers(buf);
    }
};

TEST_F( VersionTest, testExtendedFromExtendedButWithStaticMember) {
    std::vector<std::byte> mem_mgr(65536);

    VersionNone::fehler=0;

    std::byte* ptr = mem_mgr.data();

    auto &ev   = ExtVer::__new(ptr);        ptr += ev.sizeOf();
    auto &env  = ExtNoVer::__new(ptr);      ptr += env.sizeOf();
    auto &eenv = ExtExtNoVer::__new(ptr);   ptr += eenv.sizeOf();
    //line below triggers static assert with appriopriate communicate
    //uncomment to check
    //auto &eev  = ExtExtVer::__new(ptr);     ptr += eev.sizeOf();
    auto &edev = ExtDynExtVer::__new(ptr);  ptr += edev.sizeOf();
    auto &edev2 = ExtDynExtVer::__new(ptr);  ptr += edev2.sizeOf();
}

class DB0_PACKED_ATTR o_test_user : public o_base<o_test_user, 0, true> {
public:
    o_test_user(const std::string &first_name, const std::string &last_name)
    {
        arrangeMembers()
            (o_string::type(), first_name)
            (o_string::type(), last_name);
    }

    static std::size_t measure(const std::string &first_name, const std::string &last_name) {
        return measureMembers()
            (o_string::type(), first_name)
            (o_string::type(), last_name);
    }

    template <class buf_t> static std::size_t safeSizeOf(buf_t buf) {
        return sizeOfMembers(buf)
            (o_string::type())
            (o_string::type());
    }

    const o_string& getFirstName() const {
        return getDynFirst(o_string::type());
    }

    const o_string& getLastName() const {
        return getDynAfter(getFirstName(), o_string::type());
    }
};

class DB0_PACKED_ATTR o_test_user_v2 : public o_base<o_test_user_v2, 1, true> {
public:
    o_test_user_v2(std::uint64_t id, const std::string &last_name, const std::string &first_name)
    {
        arrangeMembers()
            (o_simple<std::uint64_t>::type(), id)
            (o_string::type(), last_name)
            (o_string::type(), first_name);
    }

    static std::size_t measure(std::uint64_t id, const std::string &last_name, const std::string &first_name) {
        return measureMembers()
            (o_simple<std::uint64_t>::type(), id)
            (o_string::type(), last_name)
            (o_string::type(), first_name);
    }

    template <class buf_t> static std::size_t safeSizeOf(buf_t buf) {
        if(getObjVer(buf) == 0) {
            return sizeOfMembers(buf)
                (o_string::type())
                (o_string::type());
        }
        return sizeOfMembers(buf)
            (o_simple<std::uint64_t>::type())
            (o_string::type())
            (o_string::type());
    }

    const o_simple<std::uint64_t>& getID() const {
        return getDynFirst(o_simple<std::uint64_t>::type(), 1);
    }

    const o_string& getFirstName() const {
        if(getObjVer() == 0) {
            return getDynFirst(o_string::type());
        }
        return getDynAfter(getLastName(), o_string::type(), 1);
    }

    const o_string& getLastName() const {
        if(getObjVer() == 0) {
            return getDynAfter(getFirstName(), o_string::type());
        }
        return getDynAfter(getID(), o_string::type(), 1);
    }
};

TEST_F(VersionTest, testRearrangedMembersInVersionedObject) {
    std::size_t obj1_measure = o_test_user::measure("Jan", "Kowalski");
    auto obj1 = std::make_unique<std::byte[]>(obj1_measure);
    auto obj1_ptr = obj1.get();
    o_test_user::__new(obj1_ptr, "Jan", "Kowalski");
    ASSERT_EQ(o_test_user::__ref(obj1_ptr).getFirstName(), "Jan");
    ASSERT_EQ(o_test_user::__ref(obj1_ptr).getLastName(), "Kowalski");
    std::size_t obj1_size = o_test_user::__ref(obj1_ptr).sizeOf();
    ASSERT_EQ(obj1_measure, obj1_size);

    std::size_t obj2_measure = o_test_user_v2::measure(1234, "Kowalski", "Jan");
    auto obj2 = std::make_unique<std::byte[]>(obj2_measure);
    auto obj2_ptr = obj2.get();
    o_test_user_v2::__new(obj2_ptr, 1234, "Kowalski", "Jan");
    ASSERT_EQ(o_test_user_v2::__ref(obj2_ptr).getID(), uint64_t(1234));
    ASSERT_EQ(o_test_user_v2::__ref(obj2_ptr).getFirstName(), "Jan");
    ASSERT_EQ(o_test_user_v2::__ref(obj2_ptr).getLastName(), "Kowalski");
    std::size_t obj2_size = o_test_user_v2::__ref(obj2_ptr).sizeOf();
    ASSERT_EQ(obj2_measure, obj2_size);
    ASSERT_EQ(obj2_size, obj1_size + 8);

    ASSERT_EQ(o_test_user_v2::__ref(obj1_ptr).getFirstName(), "Jan");
    ASSERT_EQ(o_test_user_v2::__ref(obj1_ptr).getLastName(), "Kowalski");
    ASSERT_ANY_THROW(o_test_user_v2::__ref(obj1_ptr).getID());
}

struct DB0_PACKED_ATTR o_test_array_v1 : public o_fixed<o_test_array_v1> {
public:
    std::array<std::uint64_t, 4> data;

    o_test_array_v1() {
        std::fill(data.begin(), data.end(), 0);
    }
};

struct DB0_PACKED_ATTR o_test_array_v2 : public o_fixed<o_test_array_v2> {
public:
    std::array<std::uint64_t, 8> data;

    o_test_array_v2() {
        std::fill(data.begin(), data.end(), 0);
    }
};

template<int VER>
class DB0_PACKED_ATTR o_test_content : public o_base<o_test_content<VER>, VER, true> {
    using super_t = o_base<o_test_content, VER, true>;

public:
    o_test_content() {
        if(VER == 0) {
            this->arrangeMembers()(o_test_array_v1::type());
        }
        if(VER == 1) {
            this->arrangeMembers()(o_test_array_v2::type());
        }
    }

    static std::size_t measure() {
        if(VER == 0) {
            return super_t::measureMembers()(o_test_array_v1::type());
        }
        if(VER == 1) {
            return super_t::measureMembers()(o_test_array_v2::type());
        }
    }

    template <class buf_t> static std::size_t safeSizeOf(buf_t buf) {
        if(super_t::getObjVer(buf) == 0) {
            return super_t::sizeOfMembers(buf)(o_test_array_v1::type());
        }
        return super_t::sizeOfMembers(buf)(o_test_array_v2::type());
    }

    std::uint64_t* begin() {
        return reinterpret_cast<std::uint64_t*>(this->beginOfDynamicArea());
    }

    std::uint64_t* end() {
        return reinterpret_cast<std::uint64_t*>(this->beginOfDynamicArea() + this->sizeOf() - this->baseSize());
    }

    std::size_t length() {
        return end() - begin();
    }

    std::uint64_t operator[](std::size_t i) {
        return *(begin() + i);
    }
};

TEST_F(VersionTest, testFixedContentReplaceInVersionedObject) {
    using o_test_content_v1 = o_test_content<0>;
    using o_test_content_v2 = o_test_content<1>;

    std::size_t obj1_measure = o_test_content_v1::measure();
    auto obj1 = std::make_unique<std::byte[]>(obj1_measure);
    auto obj1_ptr = obj1.get();
    o_test_content_v1::__new(obj1_ptr);
    std::size_t obj1_size = o_test_content_v1::__ref(obj1_ptr).sizeOf();
    ASSERT_EQ(obj1_measure, obj1_size);
    ASSERT_EQ(o_test_content_v1::__ref(obj1_ptr).length(), 4);
    {
        auto &obj1_ref = o_test_content_v1::__ref(obj1_ptr);
        ASSERT_EQ(std::count(obj1_ref.begin(), obj1_ref.end(), 0), 4);
        std::iota(obj1_ref.begin(), obj1_ref.end(), 1);
    }
    for(std::size_t i = 0; i != 4; ++i) {
        ASSERT_EQ(o_test_content_v1::__ref(obj1_ptr)[i], i + 1);
    }

    std::size_t obj2_measure = o_test_content_v2::measure();
    auto obj2 = std::make_unique<std::byte[]>(obj2_measure);
    auto obj2_ptr = obj2.get();
    o_test_content_v2::__new(obj2_ptr);
    std::size_t obj2_size = o_test_content_v2::__ref(obj2_ptr).sizeOf();
    ASSERT_EQ(obj2_measure, obj2_size);
    ASSERT_EQ(o_test_content_v2::__ref(obj2_ptr).length(), 8);
    {
        auto &obj2_ref = o_test_content_v2::__ref(obj2_ptr);
        ASSERT_EQ(std::count(obj2_ref.begin(), obj2_ref.end(), 0), 8);
        std::iota(obj2_ref.begin(), obj2_ref.end(), 1);
    }
    for(std::size_t i = 0; i != 8; ++i) {
        ASSERT_EQ(o_test_content_v2::__ref(obj2_ptr)[i], i + 1);
    }

    auto &obj1_ref = o_test_content_v1::__ref(obj1_ptr);
    auto &obj1_backref = o_test_content_v2::__ref(obj1_ptr);
    ASSERT_EQ(obj1_ref.sizeOf(), obj1_backref.sizeOf());
    ASSERT_EQ(obj1_ref.length(), obj1_backref.length());
    ASSERT_TRUE(std::equal(obj1_ref.begin(), obj1_ref.end(), obj1_backref.begin(), obj1_backref.end()));
}

DB0_PACKED_END

} //namespace tests
