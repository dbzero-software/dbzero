// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/exception/AbstractException.hpp>

namespace db0

{

    namespace EXCEPTION_ID_PREFIX  {

        enum : int {
            // common exceptions
            BASIC = 0x00000000,
            // application specific exceptions
            APP = 0x00000100,
            NETWORK = 0x08000000
        };

    }

    class CriticalException: public AbstractException
    {
    public:	
        static constexpr int exception_id = EXCEPTION_ID_PREFIX::BASIC | 0x00caffee;

        CriticalException(int err_id = exception_id);
    };
    
    class RecoverableException: public AbstractException 
    {
    public:	
        static constexpr int exception_id = EXCEPTION_ID_PREFIX::BASIC | 0x0000beef;

        RecoverableException(int err_id);
        virtual ~RecoverableException() = default;
    };

    class InternalException: public CriticalException
    {
    public:
        static constexpr int exception_id = EXCEPTION_ID_PREFIX::BASIC | 0x01;

        InternalException(int err_id = exception_id);
    };
        
    class InputException: public RecoverableException
    {
    public:
        static constexpr int exception_id = EXCEPTION_ID_PREFIX::BASIC | 0x03;

        InputException(int err_id = exception_id);
        virtual ~InputException() = default;
    };
    
    class KeyNotFoundException: public InputException
    {
    public :
        static constexpr int exception_id = EXCEPTION_ID_PREFIX::BASIC | 0x09;

        KeyNotFoundException(int err_id = exception_id);
    };

    class IOException: public RecoverableException
    {
    public:
        static constexpr int exception_id = EXCEPTION_ID_PREFIX::BASIC | 0x02;

        IOException(int err_id = exception_id);
    };

    class OutOfDiskSpaceException: public CriticalException
    {
    public:
        static constexpr int exception_id = EXCEPTION_ID_PREFIX::BASIC | 0x04;

        OutOfDiskSpaceException();
    };

    class MemoryException: public CriticalException
    {
    public:
        static constexpr int exception_id = EXCEPTION_ID_PREFIX::BASIC | 0x0a;

        MemoryException();
    };

    // Language specific class / type was not found
    class ClassNotFoundException: public CriticalException
    {
    public:
        static constexpr int exception_id = EXCEPTION_ID_PREFIX::BASIC | 0x0b;

        ClassNotFoundException();
    };

    class IndexException: public CriticalException
    {
    public:
        static constexpr int exception_id = EXCEPTION_ID_PREFIX::BASIC | 0x0e;

        IndexException();
    };
    
    class AccessTypeException: public CriticalException
    {
    public:
        static constexpr int exception_id = EXCEPTION_ID_PREFIX::BASIC | 0x0c;

        AccessTypeException();
    };

    // Address of the dbzero object does not exist or is no longer valid
    class BadAddressException: public CriticalException
    {
    public:
        static constexpr int exception_id = EXCEPTION_ID_PREFIX::BASIC | 0x0d;

        BadAddressException();
    };

    class CacheException: public CriticalException
    {
    public:
        static constexpr int exception_id = EXCEPTION_ID_PREFIX::BASIC | 0x0f;
        CacheException();
    };

    class PrefixNotFoundException: public RecoverableException
    {
    public:
        static constexpr int exception_id = EXCEPTION_ID_PREFIX::BASIC | 0x10;
        PrefixNotFoundException();
    };

}