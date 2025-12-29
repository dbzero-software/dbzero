// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

/////////////////////////////////////////////////////////////////////////////
//
// (C) Copyright Ion Gaztanaga 2006. Distributed under the Boost
// Software License, Version 1.0. (See accompanying file
// LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// See http://www.boost.org/libs/intrusive for documentation.
//
/////////////////////////////////////////////////////////////////////////////

#pragma once
	
#include "config_begin.hpp"

namespace boost {

template<class T, class U>
   struct pointer_to_other;

template<class T, class U, template <class> class Sp>
   struct pointer_to_other< Sp<T>, U >
{
   typedef Sp<U> type;
};

template<class T, class T2, class U,
        template <class, class> class Sp>
   struct pointer_to_other< Sp<T, T2>, U >
{
   typedef Sp<U, T2> type;
};

template<class T, class T2, class T3, class U,
        template <class, class, class> class Sp>
struct pointer_to_other< Sp<T, T2, T3>, U >
{
   typedef Sp<U, T2, T3> type;
};

template<class T, class U>
struct pointer_to_other< T*, U > 
{
   typedef U* type;
};

} // namespace boost

#include "config_end.hpp"
