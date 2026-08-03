/////////////////////////////////////////////////////////////////////////////
// Original work:
// (C) Copyright Ion Gaztanaga 2007
// Distributed under the Boost Software License, Version 1.0.
//    (See THIRD_PARTY_LICENSES/BOOST_LICENSE_1_0 or
//     http://www.boost.org/LICENSE_1_0.txt)
//
// This file may contain modifications by Wojciech Sebastian Kozlowski
// Any modifications are Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski
// and licensed under LGPL-2.1-or-later.
//
// SPDX-License-Identifier: BSL-1.0 AND LGPL-2.1-or-later
/////////////////////////////////////////////////////////////////////////////

#ifndef BOOST_VSO_INTRUSIVE_NO_EXCEPTION_SUPPORT_HPP

#if !(defined BOOST_VSO_INTRUSIVE_DISABLE_EXCEPTION_HANDLING)
#    include <boost/detail/no_exceptions_support.hpp>
#    define BOOST_VSO_INTRUSIVE_TRY        BOOST_TRY
#    define BOOST_VSO_INTRUSIVE_CATCH(x)   BOOST_CATCH(x)
#    define BOOST_VSO_INTRUSIVE_RETHROW    BOOST_RETHROW
#    define BOOST_VSO_INTRUSIVE_CATCH_END  BOOST_CATCH_END
#else
#    define BOOST_VSO_INTRUSIVE_TRY        { if (true)
#    define BOOST_VSO_INTRUSIVE_CATCH(x)   else if (false)
#    define BOOST_VSO_INTRUSIVE_RETHROW
#    define BOOST_VSO_INTRUSIVE_CATCH_END  }
#endif

#endif   //#ifndef BOOST_VSO_INTRUSIVE_NO_EXCEPTION_SUPPORT_HPP
