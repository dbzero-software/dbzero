/////////////////////////////////////////////////////////////////////////////
// Original work:
// (C) Copyright Joaqu�n M L�pez Mu�oz  2006-2007
// Distributed under the Boost Software License, Version 1.0.
//    (See THIRD_PARTY_LICENSES/BOOST_LICENSE_1_0 or
//     http://www.boost.org/LICENSE_1_0.txt)
//
// This file may contain modifications by DBZero Software sp. z o.o.
// Any modifications are Copyright (c) 2025 DBZero Software sp. z o.o.
// and licensed under LGPL-2.1.
//
// SPDX-License-Identifier: BSL-1.0 AND LGPL-2.1
/////////////////////////////////////////////////////////////////////////////

#pragma once
	
#include "mpl.hpp"
	
namespace intrusive {

namespace detail {

template<typename T, bool IsEmpty = true>
class ebo_functor_holder_impl
{
   public:
   ebo_functor_holder_impl(){}
   ebo_functor_holder_impl(const T& t):t(t){}

   T&       get(){return t;}
   const T& get()const{return t;}

   private:
   T t;
};

template<typename T>
class ebo_functor_holder_impl<T, false>
   :  public T
{
   public:
   ebo_functor_holder_impl(){}
   ebo_functor_holder_impl(const T& t):T(t){}

   T&       get(){return *this;}
   const T& get()const{return *this;}
};

template<typename T>
class ebo_functor_holder
   :  public ebo_functor_holder_impl<T, is_unary_or_binary_function<T>::value>
{
   private:
   typedef ebo_functor_holder_impl<T, is_unary_or_binary_function<T>::value> super;

   public:
   ebo_functor_holder(){}
   ebo_functor_holder(const T& t):super(t){}

   ebo_functor_holder& operator=(const ebo_functor_holder& x)
   {
      this->get()=x.get();
      return *this;
   }
};

}  //namespace detail {
}  //namespace intrusive {


