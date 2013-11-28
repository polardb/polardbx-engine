/* Copyright (c) 2013, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA */

#ifndef PREALLOCED_ARRAY_INCLUDED
#define PREALLOCED_ARRAY_INCLUDED

#include "my_global.h"
#include "my_sys.h"
#include "my_dbug.h"

/**
  A typesafe replacement for DYNAMIC_ARRAY. We do our own memory management,
  and pre-allocate space for a number of elements. The purpose is to
  pre-allocate enough elements to cover normal use cases, thus saving
  malloc()/free() overhead.
  If we run out of space, we use malloc to allocate more space.

  The interface is chosen to be similar to std::vector.
  We keep the std::vector property that storage is contiguous.

  @remark
  Unlike DYNAMIC_ARRAY, elements are properly copied
  (rather than memcpy()d) if the underlying array needs to be expanded.

  @remark
  Depending on Has_trivial_destructor, we destroy objects which are
  removed from the array (including when the array object itself is destroyed).

  @tparam Element_type The type of the elements of the container.
          Elements must be copyable.
  @tparam Prealloc Number of elements to pre-allocate.
  @tparam Has_trivial_destructor If true, we don't destroy elements.
          We could have used type traits to determine this.
          __has_trivial_destructor is supported by some (but not all)
          compilers we use.
          We set the default to true, since we will most likely store pointers
          (shuffling objects around may be expensive).
 */
template<typename Element_type,
         size_t Prealloc,
         bool Has_trivial_destructor = true>
class Prealloced_array
{
  /**
    Casts the raw buffer to the proper Element_type.
    We use a raw buffer rather than Element_type[] in order to avoid having
    CTORs/DTORs invoked by the C++ runtime.
  */
  Element_type *cast_rawbuff()
  {
    return static_cast<Element_type*>(static_cast<void*>(&m_buff[0]));
  }
public:
  explicit Prealloced_array(PSI_memory_key psi_key)
    : m_size(0), m_capacity(Prealloc), m_array_ptr(cast_rawbuff()),
      m_psi_key(psi_key)
  {
    // We do not want a zero-size array.
    compile_time_assert(Prealloc != 0);
  }

  /**
    An object instance "owns" its array, so we do deep copy here.
   */
  Prealloced_array(const Prealloced_array &that)
    : m_size(0), m_capacity(Prealloc), m_array_ptr(cast_rawbuff()),
      m_psi_key(that.m_psi_key)
  {
    if (this->reserve(that.capacity()))
      return;
    for (const Element_type *p= that.begin(); p != that.end(); ++p)
      this->push_back(*p);
  }

  /**
    Runs DTOR on all elements if needed.
    Deallocates array if we exceeded the Preallocated amount.
   */
  ~Prealloced_array()
  {
    if (!Has_trivial_destructor)
    {
      clear();
    }
    if (m_array_ptr != cast_rawbuff())
      my_free(m_array_ptr);
  }

  size_t capacity() const     { return m_capacity; }
  size_t element_size() const { return sizeof(Element_type); }
  bool   empty() const        { return m_size == 0; }
  size_t size() const         { return m_size; }

  Element_type &at(size_t n)
  {
    DBUG_ASSERT(n < size());
    return m_array_ptr[n];
  }

  const Element_type &at(size_t n) const
  {
    DBUG_ASSERT(n < size());
    return m_array_ptr[n];
  }

  Element_type &operator[](size_t n) { return at(n); }
  const Element_type &operator[](size_t n) const { return at(n); }

  Element_type &back() { return at(size() - 1); }
  const Element_type &back() const { return at(size() - 1); }

  Element_type &front() { return at(0); }
  const Element_type &front() const { return at(0); }

  typedef Element_type *iterator;
  typedef const Element_type *const_iterator;

  /**
    begin : Returns a pointer to the first element in the array.
    end   : Returns a pointer to the past-the-end element in the array.
   */
  iterator begin() { return m_array_ptr; }
  iterator end()   { return m_array_ptr + size(); }
  const_iterator begin() const { return m_array_ptr; }
  const_iterator end()   const { return m_array_ptr + size(); }

  /**
    Reserves space for array elements.
    Copies over existing elements, in case we are re-expanding the array.

    @param  n number of elements.
    @retval true if out-of-memory, false otherwise.
  */
  bool reserve(size_t n)
  {
    if (n <= m_capacity)
      return false;

    void *mem= my_malloc(m_psi_key, n * element_size(), MYF(MY_WME));
    if (!mem)
      return true;
    Element_type *new_array= static_cast<Element_type*>(mem);

    // Copy all the existing elements into the new array.
    for (size_t ix= 0; ix < m_size; ++ix)
    {
      Element_type *new_p= &new_array[ix];
      const Element_type &old_p= m_array_ptr[ix];
      ::new (new_p) Element_type(old_p);    // Copy into new location.
      if (!Has_trivial_destructor)
        old_p.~Element_type();              // Destroy the old element.
    }

    if (m_array_ptr != cast_rawbuff())
      my_free(m_array_ptr);

    // Forget the old array;
    m_array_ptr= new_array;
    m_capacity= n;
    return false;
  }

  /**
    Copies an element into the back of the array.
    Complexity: Constant (amortized time, reallocation may happen).
    @retval true if out-of-memory, false otherwise.
   */
  bool push_back(const Element_type &element)
  {
    const size_t expansion_factor= 2;
    if (m_size == m_capacity && reserve(m_capacity * expansion_factor))
      return true;
    Element_type *p= &m_array_ptr[m_size++];
    ::new (p) Element_type(element);
    return false;
  }

  /**
    Removes the last element in the array, effectively reducing the
    container size by one. This destroys the removed element.
   */
  void pop_back()
  {
    DBUG_ASSERT(!empty());
    if (!Has_trivial_destructor)
      back().~Element_type();
    m_size-= 1;
  }

  /**
    Removes a single element from the array.
    The removed element is destroyed.
    This effectively reduces the container size by one.

    This is generally an inefficient operation, since we need to copy
    elements to fill the "hole" in the array.
   */
  void erase(size_t ix)
  {
    if (!Has_trivial_destructor)
      at(ix).~Element_type();

    for (; ix < size() - 1; ++ix)
    {
      Element_type *to= &at(ix);
      const Element_type &from= at(ix + 1);
      ::new (to) Element_type(from);
      if (!Has_trivial_destructor)
        from.~Element_type();
    }
    m_size-= 1;
  }

  /**
    Resizes the container so that it contains n elements.

    If n is smaller than the current container size, the content is
    reduced to its first n elements, removing those beyond (and
    destroying them).

    If n is greater than the current container size, the content is
    expanded by inserting at the end as many elements as needed to
    reach a size of n. If val is specified, the new elements are
    initialized as copies of val, otherwise, they are
    value-initialized.

    If n is also greater than the current container capacity, an automatic
    reallocation of the allocated storage space takes place.

    Notice that this function changes the actual content of the
    container by inserting or erasing elements from it.
   */
  void resize(size_t n, Element_type val= Element_type())
  {
    if (n == m_size)
      return;
    if (n > m_size)
    {
      if (!reserve(n))
      {
        while (n != m_size)
          push_back(val);
      }
      return;
    }
    if (!Has_trivial_destructor)
    {
      while (n != m_size)
        pop_back();
    }
    m_size= n;
  }

  /**
    Removes (and destroys) all elements.
    Does not change capacity.
   */
  void clear()
  {
    if (!Has_trivial_destructor)
    {
      for (Element_type *p= begin(); p != end(); ++p)
        p->~Element_type();                     // Destroy discarded element.
    }
    m_size= 0;
  }

private:
  size_t         m_size;
  size_t         m_capacity;
  // This buffer must be properly aligned. Two size_t above should ensure it.
  char           m_buff[Prealloc * sizeof(Element_type)];
  Element_type  *m_array_ptr;
  PSI_memory_key m_psi_key;

  // Not (yet) implemented.
  Prealloced_array &operator=(const Prealloced_array&);
};

#endif  // PREALLOCED_ARRAY_INCLUDED
