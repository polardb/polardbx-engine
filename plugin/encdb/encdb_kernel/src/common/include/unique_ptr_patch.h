#pragma once

#include <memory>

#if (__cplusplus <= 202002L)
#include <type_traits>
#include <utility>

// implementations for std::make_unique (since C++14) /
// std::make_unique_for_overwrite (since C++20)

namespace std {

template <class T>
struct _Unique_if {
  typedef unique_ptr<T> _Single_object;
};

template <class T>
struct _Unique_if<T[]> {
  typedef unique_ptr<T[]> _Unknown_bound;
};

template <class T, size_t N>
struct _Unique_if<T[N]> {
  typedef void _Known_bound;
};

// C++20 make_unique_for_overwrite
template <class T>
typename _Unique_if<T>::_Single_object make_unique_for_overwrite() {
  return unique_ptr<T>(new T);
}

template <class T>
typename _Unique_if<T>::_Unknown_bound make_unique_for_overwrite(size_t n) {
  typedef typename remove_extent<T>::type U;
  // allocate memory without initialization to boost performance
  return unique_ptr<T>(new U[n]);
}

#if (__cplusplus <= 201103L)

// implement std::make_unique since c++11 does not provide it

template <class T, class... Args>
typename _Unique_if<T>::_Single_object make_unique(Args &&...args) {
  return unique_ptr<T>(new T(std::forward<Args>(args)...));
}

template <class T>
typename _Unique_if<T>::_Unknown_bound make_unique(size_t n) {
  typedef typename remove_extent<T>::type U;
  return unique_ptr<T>(new U[n]());
}

template <class T, class... Args>
typename _Unique_if<T>::_Known_bound make_unique(Args &&...) = delete;

#endif  //__cplusplus <= 201103L

}  // namespace std

#endif  //__cplusplus <= 202002L
