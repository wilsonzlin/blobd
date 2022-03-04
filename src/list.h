#pragma once

#include <stdint.h>
#include <stdlib.h>

#define LIST_DEF(name, type_t) \
  typedef struct { \
    uint64_t cap; \
    uint64_t len; \
    type_t* elems; \
  } name##_t; \
  \
  name##_t* name##_create_with_capacity(uint64_t init_cap); \
  \
  name##_t* name##_create(); \
  \
  void name##_append(name##_t* list, type_t elem); \
  \
  type_t* name##_last_mut(name##_t* list);

#define LIST(name, type_t) \
  name##_t* name##_create_with_capacity(uint64_t init_cap) { \
    name##_t* list = malloc(sizeof(name##_t)); \
    list->cap = init_cap; \
    list->len = 0; \
    list->elems = malloc(sizeof(type_t) * init_cap); \
    return list; \
  } \
  \
  name##_t* name##_create() { \
    return name##_create_with_capacity(16); \
  } \
  \
  void name##_append(name##_t* list, type_t elem) { \
    if (list->len == list->cap) { \
      uint64_t new_cap = list->cap * 2; \
      list->elems = realloc(list->elems, sizeof(type_t) * new_cap); \
      list->cap = new_cap; \
    } \
    list->elems[list->len++] = elem; \
  } \
  \
  type_t* name##_last_mut(name##_t* list) { \
    if (list->len) { \
      return &list->elems[list->len - 1]; \
    } else { \
      return NULL; \
    } \
  }
