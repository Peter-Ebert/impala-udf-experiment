// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "uda-sample.h"
#include <assert.h>
#include <sstream>

using namespace impala_udf;
using namespace std;

template <typename T>
StringVal ToStringVal(FunctionContext* context, const T& val) {
  stringstream ss;
  ss << val;
  string str = ss.str();
  StringVal string_val(context, str.size());
  memcpy(string_val.ptr, str.c_str(), str.size());
  return string_val;
}

template <>
StringVal ToStringVal<DoubleVal>(FunctionContext* context, const DoubleVal& val) {
  if (val.is_null) return StringVal::null();
  return ToStringVal(context, val.val);
}

// ---------------------------------------------------------------------------
// This is a sample of implementing a COUNT aggregate function.
// ---------------------------------------------------------------------------
void CountInit(FunctionContext* context, BigIntVal* val) {
  val->is_null = false;
  val->val = 0;
}

void CountUpdate(FunctionContext* context, const IntVal& input, BigIntVal* val) {
  if (input.is_null) return;
  ++val->val;
}

void CountMerge(FunctionContext* context, const BigIntVal& src, BigIntVal* dst) {
  dst->val += src.val;
}

BigIntVal CountFinalize(FunctionContext* context, const BigIntVal& val) {
  return val;
}


//// ---------------------------------------------------------------------------
//   HashSet Distinct Count
//// ---------------------------------------------------------------------------

// Hash Function
static const uint64_t FNV64_PRIME = 1099511628211UL;
static const uint64_t FNV64_SEED = 14695981039346656037UL;

static uint64_t FnvHash(const void* data, int32_t bytes, uint64_t hash) {
  const uint8_t* ptr = reinterpret_cast<const uint8_t*>(data);
  while (bytes--) {
    hash = (*ptr ^ hash) * FNV64_PRIME;
    ++ptr;
  }
  return hash;
}

//HashTable
static const IntVal UPDATE_BUCKETS = 200000;
static const IntVal FINALIZE_BUCKETS = 300000;
//Note: seperator cannot be present in source strings (will cause bad counts)
static const StringVal STRING_SEPARATOR((uint8_t*)"\0", 1);

struct DistHashSet {
  //DistHashSet(): buckets(NULL) {}
  double sum;
  int count;
  int size;
  int bucket_count;
  StringVal** buckets;
};


// Initialize the StringVal intermediate to a zero'd DistHashSet
void DistHashSetInit300k(FunctionContext* context, StringVal* strvaldhs) {
  strvaldhs->is_null = false;
  strvaldhs->len = sizeof(DistHashSet);
  strvaldhs->ptr = context->Allocate(strvaldhs->len);
  memset(strvaldhs->ptr, 0, strvaldhs->len);

  DistHashSet* dhs = reinterpret_cast<DistHashSet*>(strvaldhs->ptr);

  dhs->bucket_count = 30000;
  
  dhs->buckets = (StringVal **) context->Allocate(sizeof(StringVal *) * dhs->bucket_count);
  memset(dhs->buckets, 0, sizeof(StringVal *) * dhs->bucket_count);

  //testing - remove
  // StringVal hello = StringVal("hello");
  // dhs->buckets[0] = &hello;

  StringVal* helloptr = (StringVal*) context->Allocate(sizeof(StringVal));
  StringVal hello = StringVal("hello");
  uint8_t* copy = context->Allocate(hello.len);
  memcpy(copy, hello.ptr, hello.len);

  helloptr->ptr = copy;
  helloptr->len = hello.len;
  helloptr->is_null = false;


  dhs->buckets[0] = helloptr;
  
  //context->AddWarning("Dev build for test purposes only, NOT TO BE RUN IN PRODUCTION");
}

void DistHashSetUpdate(FunctionContext* context, const StringVal& str, StringVal* strvaldhs) {
  if (str.is_null) return;

  assert(!strvaldhs->is_null);
  assert(strvaldhs->len == sizeof(DistHashSet));
  DistHashSet* avg = reinterpret_cast<DistHashSet*>(strvaldhs->ptr);
  
  ++avg->count;

}

// !i when deserializing, check the order, if out of order (incremental) then \0 in the string
// simply loop through the list and append where no duplicates, use larger table and loop through smaller for memmbership until greater hash value detected
// finalize the large combined string by counting each value or /0
void DistHashSetMerge(FunctionContext* context, const StringVal& src, StringVal* dst) {
  //if either string is null, return the other
  if (src.is_null) return;
  const DistHashSet* src_avg = reinterpret_cast<const DistHashSet*>(src.ptr);
  DistHashSet* dst_avg = reinterpret_cast<DistHashSet*>(dst->ptr);
  dst_avg->sum += src_avg->sum;
  dst_avg->count += src_avg->count;



  //FnvHash(hello.c_str(), hello.length(), FNV64_SEED) % 999
}

// A serialize function is necesary to free the intermediate state allocation. We use the
// StringVal constructor to allocate memory owned by Impala, copy the intermediate state,
// and free the original allocation. Note that memory allocated by the StringVal ctor is
// not necessarily persisted across UDA function calls, which is why we don't use it in
// AvgInit().
const StringVal DistHashSetSerialize(FunctionContext* context, const StringVal& strvaldhs) {
  assert(!strvaldhs.is_null);
  StringVal result(context, strvaldhs.len);
  memcpy(result.ptr, strvaldhs.ptr, strvaldhs.len);
  
  DistHashSet* dhs = reinterpret_cast<DistHashSet*>(strvaldhs.ptr);
  

  //free bucket ptrs
  context->Free((uint8_t*) dhs->buckets[0]->ptr);
  //free buckets contents
  context->Free((uint8_t*) dhs->buckets[0]);
  //free buckets
  context->Free((uint8_t*) dhs->buckets);
  //free struct
  context->Free(strvaldhs.ptr);
  return result;
}

StringVal DistHashSetFinalize(FunctionContext* context, const StringVal& strvaldhs) {
  assert(!strvaldhs.is_null);
  assert(strvaldhs.len == sizeof(DistHashSet));
  DistHashSet* dhs = reinterpret_cast<DistHashSet*>(strvaldhs.ptr);
  StringVal result;
  //if string is null, return null
  if (dhs->count == 0) {
    result = StringVal::null();
  } else {
    // Copies the result to memory owned by Impala
    
    // StringVal hello = StringVal("hello");
    // dhs->buckets[0] = &hello;
    if (dhs->buckets[0])
    {
      if(dhs->buckets[0]->is_null) {
        result = StringVal("string null");
      } else {

        result = StringVal(context, dhs->buckets[0]->len);
        memcpy(result.ptr, dhs->buckets[0]->ptr, dhs->buckets[0]->len);
        //result = StringVal(dhs->buckets[0]->ptr, dhs->buckets[0]->len);
      }
    } else {
      result = StringVal("bucket null");
    }

    //result = ToStringVal(context, sizeof(StringVal("hello!")));
    //result = *dhs->buckets[0];
    //result = ToStringVal(context, *dhs->bucket_count);
    //result = StringVal("placeholder");
  }

  //free bucket ptrs
  context->Free((uint8_t*) dhs->buckets[0]->ptr);
  //free buckets contents
  context->Free((uint8_t*) dhs->buckets[0]);
  //free bucket array
  context->Free((uint8_t*) dhs->buckets);
  context->Free(strvaldhs.ptr);
  return result;
}







// ---------------------------------------------------------------------------
// This is a sample of implementing a AVG aggregate function.
// ---------------------------------------------------------------------------
struct AvgStruct {
  double sum;
  int64_t count;
};

// Initialize the StringVal intermediate to a zero'd AvgStruct
void AvgInit(FunctionContext* context, StringVal* val) {
  val->is_null = false;
  val->len = sizeof(AvgStruct);
  val->ptr = context->Allocate(val->len);
  memset(val->ptr, 0, val->len);
}

void AvgUpdate(FunctionContext* context, const DoubleVal& input, StringVal* val) {
  if (input.is_null) return;
  assert(!val->is_null);
  assert(val->len == sizeof(AvgStruct));
  AvgStruct* avg = reinterpret_cast<AvgStruct*>(val->ptr);
  avg->sum += input.val;
  ++avg->count;
}

void AvgMerge(FunctionContext* context, const StringVal& src, StringVal* dst) {
  if (src.is_null) return;
  const AvgStruct* src_avg = reinterpret_cast<const AvgStruct*>(src.ptr);
  AvgStruct* dst_avg = reinterpret_cast<AvgStruct*>(dst->ptr);
  dst_avg->sum += src_avg->sum;
  dst_avg->count += src_avg->count;
}

// A serialize function is necesary to free the intermediate state allocation. We use the
// StringVal constructor to allocate memory owned by Impala, copy the intermediate state,
// and free the original allocation. Note that memory allocated by the StringVal ctor is
// not necessarily persisted across UDA function calls, which is why we don't use it in
// AvgInit().
const StringVal AvgSerialize(FunctionContext* context, const StringVal& val) {
  assert(!val.is_null);
  StringVal result(context, val.len);
  memcpy(result.ptr, val.ptr, val.len);
  context->Free(val.ptr);
  return result;
}

StringVal AvgFinalize(FunctionContext* context, const StringVal& val) {
  assert(!val.is_null);
  assert(val.len == sizeof(AvgStruct));
  AvgStruct* avg = reinterpret_cast<AvgStruct*>(val.ptr);
  StringVal result;
  if (avg->count == 0) {
    result = StringVal::null();
  } else {
    // Copies the result to memory owned by Impala
    result = ToStringVal(context, avg->sum / avg->count);
  }
  context->Free(val.ptr);
  return result;
}

// ---------------------------------------------------------------------------
// This is a sample of implementing the STRING_CONCAT aggregate function.
// Example: select string_concat(string_col, ",") from table
// ---------------------------------------------------------------------------
// Delimiter to use if the separator is NULL.
static const StringVal DEFAULT_STRING_CONCAT_DELIM((uint8_t*)", ", 2);

void StringConcatInit(FunctionContext* context, StringVal* val) {
  val->is_null = true;
}

void StringConcatUpdate(FunctionContext* context, const StringVal& str,
    const StringVal& separator, StringVal* result) {
  if (str.is_null) return;
  if (result->is_null) {
    // This is the first string, simply set the result to be the value.
    uint8_t* copy = context->Allocate(str.len);
    memcpy(copy, str.ptr, str.len);
    *result = StringVal(copy, str.len);
    return;
  }

  const StringVal* sep_ptr = separator.is_null ? &DEFAULT_STRING_CONCAT_DELIM :
      &separator;

  // We need to grow the result buffer and then append the new string and
  // separator.
  int new_size = result->len + sep_ptr->len + str.len;
  result->ptr = context->Reallocate(result->ptr, new_size);
  memcpy(result->ptr + result->len, sep_ptr->ptr, sep_ptr->len);
  result->len += sep_ptr->len;
  memcpy(result->ptr + result->len, str.ptr, str.len);
  result->len += str.len;
}

void StringConcatMerge(FunctionContext* context, const StringVal& src, StringVal* dst) {
  if (src.is_null) return;
  StringConcatUpdate(context, src, ",", dst);
}

// A serialize function is necesary to free the intermediate state allocation. We use the
// StringVal constructor to allocate memory owned by Impala, copy the intermediate
// StringVal, and free the intermediate's memory. Note that memory allocated by the
// StringVal ctor is not necessarily persisted across UDA function calls, which is why we
// don't use it in StringConcatUpdate().
const StringVal StringConcatSerialize(FunctionContext* context, const StringVal& val) {
  if (val.is_null) return val;
  StringVal result(context, val.len);
  memcpy(result.ptr, val.ptr, val.len);
  context->Free(val.ptr);
  return result;
}

// Same as StringConcatSerialize().
StringVal StringConcatFinalize(FunctionContext* context, const StringVal& val) {
  if (val.is_null) return val;
  StringVal result(context, val.len);
  memcpy(result.ptr, val.ptr, val.len);
  context->Free(val.ptr);
  return result;
}
