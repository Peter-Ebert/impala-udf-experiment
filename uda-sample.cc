// Abandon hope all ye who enter here
// code has not been cleaned up at all, TURN BACK NOW

#include "uda-sample.h"
#include <assert.h>
#include <sstream>
#include <algorithm>

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
//todo: murmur3 would be faster, but FNV is easier to implement
// costarring collides with liquid
// declinate collides with macallums
// altarage collides with zinke
// altarages collides with zinkes

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
// static const IntVal UPDATE_BUCKETS = 200000;
// static const IntVal FINALIZE_BUCKETS = 300000;
//Note: seperator cannot be present in source strings (will cause bad counts)
static const StringVal STRING_SEPARATOR((uint8_t*)"Z", 1); //"\0"
static const uint8_t MAGIC_BYTE_DHS = 'H';
static const uint8_t MAGIC_BYTE_DELIMSTR = 'D';
static const uint8_t MAGIC_BYTE_SIZE = 1;
static const int BUCKET_COUNT = 5;
//static const StringVal MAGIC_BYTE_DELIMSTR((uint8_t*)255, 1);

struct DistHashSet {
  //DistHashSet(): buckets(NULL) {}
  //add check to ensure data is well formed? length at start of string?
  uint8_t magic_byte;
  //to test: handle empty string
  double sum;
  int count;
  int size;
  int bucket_count;
  bool buckets_need_alloc;
  StringVal** buckets;
};


// Initialize the StringVal intermediate to a zero'd DistHashSet
void DistHashSetInit300k(FunctionContext* context, StringVal* strvaldhs) {
  strvaldhs->is_null = false;
  strvaldhs->len = sizeof(DistHashSet);
  strvaldhs->ptr = context->Allocate(strvaldhs->len);
  memset(strvaldhs->ptr, 0, strvaldhs->len);

  DistHashSet* dhs = reinterpret_cast<DistHashSet*>(strvaldhs->ptr);
  //set magic byte
  dhs->magic_byte = MAGIC_BYTE_DHS;
  //*(uint8_t *)"s";//

  //could also be moved to update to avoid spinup for finalize where not needed
  dhs->bucket_count = BUCKET_COUNT;
  dhs->buckets_need_alloc = true;

  // dhs->buckets_need_alloc = false;
  // dhs->buckets = (StringVal **) context->Allocate(sizeof(StringVal *) * dhs->bucket_count);
  // memset(dhs->buckets, 0, sizeof(StringVal *) * dhs->bucket_count);

  //StringVal* helloptr = (StringVal*) context->Allocate(sizeof(StringVal));
  //StringVal hello = StringVal("hello");
  // uint8_t* copy = context->Allocate(hello.len);
  // memcpy(copy, hello.ptr, hello.len);

  // helloptr->ptr = copy;
  // helloptr->len = hello.len;
  // helloptr->is_null = false;
  //dhs->buckets[0] = helloptr;
  
  //context->AddWarning("Dev build for test purposes only, NOT TO BE RUN IN PRODUCTION");
}

void DistHashSetUpdate(FunctionContext* context, const StringVal& str, StringVal* strvaldhs) {
  // context->AddWarning("updated");
  if (str.is_null) return;
  assert(!strvaldhs->is_null);
  assert(strvaldhs->len == sizeof(DistHashSet));
  DistHashSet* dhs = reinterpret_cast<DistHashSet*>(strvaldhs->ptr);
  
  if (dhs->buckets_need_alloc) {
    //allocate memory for buckets
    dhs->buckets = (StringVal **) context->Allocate(sizeof(StringVal *) * dhs->bucket_count);
    memset(dhs->buckets, 0, sizeof(StringVal *) * dhs->bucket_count);
    dhs->buckets_need_alloc = false;
  }
  
  ++dhs->count;

  uint64_t mybucket = FnvHash(str.ptr, str.len, FNV64_SEED) % dhs->bucket_count;
  if(!dhs->buckets[mybucket]) {
    //First entry in bucket
    //allocate bucket memory
    dhs->buckets[mybucket] = (StringVal*) context->Allocate(sizeof(StringVal));
    //copy str+delimiter into bucket
    int new_len = STRING_SEPARATOR.len + str.len;
    uint8_t* copy = context->Allocate(new_len);
    memcpy(copy, str.ptr, str.len);
    memcpy(copy+str.len, STRING_SEPARATOR.ptr, STRING_SEPARATOR.len);
    dhs->buckets[mybucket]->is_null = false;
    dhs->buckets[mybucket]->len = new_len;
    dhs->buckets[mybucket]->ptr = copy;

    //context->AddWarning((char *) ToStringVal(context, str.len).ptr);
    
    //memset(dhs->buckets[mybucket], 0, sizeof(StringVal));
    //dhs->buckets[mybucket]->ptr = NULL;

  } else {
    //Collision, search bucket for duplicate, add if no dup
    //!todo: no collision handling
    //loop through current bucket, add only if not found

    uint8_t* loc_start = dhs->buckets[mybucket]->ptr;
    uint8_t* loc_delim;
    uint8_t* bucket_end = dhs->buckets[mybucket]->ptr + dhs->buckets[mybucket]->len - STRING_SEPARATOR.len;
    bool match_found = false;

    while (loc_start < bucket_end) {

      loc_delim = (uint8_t*)memchr(loc_start, *STRING_SEPARATOR.ptr, loc_start - bucket_end);

      if ( (str.len) == (loc_delim - loc_start) ) {
        //strings are same size
        if (!memcmp(str.ptr, loc_start, str.len)) {
          //strings identical, set do not add flag, exit loop
          match_found = true;
          loc_delim = bucket_end;
        }
      }

      loc_start = loc_delim + STRING_SEPARATOR.len;
    }

    if (!match_found) {
      //append

      int new_len = dhs->buckets[mybucket]->len + str.len + STRING_SEPARATOR.len;
      dhs->buckets[mybucket]->ptr = context->Reallocate(dhs->buckets[mybucket]->ptr, new_len);
      memcpy(dhs->buckets[mybucket]->ptr + dhs->buckets[mybucket]->len, str.ptr, str.len);
      memcpy(dhs->buckets[mybucket]->ptr + new_len - STRING_SEPARATOR.len, STRING_SEPARATOR.ptr, STRING_SEPARATOR.len);
      dhs->buckets[mybucket]->len = new_len;

      context->AddWarning("!!APPENDED!!");
      context->AddWarning((char *) dhs->buckets[mybucket]->ptr);
      context->AddWarning((char *) str.ptr);
      context->AddWarning((char *) ToStringVal(context, new_len).ptr);
      //add delim

    }
  }

}

// A serialize function is necesary to free the intermediate state allocation. We use the
// StringVal constructor to allocate memory owned by Impala, copy the intermediate state,
// and free the original allocation. Note that memory allocated by the StringVal ctor is
// not necessarily persisted across UDA function calls, which is why we don't use it in
// AvgInit().
const StringVal DistHashSetSerialize(FunctionContext* context, const StringVal& strvaldhs) {
  //ensure this is a disthashset, use magic byte?
  assert(strvaldhs.len == sizeof(DistHashSet));
  StringVal temp;
  temp.ptr = context->Allocate(sizeof(MAGIC_BYTE_DELIMSTR));
  memcpy(temp.ptr, &MAGIC_BYTE_DELIMSTR, sizeof(MAGIC_BYTE_DELIMSTR));
  temp.len = sizeof(MAGIC_BYTE_DELIMSTR);
  temp.is_null = false;

  //result.is_null = true;
  // if (!result.ptr) {
  //   context->AddWarning("ptr null");
  // }

  //  StringVal result("a\0");
  
  
  if (strvaldhs.ptr[0] == MAGIC_BYTE_DELIMSTR) {
    //intermediate type is delimited string
    //result = StringVal("delim list"); //shouldn't happen
    context->AddWarning("serialize strdelim shouldn't happen");
  } else {

//     StringVal fixed("a\0");
// context->AddWarning((char *) ToStringVal(context, fixed.len).ptr);
//     StringVal result(context, fixed.len);
//     memcpy(result.ptr, fixed.ptr, fixed.len);

    // if(*result.ptr) {
    //   context->AddWarning("not dhs");
    // }

    // StringVal result(context, strvaldhs.len);
    // memcpy(result.ptr, strvaldhs.ptr, strvaldhs.len);

    
    DistHashSet* dhs = reinterpret_cast<DistHashSet*>(strvaldhs.ptr);
    
    //!done: convert to hash ordered delimited string 

    //build serialized string and free memory as we go
    if(!dhs->buckets_need_alloc) {
      //check mem allocation
      for (int i = 0; i < dhs->bucket_count; i++) {
        if (dhs->buckets[i]) {
          if (dhs->buckets[i]->ptr) {
            //always append to list, seperator already added.
            int new_len = temp.len + dhs->buckets[i]->len;
            temp.ptr = context->Reallocate(temp.ptr, new_len);
            memcpy(temp.ptr + temp.len, dhs->buckets[i]->ptr, dhs->buckets[i]->len);
            temp.len = new_len;

            // if (temp.len == 0) {
            //   context->AddWarning("created");
              
            //   context->Free(temp.ptr);
            //   temp.ptr = context->Allocate(dhs->buckets[i]->len);
            //   memcpy(temp.ptr, dhs->buckets[i]->ptr, dhs->buckets[i]->len);
            //   temp.len = dhs->buckets[i]->len;
            //   temp.is_null = false;
              
            // }

            //append to result
            
            //context->AddWarning("always append");
            // int new_len = temp.len + dhs->buckets[i]->len;
            // temp.ptr = context->Reallocate(temp.ptr, new_len);
            // memcpy(temp.ptr + temp.len, dhs->buckets[i]->ptr, dhs->buckets[i]->len);
            // temp.len = new_len;
            
////              memcpy(result.ptr, dhs->buckets[i]->ptr, dhs->buckets[i]->len)
          
            //result = StringVal("1234");
            
            // if(result.len == 0) {
            //   context->AddWarning("res is still 0");
            // }
            
            //free bucket ptrs
            context->Free((uint8_t*) dhs->buckets[i]->ptr);  
          }  
          //free buckets contents
          context->Free((uint8_t*) dhs->buckets[i]);
          //dhs->buckets[i] = NULL;
        }  
      }

      //free buckets array
      context->Free((uint8_t*) dhs->buckets);
    }
    
    //free struct
    context->Free(strvaldhs.ptr);
    /////Memory Freed
  }

  //finalize result
  StringVal result(context, temp.len);
  memcpy(result.ptr, temp.ptr, temp.len);
  context->Free(temp.ptr);
  return result;
}


//todo: rename intermediate
//! when deserializing, check the order, if out of order (incremental) then \0 in the string
// simply loop through the list and append where no duplicates, use larger table and loop through smaller for memmbership until greater hash value detected
// finalize the large combined string by counting each value or /0
void DistHashSetMerge(FunctionContext* context, const StringVal& src, StringVal* dst) {
  //if string contains only magic byte there are no values in the list, can safely return
  if (src.len <= 1) return;



  if (dst->ptr[0] == MAGIC_BYTE_DHS) { //todo:move to end, less likely than other if
    //init was run for dhs, drop and set equal to current string to be merged
    //should happen once per merge
    context->Free(dst->ptr);
    uint8_t* copy = context->Allocate(src.len);
    memcpy(copy, src.ptr, src.len);
    *dst = StringVal(copy, src.len);

  } else if (dst->ptr[0] == MAGIC_BYTE_DELIMSTR) {
    //note:technically if size changed [0] would error
    //merge delimited strings
    //context->AddWarning("merge concat");

    //!todo: loop through source and dst, merge join

    //to avoid having to grow the buffer, set it to the max possible size (shrink at end)
    uint8_t* merge_buffer = context->Allocate(src.len + (dst->len - MAGIC_BYTE_SIZE));
    memcpy(merge_buffer, &MAGIC_BYTE_DELIMSTR, MAGIC_BYTE_SIZE);
    uint8_t* buffer_loc = merge_buffer + MAGIC_BYTE_SIZE;

    //initial values
    //will always be at least one item in delim list
    uint8_t* src_end = src.ptr + src.len;
    uint8_t* dst_end = dst->ptr + dst->len;

    //cur_loc always points to the the start of the string
    uint8_t* src_cur_loc = src.ptr + sizeof(MAGIC_BYTE_DELIMSTR);
    uint8_t* dst_cur_loc = dst->ptr + sizeof(MAGIC_BYTE_DELIMSTR);

    uint8_t* src_chunk_start = src_cur_loc;
    uint8_t* dst_chunk_start = dst_cur_loc;//needed?

    //next_loc always points to the location after the delimiter (begining of next string)
    uint8_t* src_next_loc = (uint8_t*)memchr(src_cur_loc, *STRING_SEPARATOR.ptr, src_cur_loc - src_end) + STRING_SEPARATOR.len;
    uint8_t* dst_next_loc = (uint8_t*)memchr(dst_cur_loc, *STRING_SEPARATOR.ptr, dst_cur_loc - dst_end) + STRING_SEPARATOR.len;

    uint64_t src_bucket_val = FnvHash(src_cur_loc, (src_next_loc - STRING_SEPARATOR.len) - src_cur_loc, FNV64_SEED) % BUCKET_COUNT;
    uint64_t dst_bucket_val = FnvHash(dst_cur_loc, (dst_next_loc - STRING_SEPARATOR.len) - dst_cur_loc, FNV64_SEED) % BUCKET_COUNT;

    uint64_t src_next_bucket_val = src_bucket_val;
    uint64_t dst_next_bucket_val = dst_bucket_val;


    // context->AddWarning((char *) src.ptr);
    // context->AddWarning((char *) dst->ptr);
    // context->AddWarning("---loop---");

    // context->AddWarning((char *) dst_chunk_start);

    do {
      // context->AddWarning("--iter--");
      // context->AddWarning((char *) ToStringVal(context, dst->ptr).ptr);
      // context->AddWarning((char *) ToStringVal(context, (dst_next_loc - STRING_SEPARATOR.len) - dst_cur_loc).ptr);
      // context->AddWarning((char *) ToStringVal(context, dst_bucket_val).ptr);
      // context->AddWarning((char *) ToStringVal(context, src.ptr).ptr);
      // context->AddWarning((char *) ToStringVal(context, (src_next_loc - STRING_SEPARATOR.len) - src_cur_loc).ptr);
      // context->AddWarning((char *) ToStringVal(context, src_bucket_val).ptr);

      // context->AddWarning((char *) ToStringVal(context, dst_bucket_val < src_bucket_val).ptr);
      if (dst_bucket_val < src_bucket_val) { // OR at end of dst?
        //keep going until > src_bucket or end of dst
        
        //context->AddWarning((char *) ToStringVal(context, dst_end-dst_next_loc).ptr);
        
        while (dst_bucket_val < src_bucket_val && dst_next_loc < dst_end) {
          // context->AddWarning("-while-");
          // context->AddWarning((char *) ToStringVal(context, dst_bucket_val).ptr);
          // context->AddWarning((char *) ToStringVal(context, src_bucket_val).ptr);
          //keep searching
          dst_cur_loc = dst_next_loc;
          dst_next_loc = (uint8_t*)memchr(dst_cur_loc, *STRING_SEPARATOR.ptr, dst_cur_loc - dst_end) + STRING_SEPARATOR.len;
          if (dst_cur_loc < dst_end && src_next_loc) {
            dst_bucket_val = FnvHash(dst_cur_loc, (dst_next_loc - STRING_SEPARATOR.len) - dst_cur_loc, FNV64_SEED) % BUCKET_COUNT;  
          } else {
            dst_next_loc = dst_end;  
            context->AddWarning("skip to end dst");
          }
          
          
          // context->AddWarning((char *) ToStringVal(context, dst_bucket_val).ptr);
          // context->AddWarning((char *) ToStringVal(context, dst_next_loc < dst_end).ptr);
          
        }

        //reached end
        if (dst_next_loc == dst_end && dst_bucket_val < src_bucket_val) {
          dst_cur_loc = dst_next_loc;
          context->AddWarning("reached end dst");
        }

        //append dst
        memcpy(buffer_loc, dst_chunk_start, dst_cur_loc - dst_chunk_start);
        buffer_loc += (dst_cur_loc - dst_chunk_start);
        //advance start of chunk
        dst_chunk_start = dst_cur_loc;

        //either way write out to buffer

        // if (dst_next_bucket_val > src_bucket_val) {
        //   //write out to buffer
        // }

      } else if (src_bucket_val < dst_bucket_val){
        //loop through src until > dst_bucket or end of src
        
        //context->AddWarning((char *) ToStringVal(context, src_end-src_next_loc).ptr);
        
        //context->AddWarning((char *) ToStringVal(context, (src_bucket_val < dst_bucket_val && src_next_loc < src_end)).ptr);

        while (src_bucket_val < dst_bucket_val && src_next_loc < src_end) {
          // context->AddWarning("-while src-");
          // context->AddWarning((char *) ToStringVal(context, dst_bucket_val).ptr);
          // context->AddWarning((char *) ToStringVal(context, src_bucket_val).ptr);
          //keep searching
          src_cur_loc = src_next_loc;
          src_next_loc = (uint8_t*)memchr(src_cur_loc, *STRING_SEPARATOR.ptr, src_cur_loc - src_end) + STRING_SEPARATOR.len;
          if (src_cur_loc < src_end && src_next_loc) {
            src_bucket_val = FnvHash(src_cur_loc, (src_next_loc - STRING_SEPARATOR.len) - src_cur_loc, FNV64_SEED) % BUCKET_COUNT;
          } else {
            src_next_loc = src_end;
            context->AddWarning("skip to end src");
          }
          
          
          // context->AddWarning((char *) ToStringVal(context, dst_bucket_val).ptr);
          // context->AddWarning((char *) ToStringVal(context, dst_next_loc < dst_end).ptr);
          
        }

        //end reached and less than, write it all
        if (src_next_loc == src_end && src_bucket_val < dst_bucket_val) {
          src_cur_loc = src_next_loc;//todo:move out?
          context->AddWarning("reached end src");
        }

        //append dst
        memcpy(buffer_loc, src_chunk_start, src_cur_loc - src_chunk_start);
        buffer_loc += src_cur_loc - src_chunk_start;
        src_chunk_start = src_cur_loc;
          
      // context->AddWarning((char *) ToStringVal(context, buffer_loc - merge_buffer).ptr);
      // context->AddWarning((char *) StringVal(merge_buffer, buffer_loc - merge_buffer).ptr);

      //   context->AddWarning("buffer loc");


        //exit: dst_next_loc = dst_end;
      } else {
        context->AddWarning("same");
        //work on this hash value only
        
        //skip if found
        
        //todo: test if last item in both have collision
        //find end of src bucket
        uint8_t* src_bucket_start = src_cur_loc;
        uint64_t src_next_bucket_val = src_bucket_val;
        do {
          src_cur_loc = src_next_loc;
          src_next_loc = (uint8_t*)memchr(src_cur_loc, *STRING_SEPARATOR.ptr, src_cur_loc - src_end) + STRING_SEPARATOR.len;
          src_next_bucket_val = FnvHash(src_cur_loc, (src_next_loc - STRING_SEPARATOR.len) - dst_cur_loc, FNV64_SEED) % BUCKET_COUNT;
        } while (src_next_bucket_val == src_bucket_val && src_cur_loc < src_end);
        uint8_t* src_bucket_end = src_cur_loc;
        //all src variables setup for next outter loop at this point

        //loop through current dst bucket
        do {

          //loop through src, test for duplicate
          bool match_found = false;
          uint8_t* src_inner_cur_loc = src_bucket_start;
          uint8_t* src_inner_next_loc = src_bucket_start;
          while (src_inner_cur_loc < src_bucket_end) {
            src_inner_cur_loc = (uint8_t*)memchr(src_inner_cur_loc, *STRING_SEPARATOR.ptr, src_inner_cur_loc - src_end) + STRING_SEPARATOR.len;
            //todo: check for match
            // if ( (str.len) == (loc_delim - loc_start) ) {
            //   //strings are same size
            //   if (!memcmp(str.ptr, loc_start, str.len)) {
            //     //strings identical, set do not add flag, exit loop
            //     match_found = true;
            //     loc_delim = bucket_end;
            //   }
            // }
          }

          if (!match_found) {
            //append to end
          }

          //if not found
          //append dst
          // memcpy(buffer_loc, dst_chunk_start, dst_cur_loc - dst_chunk_start);
          // buffer_loc += (dst_cur_loc - dst_chunk_start);
        } while (dst_bucket_val == src_bucket_val);

        //todo: add entire src bucket

        // dst_chunk_start = dst_end;
        // src_chunk_start = src_end;
      }

      // context->AddWarning("what's left");
      // context->AddWarning((char *) ToStringVal(context, src_next_loc).ptr);
      // context->AddWarning((char *) ToStringVal(context, dst_next_loc).ptr);

      // context->AddWarning((char *) ToStringVal(dst_bucket_val < src_bucket_val && dst_next_loc < dst_end).ptr);
      // context->AddWarning((char *) ToStringVal(src_bucket_val < dst_bucket_val && src_next_loc < src_end).ptr);

    } while (dst_chunk_start < dst_end && src_chunk_start < src_end);
    //!todo:test empty string at end of list

    //check if one list still contains entries, if so append
    if (dst_chunk_start < dst_end) {
      memcpy(buffer_loc, dst_chunk_start, dst_end - dst_chunk_start);
      buffer_loc += (dst_end - dst_chunk_start);
    } else if (src_chunk_start < src_end) {
      memcpy(buffer_loc, src_chunk_start, src_end - src_chunk_start);
      buffer_loc += src_end - src_chunk_start;
    }

    context->AddWarning("merge buff final");
    // context->AddWarning((char *) ToStringVal(context, buffer_loc - merge_buffer).ptr);
    // context->AddWarning((char *) StringVal(merge_buffer, buffer_loc - merge_buffer).ptr);

    context->Free(dst->ptr);
    dst->ptr = context->Reallocate(merge_buffer, buffer_loc - merge_buffer);
    dst->len = buffer_loc - merge_buffer; 

    // int src_len = src.len - sizeof(MAGIC_BYTE_DELIMSTR);
    // int new_len = dst->len + src_len;
    // dst->ptr = context->Reallocate(dst->ptr, new_len);
    // memcpy(dst->ptr + dst->len, src.ptr + sizeof(MAGIC_BYTE_DELIMSTR), src_len);
    // dst->len = new_len; 

    //context->Free(merge_buffer);

  } else {
    context->AddWarning("undefined");
  }

  
}


StringVal DistHashSetFinalize(FunctionContext* context, const StringVal& strvaldhs) {
  assert(!strvaldhs.is_null);
  int unique_count = 0;
  StringVal result;

  // if(*strvaldhs.ptr) {
  //   result = StringVal("not zero");
  // } else {
  //   result = StringVal("zero");
  // }
  
  //strvaldhs.len > 1 && 
  if (strvaldhs.ptr[0] == MAGIC_BYTE_DELIMSTR) {
    //intermediate type is delimited string
    context->AddWarning("!!Final delimstr"); //debug

    //debug
    //context->AddWarning((char *) strvaldhs.ptr);

    //count number of seperators
    int n = count(strvaldhs.ptr, strvaldhs.ptr + strvaldhs.len, (int) *STRING_SEPARATOR.ptr);
    result = ToStringVal(context, n);
    // for(int i = 0; i < strvaldhs.len; i++) {

    // }
    
    //context->AddWarning("final not dhs");
    // result = StringVal("delim list");
    // if (strvaldhs.len == 1) {
    //   result = StringVal("1");
    // } else if (strvaldhs.len == 3) {
    // result = StringVal("3");
    // } else {
    //   //result = strvaldhs;
    //   result = ToStringVal(context, strvaldhs.len);
    //   //result = StringVal("other");
    // }

    //context->Free(strvaldhs.ptr); 
  } else if (strvaldhs.ptr[0] == MAGIC_BYTE_DHS) {
    context->AddWarning("final dhs");
    //intermediate type is DHS
    //context->AddWarning("final dhs");
    assert(strvaldhs.len == sizeof(DistHashSet));
    DistHashSet* dhs = reinterpret_cast<DistHashSet*>(strvaldhs.ptr);
    
    // if (dhs->count == 0) {
    //   result = StringVal("count 0");
    // } else {
    //   result = StringVal("DHS");
    // }

    ////Free memory
    if(!dhs->buckets_need_alloc) {
      result = StringVal("bucket alloced");
      //check mem allocation
      for (int i = 0; i < dhs->bucket_count; i++) {
        if (dhs->buckets[i]) {
          if (dhs->buckets[i]->ptr) {
            //increment for every separator
            unique_count += count(dhs->buckets[i]->ptr, dhs->buckets[i]->ptr + dhs->buckets[i]->len, (int) *STRING_SEPARATOR.ptr);
            //free bucket ptrs
            context->Free((uint8_t*) dhs->buckets[i]->ptr);  
          }  
          //free buckets contents
          context->Free((uint8_t*) dhs->buckets[i]);
          //dhs->buckets[i] = NULL;
        }  
      }
      //free buckets array
      context->Free((uint8_t*) dhs->buckets);
      result = ToStringVal(context, unique_count);
    } else {
      //this handles an empty dhs, e.g. all nulls or 0 rows
      result = StringVal::null();
      // result = ToStringVal(context, strvaldhs.len);
    }
    //context->Free(strvaldhs.ptr); 
    /////Memory Freed
    
  } else {
    context->AddWarning("neither");
    result = StringVal::null();
  }

  //both paths lead to freeing ptr
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
