drop function hashsetcount(string);

create aggregate function hashsetcount(string) returns string
location '/user/root/udfs/libudasample.so'
init_fn='DistHashSetInit300k'
update_fn='DistHashSetUpdate'
merge_fn='DistHashSetMerge'
finalize_fn='DistHashSetFinalize';


-- init->update->serialize
-- init->serialize->merge->finalize;

Normal
	Init() Update() Serialize() 
	Init() Merge() Finalize()

Num_nodes=1
	Init() Update() Finalize()


//todo: could sort on insert to bucket and merge sort in our merge sort

?????????????
	DWorldZcostarringZliquidZ

2: dst->ptr = (uint8_t *) 0x6a8e90 "DHelloZliquidZ"
1: src.ptr = (uint8_t *) 0x6334c0 "DHelloZliquidZ"
//collision!

break DistHashSetFinalize
watch n = 3

break DistHashSetMerge

watch dst->len = 25

continue 4x

break 413

watch merge_buffer == "DHelloZliquidZ"
watch buffer_loc - merge_buffer  > 8

display src.ptr
display dst->ptr

display merge_buffer

display src_chunk_start
display dst_chunk_start

display src_bucket_val
display dst_bucket_val

display src_cur_loc
display dst_cur_loc

break DistHashSetUpdate
watch dhs->buckets[mybucket] == NULL