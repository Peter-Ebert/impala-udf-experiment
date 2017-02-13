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

break DistHashSetMerge

break 413

display src.ptr
display dst->ptr

display merge_buffer

display src_chunk_start
display dst_chunk_start

display src_bucket_val
display dst_bucket_val

display src_cur_loc
display dst_cur_loc

