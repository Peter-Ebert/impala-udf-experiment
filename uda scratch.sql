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

Local
	Init() Update() Finalize()