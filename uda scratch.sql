
create aggregate function mycount(value as string) returns string
location '/user/root/udfs/libudasample.so'
init_fn='DistHashSetInit300k'
update_fn='DistHashSetUpdate'
merge_fn='DistHashSetMerge'
finalize_fn='DistHashSetFinalize';