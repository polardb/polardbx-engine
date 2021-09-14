# test end key push down in write batch
begin;
eval insert into $table (k1, k2, c1, c2) values ($kv1, $kv1, $cv1, $cv1);
eval insert into $table (k1, k2, c1, c2) values ($kv2, $kv2, $cv2, $cv2);
eval insert into $table (k1, k2, c1, c2) values ($kv3, $kv3, $cv3, $cv3);
eval insert into $table (k1, k2, c1, c2) values ($kv4, $kv4, $cv4, $cv4);
eval insert into $table (k1, k2, c1, c2) values ($kv5, $kv5, $cv5, $cv5);

eval select count(1) = 1 from $table where k1 > $kv2 and k1 < $kv4;
eval select count(1) = 3 from $table where k1 >= $kv2 and k1 <= $kv4;
eval select count(1) = 1 from $table where k1 > $kv2 and k2 > $kv2 and k1 < $kv4 and k2 < $kv4;
eval select count(1) = 3 from $table where k1 >= $kv2 and k2 >= $kv2 and k1 <= $kv4 and k2 <= $kv4;
eval select count(1) = 1 from $table force index($index) where c1 > $cv2 and c1 < $cv4;
eval select count(1) = 3 from $table force index($index) where c1 >= $cv2 and c1 <= $cv4;
eval select count(1) = 1 from $table force index($index) where c1 > $cv2 and c2 > $cv2 and c1 < $cv4 and c2 < $cv4;
eval select count(1) = 3 from $table force index($index) where c1 >= $cv2 and c2 >= $cv2 and c1 <= $cv4 and c2 <= $cv4;
commit;

# test end key push down in memtable
eval select count(1) = 1 from $table where k1 > $kv2 and k1 < $kv4;
eval select count(1) = 3 from $table where k1 >= $kv2 and k1 <= $kv4;
eval select count(1) = 1 from $table where k1 > $kv2 and k2 > $kv2 and k1 < $kv4 and k2 < $kv4;
eval select count(1) = 3 from $table where k1 >= $kv2 and k2 >= $kv2 and k1 <= $kv4 and k2 <= $kv4;
eval select count(1) = 1 from $table force index($index) where c1 > $cv2 and c1 < $cv4;
eval select count(1) = 3 from $table force index($index) where c1 >= $cv2 and c1 <= $cv4;
eval select count(1) = 1 from $table force index($index) where c1 > $cv2 and c2 > $cv2 and c1 < $cv4 and c2 < $cv4;
eval select count(1) = 3 from $table force index($index) where c1 >= $cv2 and c2 >= $cv2 and c1 <= $cv4 and c2 <= $cv4;

# test end key push down in sstable
set global xengine_force_flush_memtable_now=on;

eval select count(1) = 1 from $table where k1 > $kv2 and k1 < $kv4;
eval select count(1) = 3 from $table where k1 >= $kv2 and k1 <= $kv4;
eval select count(1) = 1 from $table where k1 > $kv2 and k2 > $kv2 and k1 < $kv4 and k2 < $kv4;
eval select count(1) = 3 from $table where k1 >= $kv2 and k2 >= $kv2 and k1 <= $kv4 and k2 <= $kv4;
eval select count(1) = 1 from $table force index($index) where c1 > $cv2 and c1 < $cv4;
eval select count(1) = 3 from $table force index($index) where c1 >= $cv2 and c1 <= $cv4;
eval select count(1) = 1 from $table force index($index) where c1 > $cv2 and c2 > $cv2 and c1 < $cv4 and c2 < $cv4;
eval select count(1) = 3 from $table force index($index) where c1 >= $cv2 and c2 >= $cv2 and c1 <= $cv4 and c2 <= $cv4;
