cp ./fat_thin.test ./$1.test
sed -i "s/fat_thin/$1/g" ./$1.test
cp ./fat_thin-master.opt ./$1-master.opt
cp ./fat_thin.yy ./$1.yy
sed -i "s/fat_thin/$1/g" ./$1.yy
cp ./fat_thin.zz ./$1.zz
sed -i "s/fat_thin/$1/g" ./$1.zz
cp ../r/fat_thin.result ../r/$1.result
sed -i "s/fat_thin/$1/g" ../r/$1.result

