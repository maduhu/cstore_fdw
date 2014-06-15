
rm -f $1

#for q in `seq 1 22`; do
for q in 3 5 6 7 8 10 12 14 19; do

  sudo ./drop_caches

  start=$(date +%s.%N)
  sudo -u postgres psql rados postgres < q-$q.sql
  end=$(date +%s.%N)
  duration=$(echo "$end - $start" | bc)
  echo "$q,cold,$duration" | tee -a $1

  start=$(date +%s.%N)
  sudo -u postgres psql rados postgres < q-$q.sql
  end=$(date +%s.%N)
  duration=$(echo "$end - $start" | bc)
  echo "$q,hot,$duration" | tee -a $1

done
