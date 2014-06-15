QGEN=$HOME/tpch_2_17_0/dbgen/qgen
DISTS=$HOME/tpch_2_17_0/dbgen/dists.dss
for q in `seq 1 22`; do
  DSS_QUERY=queries-pg $QGEN -b $DISTS $q > q-$q.sql
done
