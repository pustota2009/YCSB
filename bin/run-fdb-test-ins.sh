th=$1
count=$2
exec_time=$3
num=$4
max_num=$5
batch_size=$6
region_count=$7
fcount=$8
conn_type=$9

#asinfo -v 'truncate:namespace=ycsb;set=usertable;'
#source /etc/profile.d/maven.sh

# INSERT
./ycsb load fdb -s \
-jvm-args "-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9011 -Dcom.sun.management.jmxremote.rmi.port=9011 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.local.only=false -Djava.rmi.server.hostname=127.0.0.1" \
-threads $th \
-P ../workloads/workload \
-p table=test_perf \
-p namespace=default \
-p columnfamily=cf \
-p recordcount=$count \
-p operationcount=$count \
-p insertstart=$(($num*$count)) \
-p readproportion=0 \
-p updateproportion=0 \
-p insertproportion=1 \
-p batchsize=$batch_size \
-p regioncount=$region_count \
-p fieldcount=$fcount \
-p connection_nio=$conn_type \
> run.thr$1.cnt$2.tim$3.num$4.max$5.bch$6.rc$7.fc$8.nio-$9.ins.out 2> run.thr$1.cnt$2.tim$3.num$4.max$5.bch$6.rc$7.fc$8.nio-$9.ins
