echo "=================================="
echo "Issue IO load:"
echo "=================================="

LD_PRELOAD=/root/spdk/build/fio/spdk_nvme /root/fio/fio /root/spdk/scripts/demo/test.fio

# ./build/examples/perf -q 1 -o 4096 -w randwrite -r 'trtype:RDMA adrfam:IPv4 traddr:10.0.2.100 trsvcid:4421' -t 300 -c 0x10