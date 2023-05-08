echo "=================================="
echo "Issue IO payload:"
echo "=================================="

fio --filename=/dev/nvme0n1 --direct=1 --rw=randwrite --bs=4k --ioengine=libaio --iodepth=1 --runtime=600 --numjobs=1 --time_based