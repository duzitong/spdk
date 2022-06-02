set -e

modprobe nvme-tcp
./scripts/rpc.py < rpc.txt
nvme connect -t tcp -a 127.0.0.1 -s 4420 -n "nqn.2016-06.io.spdk:cnode1"
# wtf? why canot access the device right after connect?
sleep 1
nvme write /dev/nvme0n1 -z 4096 -d data.txt -y 8 -M metadata.txt
nvme read /dev/nvme0n1 -z 4096
nvme disconnect-all