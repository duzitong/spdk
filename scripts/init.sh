#/bin/bash

git submodule update --init
apt update
./scripts/pkgdep.sh --all
./configure --with-rdma --with-virtio
apt install -y nvme-cli
pip3 install --upgrade ansible

./scripts/setup.sh status > tmp.txt
bdf=$(cat tmp.txt | grep nvme0n1 | awk '{ print $2 }')
HUGEMEM=32768 PCI_ALLOWED="$bdf" ./scripts/setup.sh
rm -f tmp.txt

make