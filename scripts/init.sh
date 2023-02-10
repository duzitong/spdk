#/bin/bash

git submodule update --init
./scripts/pkgdep.sh --all
./configure --with-rdma --with-virtio
apt install -y nvme-cli
pip3 install --upgrade ansible
