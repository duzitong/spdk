export GIT_BRANCH=wals
echo "=================================="
echo "Launching backend (4 data nodes): "
echo "=================================="

ansible-playbook ansible/playbook/dev.yaml -i ansible/inventory/hpc.yaml -e "ansible_ssh_pass=Test@1234567"

echo "=================================="
echo "Launching client (this node):"
echo "=================================="

nohup ./build/bin/nvmf_tgt -r /var/tmp/spdk2.sock -m 0x5 &> log-client.txt &
sleep 2
./scripts/rpc.py -s /var/tmp/spdk2.sock < rpc-client-jinja.txt

echo "=================================="
echo "The disk can be discovered:"
echo "=================================="
nvme discover -t rdma -a 10.0.2.100 -s 4421

echo "=================================="
echo "Finish launching POC"
echo "=================================="