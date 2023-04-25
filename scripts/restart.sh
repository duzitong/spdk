#/bin/bash
sleep $((RANDOM % 50 + 10))
./scripts/kill.sh nvmf_tgt
sleep $((RANDOM % 50 + 10))
nohup ./build/bin/nvmf_tgt -m 0x10 &> log.txt &
./scripts/rpc.py < rpc-jinja.txt