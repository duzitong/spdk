echo "=================================="
echo "Recover POC process on the node:"
echo "=================================="

nohup ./build/bin/nvmf_tgt -m 0x10 &> log.txt &
sleep 2
./scripts/rpc.py < rpc-jinja.txt

echo "=================================="
echo "POC process recovered."
echo "=================================="