a=$(ps ux | grep bdevperf | head -n 1 | awk -F ' ' '{print $2}')

timeout 0.5s build/bin/spdk_trace_record -q -s bdevperf -p $a -f /tmp/spdk_nvmf_record.trace

build/bin/spdk_trace -f /tmp/spdk_nvmf_record.trace > /tmp/trace.txt

head -n 10 /tmp/trace.txt
tail -n 10 /tmp/trace.txt

./scripts/kill.sh bdevperf