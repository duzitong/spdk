#!/usr/bin/env python3

import argparse
import re

IOPS_PREFIX = "         Wals1               :"
AVERAGE_LATENCY_PREFIX = "Average:"

iops = []
average_latency = []


if __name__ == "__main__":
    parser = argparse.ArgumentParser("bdevperf result parser")
    parser.add_argument("-f", dest="filename", type=str, help="Filename for the bdevperf result", required=True)
    args = parser.parse_args()
    with open(args.filename, "r") as f:
        for line in f.readlines():
            if line.startswith(IOPS_PREFIX):
                x = float(
                        re.search(r'([0-9]*[.])?[0-9]+', line[len(IOPS_PREFIX):]).group()
                    )
                # small number is from wrong thread
                if x > 10:
                    iops.append(x)
            if line.startswith(AVERAGE_LATENCY_PREFIX):
                average_latency.append(
                    float(
                        re.search(r'([0-9]*[.])?[0-9]+', line[len(AVERAGE_LATENCY_PREFIX):]).group()
                    )
                )

    print("IOPS:")
    print('\n'.join([str(x) for x in iops]))
    print("Lat:")
    print('\n'.join([str(x) for x in average_latency]))



