#!/usr/bin/env python3

import argparse
import re

IOPS_PREFIX = "         Wals1               :"
AVERAGE_LATENCY_PREFIX = "Average:"
MEDIUM_LATENCY_PREFIX = " 50.00000% :"

iops = []
average_latency = []
medium_latency = []

iops_cnt = 0
average_lat_cnt = 0
medium_lat_cnt = 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser("bdevperf result parser")
    parser.add_argument("-f", dest="filename", type=str, help="Filename for the bdevperf result", required=True)
    args = parser.parse_args()
    with open(args.filename, "r") as f:
        for line in f.readlines():
            if line.startswith(IOPS_PREFIX):
                iops_cnt += 1
                if iops_cnt % 2 == 0:
                    continue

                iops.append(
                    float(
                        re.search(r'([0-9]*[.])?[0-9]+', line[len(IOPS_PREFIX):]).group()
                    )
                )

            if line.startswith(AVERAGE_LATENCY_PREFIX):
                average_lat_cnt += 1
                if average_lat_cnt % 2 == 0:
                    continue

                average_latency.append(
                    float(
                        re.search(r'([0-9]*[.])?[0-9]+', line[len(AVERAGE_LATENCY_PREFIX):]).group()
                    )
                )
            
            if line.startswith(MEDIUM_LATENCY_PREFIX):
                medium_lat_cnt += 1
                if medium_lat_cnt % 2 == 0:
                    continue

                medium_latency.append(
                    float(
                        re.search(r'([0-9]*[.])?[0-9]+', line[len(MEDIUM_LATENCY_PREFIX):]).group()
                    )
                )


    print("IOPS:")
    print('\n'.join([str(x) for x in iops]))
    print("Lat:")
    print('\n'.join([str(x) for x in average_latency]))



