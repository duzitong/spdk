#/bin/bash
proc=$(pgrep -f $1 | head -n 1)
if [[ ! -z $proc && "$proc" != "$$" ]]; then
    kill -9 $proc
else
    echo "No such process."
fi
