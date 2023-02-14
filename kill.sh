# run two times to ensure that it is not the grep process
# proc_1=$(ps ux | grep $1 | head -n 1 | awk -F ' ' '{print $2}')
# proc_2=$(ps ux | grep $1 | head -n 1 | awk -F ' ' '{print $2}')
proc=$(pgrep -f $1 | head -n 1)
if [[ ! -z $proc && "$proc" != "$$" ]]; then
    kill -9 $proc
fi

# if [ "$proc_1" == "$proc_2" ]; then
#     kill -9 $proc_1
# else
#     echo "No such process."
# fi
