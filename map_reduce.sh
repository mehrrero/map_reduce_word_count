#!/bin/bash

# We initialize variables
M=""
N=""
W=""
p=""

# We first parse the command line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -M) M="$2"; shift ;;
        -N) N="$2"; shift ;;
        -W) W="$2"; shift ;;
        -p) p="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done
echo "----------------------"

# We check if M, N, or p were not provided and if not, fix them to default values.
if [[ -z "$N" ]]; then
    echo "Resorting to default value for N."
    N=6
fi
if [[ -z "$M" ]]; then
    echo "Resorting to default value for M."
    M=4
fi
if [[ -z "$W" ]]; then
    echo "Resorting to default number of workers."
    W=4
fi
if [[ -z "$p" ]]; then
    echo "Resorting to default port."
    p=8080
fi
echo "----------------------"

echo "N = $N."
echo "M = $M."
echo "Running with $W workers."

python driver.py -M $M -N $N -p $p &

for i in $(seq 1 $W)
do
    python worker.py -p $p &
done

wait
exit
