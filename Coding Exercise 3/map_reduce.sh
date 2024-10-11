#!/bin/bash

# We initialize variables
M=""
N=""
W=""
p=8084

# We first parse the command line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --M) M="$2"; shift ;;
        --N) N="$2"; shift ;;
        --W) W="$2"; shift ;;
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
echo "----------------------"

echo "N = $N."
echo "M = $M."
echo "Running with $W workers."

python driver.py --M $M --N $N --p $p &

for i in $(seq 1 $M)
do
    python worker.py --p $p &
done

wait
exit
