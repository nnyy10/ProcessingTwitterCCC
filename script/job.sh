#!/bin/bash
#SBATCH --partition=physical
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=00:05:00

printf " "
printf "Run job with 1 node and 1 core"

module load Python/3.4.3-goolf-2015a
mpiexec python3 rank.py