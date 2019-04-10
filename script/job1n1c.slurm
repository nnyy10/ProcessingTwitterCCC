#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=00:30:00

printf "Run job with 1 node and 1 core\n"

module load Python/3.4.3-goolf-2015a
mpiexec python3 main.py
