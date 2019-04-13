#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --time=00:03:00

printf "Run job with 1 node and 8 core\n"

module load Python/3.4.3-goolf-2015a
mpiexec python3 process_tweets_final.py
