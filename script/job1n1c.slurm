#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=00:07:00

printf "Run job with 1 node and 1 core\n"

module load Python/3.4.3-goolf-2015a
mpiexec python3 process_tweets_final.py
