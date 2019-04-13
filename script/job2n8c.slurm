#!/bin/bash
#SBATCH --partition=physical
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --time=00:03:00

printf "Run job with 2 node and 8 core\n"

module load Python/3.4.3-goolf-2015a
mpiexec python3 process_tweets_final.py
