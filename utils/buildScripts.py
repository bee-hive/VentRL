import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import datetime as dt
import os, sys, pickle, json, time, math, re
from joblib import Parallel, delayed
import argparse
import mimicConcepts as mc

print '-------- Creating hadms lists --------'

allIDs = open('hadms.txt', 'r').read() 
allIDs = [int(i) for i in allIDs.split()]
print 'Total # IDs:', len(allIDs)

numP = 200
size = len(allIDs)/numP + 1
partitionedIDs = [(' '.join('{0}'.format(h) for h in allIDs[i:i+size])) for i in range(0, len(allIDs), size)]
print '# Partitions:', numP, 'Size:', size

for i in range(numP):
    partitions_path='tmp/splithadms'+str(i)+'.txt'
    open(partitions_path, 'w').writelines(partitionedIDs[i])

print '-------- Creating slurm script --------'

header = ["#!/bin/bash",
          "#SBATCH -N 1",
          "#SBATCH --ntasks-per-node=20",
          "#SBATCH -t 4:00:00",
          "#SBATCH --mem=30000",
          "#SBATCH --output=/tigress/BEE/mimic/usr/np6/vent-public/processed-data/h_frames/tmp/%u_%j.out",
          "export OMP_NUM_THREADS=30",
          "export KMP_AFFINITY=granularity=fine,compact,1,0;",
          "export OMP_NESTED=TRUE",
          "export OMP_MAX_ACTIVE_LEVELS=4",
          "export PATH=\"/tigress/BEE/mimic/usr/np6/miniconda2/bin/python:$PATH\"",
          "WORKDIR=/tigress/BEE/mimic/usr/np6/vent-public", "" ]
slurm_path = "/tigress/BEE/mimic/usr/np6/vent-public/utils/slurm.sh"
open(slurm_path, 'w').writelines('\n'.join(header))

args = ["", "H=$2", ""]
open('slurm.sh', 'a').writelines('\n'.join(args))

command = ["python $WORKDIR/utils/dataPreparation.py --h $H  > $WORKDIR/processed_data/h_frames/log/fe${H}.log", ""]
open('slurm.sh', 'a').writelines('\n'.join(command))

print '-------- Creating submit script --------'

loop_command = ["EXP_NAME=\"frame_extraction\"",
                "for N in {0..199}; do", 
                "sbatch --job-name=${EXP_NAME}_${N} "+ slurm_path + " --h tmp/splithadms${N}.txt", 
                "done", ""]
open('submit.sh', 'w').writelines('\n'.join(loop_command))
