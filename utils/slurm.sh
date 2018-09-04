#!/bin/bash
#SBATCH -N 1
#SBATCH --ntasks-per-node=20
#SBATCH -t 4:00:00
#SBATCH --mem=30000
#SBATCH --output=/tigress/BEE/mimic/usr/np6/vent-public/processed_data/h_frames/tmp/%u_%j.out
export OMP_NUM_THREADS=30
export KMP_AFFINITY=granularity=fine,compact,1,0;
export OMP_NESTED=TRUE
export OMP_MAX_ACTIVE_LEVELS=4
export PATH="/tigress/BEE/mimic/usr/np6/miniconda2/bin/python:$PATH"
WORKDIR=/tigress/BEE/mimic/usr/np6/vent-public

H=$2
BASE=`echo '$H' | awk -F"/" '{print $NF}' | awk -F".txt" '{print $1}'`
python $WORKDIR/utils/dataPreparation.py --h $H  > $WORKDIR/processed_data/h_frames/log/${BASE}.log
