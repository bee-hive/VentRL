EXP_NAME="frame_extraction"
for N in {0..199}; do
sbatch --job-name=${EXP_NAME}_${N} /tigress/BEE/mimic/usr/np6/vent-public/utils/slurm.sh --h ../processed_data/splits/splithadms${N}.txt
done
