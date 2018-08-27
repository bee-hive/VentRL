# VentRL
This repository comprises work for an RL approach to weaning of mechanical ventilation in ICU, based on [this paper](https://arxiv.org/abs/1704.06300).

## Data
The code assumes access to the MIMIC-III database: https://mimic.physionet.org/. To run the project from the included files, a local postgres SQL server must be installed and the MIMIC-III database must be set up as described in https://github.com/MIT-LCP/mimic-code/tree/master/buildmimic.

## Dependencies

- numpy
- pandas
- os
- psycopg2
- scikit-learn
- scipy
- matplotlib
- seaborn

## Files
