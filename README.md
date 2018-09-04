# VentRL
This repository comprises work on an RL approach to weaning of mechanical ventilation in ICU, based on [this paper](https://arxiv.org/abs/1704.06300).

## Data
The code assumes access to the MIMIC-III database: https://mimic.physionet.org/. To run the project from the included files, a local postgres SQL server must be installed and the MIMIC-III database must be set up as described in https://github.com/MIT-LCP/mimic-code/tree/master/buildmimic.

## Dependencies

- python 2.7
- numpy
- pandas
- os
- pickle
- psycopg2
- scikit-learn
- scipy
- matplotlib
- seaborn

This is research level code, and will continue to be updated.
If you have any questions/comments, please email <np6@princeton.edu>. Thanks!
