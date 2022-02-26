import pandas as pd
import time
import numpy as np
import pickle
import joblib
from joblib import Parallel, delayed
import os
import csv
from pandas import *
import datetime as dt
from datetime import date, datetime, timedelta
from collections import Counter
import math
import copy as cp
from sklearn.ensemble import ExtraTreesRegressor
from sklearn.ensemble import ExtraTreesClassifier

def fitTree(samples,targets):
    clf = ExtraTreesRegressor(n_estimators=50, max_depth=None, min_samples_leaf=10, min_samples_split=2, 
                              random_state=1, warm_start=False)
    clf.fit(samples, targets)
    return clf

def fitCtree(states, actions):
    clf = ExtraTreesClassifier(n_estimators=50, max_depth=None, min_samples_leaf=10, min_samples_split=2, 
                               random_state=0)
    clf.fit(states, actions)
    return clf

with open("pickles/preppedSamples28Feb2.pkl",'rb') as f: 
    currStatesC, nextStatesC, actions, rewards, discretizedActions, discretizedActionSum = pickle.load(f)

# Check if vent on based on timeIn != 0
voffIndices = np.where(np.transpose(currStatesC)[29] == 0)[0]
vonIndices = np.nonzero(np.transpose(currStatesC)[29])[0]

trainingSet = np.sort(np.concatenate((voffIndices[:65020], vonIndices[:259115]) , axis=0)) 
testSet = np.sort(np.concatenate((voffIndices[65020:], vonIndices[259115:]) , axis=0)) 

batchSize = 50000
samples = np.hstack((currStatesC, discretizedActionSum))
actionChoices = (np.vstack({tuple(row) for row in discretizedActionSum}))

print 'Initialization'
batch = np.random.choice(trainingSet, batchSize, replace=False)
Qtree = fitTree([samples[s] for s in trainingSet], [rewards[s] for s in trainingSet])
gamma = 0.9
Q = np.zeros((len(actionChoices), len(trainingSet)))
Qdist = []

print 'Q-iteration'
iter = 0 
while iter < 100:
    batch = np.random.choice(trainingSet, batchSize, replace=False)
    S = {}
    Qold = cp.deepcopy(Q) 
    anum = 0
    for a in actionChoices:
        Q[anum,batch] = Qtree.predict([np.hstack((nextStatesC[s], a)) for s in batch])
        anum += 1
    Qdist.append(np.array(np.mean(abs(np.matrix(Qold) - np.matrix(Q)))))
    
    optA = [np.argmax(np.transpose(Q[:,s])) for s in batch]
    T = [(rewards[s] + gamma*max(np.transpose(Q[:,s]))) for s in batch]
    Qtree = fitTree([samples[s] for s in batch], T)
    S = {'n': batch, 'T': T, 'optA': optA}
    print 'Iter:', iter, '; Qdiff:', Qdist[len(Qdist)-1]    
    iter = iter + 1

print 'Policy tree'
optA = [np.argmax(np.transpose(Q[:,s])) for s in trainingSet]
policyTree = fitCtree([currStatesC[s] for s in trainingSet], optA) 

with open("pickles/fqi7Mar.pkl",'wb') as f:
    pickle.dump((Qtree, policyTree, Q, Qold, Qdist, S, optA, actionChoices), f)