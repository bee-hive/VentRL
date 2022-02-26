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
from sklearn import svm, preprocessing
from collections import Counter
import copy as cp
import math
from sklearn.preprocessing import StandardScaler
import logging, sys
import datetime as dt
from datetime import date, datetime, timedelta
from sklearn import svm, preprocessing
from sklearn import linear_model as lm
import seaborn as sns
from sklearn.metrics import roc_curve, auc, precision_recall_curve
from ggplot import *
from sklearn.linear_model import SGDRegressor
from scipy import stats
from scipy.stats import binned_statistic, binned_statistic_dd
from sklearn.preprocessing import StandardScaler  
from sklearn.neural_network import MLPRegressor, MLPClassifier
from sklearn.ensemble import ExtraTreesRegressor, ExtraTreesClassifier

def runFQI(classifier, features, gamma=0.9, batchSize=60000, iterations=100):
    
    ventoff = list(np.concatenate(np.where([ventActions[i] == 0 for i in trainSet])))
    trainSize = len(trainSamples)
    indices = (np.hstack([range(trainSize), ventoff, ventoff, ventoff, ventoff, ventoff]))
    possibleActions = unique(ventActions)
    print 'Initialization'
    #batch = unique(random.choice(indices, batchSize, replace=False)).astype(int)
    batch = indices
    if classifier =='NN':
        Qest = fitNN([np.transpose([trainSamples[s][i] for i in features]) for s in batch], ([trainRewards[s] for s in batch]))
    elif classifier =='Tree':
        Qest = fitTree([np.transpose([trainSamples[s][i] for i in features]) for s in batch], ([trainRewards[s] for s in batch]))
    Q = np.zeros((len(possibleActions), trainSize))
    #Q = 0.1 * np.random.randn(len(possibleActions), trainSize)
    Qdist = []

    print 'Q-iteration'
    iter = 0 
    while iter < iterations:
        batch = random.choice(indices, batchSize, replace=False).astype(int)     
        S = {}
        Qold = cp.deepcopy(Q) 
        anum = 0
        for a in possibleActions:
            Q[anum,batch] = Qest.predict(([np.hstack([np.transpose([trainNextstates[s][i] for i in features[:-1]]), [a]]) for s in batch]))
            anum += 1
        Qdist.append(np.array(mean(abs(np.matrix(Qold) - np.matrix(Q)))))

        optA = [np.argmax(np.transpose(Q[:,s])) for s in batch]
        T = [(trainRewards[s] + gamma*max(np.transpose(Q[:,s]))) for s in batch]
        
        if classifier =='NN':
            Qest = partialfitNN(Qest, [np.transpose([trainSamples[s][i] for i in features]) for s in batch], T)
        elif classifier =='Tree':
            Qest = fitTree([np.transpose([trainSamples[s][i] for i in features]) for s in batch], T)

        S = {'n': batch, 'T': T, 'optA': optA}
        print 'Iter:', iter, '; Qdiff:', Qdist[len(Qdist)-1]    
        iter = iter + 1
    
    return Qest, Q, Qdist
    
    
def fitTree(samples,targets):
    clf = ExtraTreesRegressor(n_estimators=50, max_depth=None, min_samples_leaf=50, random_state=1, warm_start=False)
    clf.fit(samples, targets)
    return clf

def fitNN(samples, targets):
    clf = MLPRegressor(solver='adam', alpha=1e-5,learning_rate='adaptive',hidden_layer_sizes=(5,2),random_state=None)
    clf.fit(samples, targets)
    return clf

def partialfitNN(clf, samples, targets):
    clf.partial_fit(samples, targets)
    return clf

def fitCtree(states, actions):    
    clf = ExtraTreesClassifier(n_estimators=500, max_depth=None, min_samples_leaf=50, class_weight='balanced')
    clf.fit(states, actions)
    return clf

def getImportances(Q, features):
    trainSize = len(trainSet)
    optA = [np.argmax(np.transpose(Q[:,s])) for s in range(trainSize)]
    policyTree = fitCtree([currStatesScaled[s][features] for s in range(trainSize)], optA)
    weights = policyTree.feature_importances_
    #std = np.std([tree.feature_importances_ for tree in policyTree.estimators_],axis=0)
    importanceDf = pd.DataFrame(data={"feats": [feats[x] for x in features], "importances": weights}, 
                                index=[feats[x] for x in features])    
    return weights, importanceDf, policyTree