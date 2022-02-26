%pylab inline
import pandas as pd
import numpy as np
import pickle
import copy as cp
import seaborn as sns
    
plt.style.use('ggplot') 
pd.set_option('display.max_columns', None)

with open("pickles/preppedSamples28Feb2.pkl",'rb') as f: 
    currStatesC, nextStatesC, actions, rewards, discretizedActions, discretizedActionSum = pickle.load(f)
    
feats= ['Age', 'Weight', 'Heart Rate', 'Respiratory Rate', '$SpO_2$', 
        'BP (Mean)', 'BP (Systolic)', 'BP (Diastolic)','$FiO_2$', 'PEEP set', 
        'Mean Airway Pressure', 'Ventilator Mode', 'Tidal Volume', 'Arterial pH', 'RR (Spont)', 
        'RASS','Peak Inspiratory Pressure', '$O_2$ Flow', 'Plateau Pressure','Arterial $O_2$ pressure', 
        'Arterial $CO_2$ pressure', 'Fentanyl (Conc)','Midazolam', 'Propofol', 'Fentanyl', 
        'Dexmedetomidine', 'Morphine', 'Hydromorphone', 'Lorazepam','Time on vent', 
        'Intubation number','Admit Type','Ethnicity','Male']

# Check if vent on based on timeIn != 0
voffIndices = np.where(np.transpose(currStatesC)[29] == 0)[0]
vonIndices = np.nonzero(np.transpose(currStatesC)[29])[0]
trainingSet = np.sort(np.concatenate((voffIndices[:65020], vonIndices[:259115]) , axis=0)) 
testSet = np.sort(np.concatenate((voffIndices[65020:], vonIndices[259115:]) , axis=0)) 

samples = np.hstack((currStatesC, discretizedActionSum))

testSamples = {}; testStates = {}; testActions = {}; testRewards = {}; testPt = {}
pt = 0; age = 0; wt = 0
for i in testSet:
    if ((samples[i][0] == age) and (samples[i][1] == wt)):
        testSamples[pt].append(samples[i])
        testStates[pt].append(currStatesC[i])
        testActions[pt].append(discretizedActionSum[i])
        testRewards[pt].append(rewards[i])
        testPt[pt].append(i)
    else:
        pt = pt + 1
        testSamples[pt] = [samples[i]]
        testStates[pt] = [currStatesC[i]]
        testActions[pt] = [discretizedActionSum[i]]
        testRewards[pt] = [rewards[i]]
        testPt[pt] = [i]
    age = samples[i][0]; wt = samples[i][1]

trainSamples = {}; trainStates = {}; trainActions = {}; trainRewards = {}; trainPt = {}
pt = 0; age = 0; wt = 0
for i in trainingSet:
    if ((samples[i][0] == age) and (samples[i][1] == wt)):
        trainSamples[pt].append(samples[i])
        trainStates[pt].append(currStatesC[i])
        trainActions[pt].append(discretizedActionSum[i])
        trainRewards[pt].append(rewards[i])
        trainPt[pt].append(i)
    else:
        pt = pt + 1
        trainSamples[pt] = [samples[i]]
        trainStates[pt] = [currStatesC[i]]
        trainActions[pt] = [discretizedActionSum[i]]
        trainRewards[pt] = [rewards[i]]
        trainPt[pt] = [i]
    age = samples[i][0]; wt = samples[i][1]
    
upper = [mean(np.transpose(currStatesC)[i]) + 4*std(np.transpose(currStatesC)[i]) for i in range(34)]
lower = [mean(np.transpose(currStatesC)[i]) - 3*std(np.transpose(currStatesC)[i]) for i in range(34)]

use = [1, 4, 6, 7, 9, 13, 14, 16, 18]
inds = unique(np.where([[(currStatesC[trainPt[k][0]][i] <= lower[i] or currStatesC[trainPt[k][0]][i] >= upper[i]) 
                         for i in use] for k in trainPt.keys()])[0])
inds2 = unique(np.where([[(currStatesC[testPt[k][0]][i] <= lower[i] or currStatesC[testPt[k][0]][i] >= upper[i]) 
                          for i in use] for k in testPt.keys()])[0])

a = (np.concatenate([trainPt[x+1] for x in inds]))
b = (np.concatenate([testPt[x+1] for x in inds2]))
c = (sort(np.append(a, b)))
mask = np.ones(len(currStatesC),dtype=bool) #np.ones_like(a,dtype=bool)
mask[c] = False
d = np.where(mask)[0]

fcurrStatesC = np.array([currStatesC[i] for i in d])
fnextStatesC = np.array([nextStatesC[i] for i in d])
fdiscretizedActionSum = np.array([discretizedActionSum[i] for i in d])
frewards = np.array([rewards[i] for i in d])
fdiscretizedActions = np.array([discretizedActions[i] for i in d])
factions = np.array([actions[i] for i in d])

with open("pickles/fpreppedSamples7Mar.pkl",'wb') as f:
    pickle.dump((fcurrStatesC, fnextStatesC, factions, frewards, fdiscretizedActions, fdiscretizedActionSum), f)

#currStatesC[264325], [trainPt[k][0] for k in trainPt.keys()][1806]
#len(currStatesC), len(filtered_cs)

for i in range(34):
    print i, feats[i], len(unique(np.transpose(fcurrStatesC)[i])), np.ptp(np.transpose(fcurrStatesC)[i])
    
sns.distplot(unique(np.transpose(currStatesC)[30]), 100)