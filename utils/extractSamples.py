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

with open("pickles/filteredFrames2.pkl",'rb') as f: filteredOutputFrames = pickle.load(f)
filteredHadms = filteredOutputFrames.keys()

vitals_list = ['Heart Rate', 'Respiratory Rate', 'O2 saturation pulseoxymetry', 'Non Invasive Blood Pressure mean',
               'Non Invasive Blood Pressure systolic', 'Non Invasive Blood Pressure diastolic', 'Inspired O2 Fraction',
               'PEEP set', 'Mean Airway Pressure','Ventilator Mode', 'Tidal Volume (observed)','PH (Arterial)',
               'Respiratory Rate (spontaneous)','Richmond-RAS Scale','Peak Insp. Pressure', 'O2 Flow',
               'Plateau Pressure','Arterial O2 pressure','Arterial CO2 Pressure'] 
                # 'Temperature Celsius','Total PEEP Level'

seds_list = ['Fentanyl (Concentrate)', 'Midazolam (Versed)', 'Propofol','Fentanyl', 'Dexmedetomidine (Precedex)', 
             'Morphine Sulfate','Hydromorphone (Dilaudid)', 'Lorazepam (Ativan)']

sbt_list = ['SBT Started', 'SBT Stopped', 'SBT Successfully Completed', 'SBT Deferred']

indices = [3,5,6,7,8,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38]

# regulateVitals[vital] = [min, low, high, max, scale] where min and max apply to extubation criteria
regulateVitals ={}
regulateVitals['Heart Rate'] = [np.nan, 60, 100, 130, 2] # 5
regulateVitals['Respiratory Rate'] = [np.nan, 12, 20, 30, 2] # 2
regulateVitals['Non Invasive Blood Pressure systolic'] = [np.nan, 90, 130, np.nan, 2] # 3
regulateVitals['Non Invasive Blood Pressure diastolic'] = [np.nan, 60, 85, np.nan, 2] # 3
regulateVitals['PH (Arterial)'] = [7.3, 7.35, 7.45, np.nan, 100] # 10
regulateVitals['Arterial O2 pressure'] = [np.nan, 45, 75, np.nan, 2] # 1
regulateVitals['Arterial CO2 Pressure'] = [np.nan, 30, 50, 60, 2] # 1
regulateVitals['O2 saturation pulseoxymetry'] = [88, 92, 100, 100, 10] # 10
regulateVitals['Inspired O2 Fraction'] = [0, 0, 70, 100, 10] # 5
regulateVitals['PEEP set'] = [np.nan, 4, 6, 8, 0] # 1
regulateVitals['Peak Insp. Pressure'] = [np.nan, 16, 20, np.nan, 0] # 1
regulateVitals['Richmond-RAS Scale'] = [-1, -5, 0, 1, 0] # 0
    
def setTimeIntnum(frame):
    frame.index = range(len(frame))
    frame['timeIn'] = frame.index
    frame['IntNum'] = 0
    span = 0
    for t in frame.index:
        if frame.Vented[t] == 1: span += 1
        else: span = 0
        frame.loc[t, 'timeIn'] = span
        if frame.Vented[t] == 1:
            if t == 0: 
                frame.loc[t, 'IntNum'] = 1
            elif frame.Vented[t-1] == 1:
                frame.loc[t, 'IntNum'] = frame.IntNum[t-1]
            elif frame.Vented[t-1] == 0:
                frame.loc[t, 'IntNum'] = frame.IntNum[t-1] + 1
        elif t != 0:
            frame.loc[t, 'IntNum'] = frame.IntNum[t-1]
    return frame    

def clusterEthnicities(ethList):
    simplified = []
    for i in ethList:
        if 'WHITE' in i: simplified.append('WHITE')
        elif 'BLACK' in i: simplified.append('BLACK')
        elif 'HISPANIC' in i: simplified.append('HISPANIC')
        elif 'ASIAN' in i: simplified.append('ASIAN')
        else: simplified.append('OTHER')
    return simplified

def cleanup(frame):
    if 'Height' in frame.columns: del frame['Height']
    frame.dropna(subset=["timestamp"], inplace=True)
    frame.SBT.fillna(value='No action', inplace=True)
    frame.fillna(method="ffill",inplace=True)
    frame.fillna(method="bfill",inplace=True) 
    frame.fillna(value=0, inplace=True)
    #frame.dropna(inplace=True)
    frame['Ethnicity'] = clusterEthnicities(frame['Ethnicity'])
    for v in vitals_list:
        frame = frame[np.abs(frame[v]-frame[v].mean())<=(5*frame[v].std())]
    return frame

#if ((len(outputFrames[i]) > 48) and (len(outputFrames[i]) < 720)):

def getVitReward(vit, val, regulateVitals):
    reward = 0
    lower = regulateVitals[vit][1]
    upper = regulateVitals[vit][2]
    scale = regulateVitals[vit][4]
    diff2 = float(upper-lower)/2
    if ((val > upper) or (val < lower)): 
        reward = -scale*(1/(1+math.exp(lower-val)) - 1/(1+math.exp(upper-val)) - (math.exp(diff2)-1)/(1+math.exp(diff2)))
    #print vit, 'Value:', val, ', Reward:', round(reward)
    return reward

def getSamples(outputFrames, i):
    
    samples = {}; currStates = []; actions = []; nextStates = []; rewards = []
    cleanOutput = cleanup(setTimeIntnum(outputFrames[i]))
        
    for t in range(len(cleanOutput) - 1):
        
        # Initialize states:
        currState = cleanOutput.iloc[[t]]
        nextState = cleanOutput.iloc[[t+1]]
        reward = 0
        
        # Initialize  actions:
        action = np.zeros(9)                                 # action[0] is ventOn/ventOff; action[1-8] are 7 sed dosages
        if (nextState.Vented.item() == 1):                      # if vent on in next, action was turn/keep vent on in curr
            action[0] = 1
        s = 1
        for sedative in seds_list:
            action[s] = currState[sedative]               # dosage of each sedative administered in currState
            s += 1
        
        # Reward from vent duration and reintubation:
        scaleFactor = currState.IntNum.item() * 5
        threshold = 6                                     # no penalty for first 6 hours
        if (currState.timeIn.item() >= threshold) and (nextState.Vented.item() == 1):
            reward -= scaleFactor/float(1 + math.exp(-0.05 * (currState.timeIn.item() - threshold)))
        
        if currState.IntNum.item() < nextState.IntNum.item():
            reward -= 5
        
        intubating = ((currState.Vented.item() == 0) and (action[0] == 1))
        stayingVented = ((currState.Vented.item() == 1) and (action[0] == 1))
        extubating = ((currState.Vented.item() == 1) and (action[0] == 0))
        stayingOff = ((currState.Vented.item() == 0) and (action[0] == 0))
        
        # Reward from vitals outside normal ranges (at any time)
        for vital in regulateVitals.keys():
            value = currState[vital].item()
            if (stayingVented or intubating): 
                reward += getVitReward(vital, value, regulateVitals)
            else:
                reward += 1.2*getVitReward(vital, value, regulateVitals)
            
        # Reward for sharp changes in key vitals:
        for vital in ['Heart Rate', 'Respiratory Rate', 'Non Invasive Blood Pressure systolic', 
                      'Non Invasive Blood Pressure diastolic']:
            currval = currState[vital].item()
            nextval = nextState[vital].item()
            change = abs(currval - nextval)/float(currval)
            if change > 0.2:
                if (stayingVented or intubating): reward -= change
                else: reward -= 1.2*change 
        
        # Reward from vitals outside min/max at extubation:
        if extubating:  
            reward += 5
        if (extubating or stayingOff):
            for vital in regulateVitals.keys():
                value = currState[vital].item()
                maxv = float(regulateVitals[vital][4])
                minv = float(regulateVitals[vital][0])
                if ((math.isnan(minv) != 1) and value < vital[0]):
                    reward -= 1
                if ((math.isnan(maxv) != 1) and value > vital[4]):
                    reward -= 1
                    
        # For staying off the vent:
        if (stayingOff and (currState.IntNum.item() != 0)):
            reward += 1
            
        # Bonus reward for successful extubation (with no reintubation within admission) -- cheating?
        if max(cleanOutput[t+1:].Vented) == 0:
            reward += 0
        
        #print '-------Total reward:', round(reward.item()), '-------'
        
        # Remove irrelevant entries
        for entry in ['timestamp', 'hadm', 'subject', 'adm_days', 'Vented', 'SBT']:
            del currState[entry], nextState[entry]
        currStates.append(currState.values)
        nextStates.append(nextState.values)
        actions.append(action)
        rewards.append(reward)
    
    samples[i] = {"hadm" : i, "currStates" : currStates, "nextStates" : nextStates, "actions": actions, "rewards": rewards}
    
    return samples[i]

def produceSamples(samples, h):
    try:
        samples[h] = getSamples(filteredOutputFrames, h)        
    except BaseException:
        samples[h] = [0]
    return samples[h]

def main():
    
    allSamples = {}
    produceSamples(allSamples, filteredHadms[0])
    allSamples = Parallel(n_jobs=16, verbose=50)(delayed(produceSamples)(allSamples, i) for i in filteredHadms)

    with open("pickles/allSamples28Feb.pkl",'wb') as f:
        pickle.dump(allSamples,f)
        
    #with open("pickles/allSamples17Feb.pkl",'rb') as f:
    #    allSamples = pickle.load(f)
    
    goodInds = []
    hadms = []
    for i in range(len(allSamples)):
        if len(allSamples[i]) > 1: 
            goodInds.append(i)
            hadms.append(allSamples[i]['hadm'])

    #for x in ['currStates', 'actions', 'rewards', 'nextStates']:
    #    l = [allSamples[i][x] for i in goodInds]
    #    vars()[x] = [item for sublist in l for item in sublist]
    
    currStates = [item for sublist in [allSamples[i]['currStates'] for i in goodInds] for item in sublist]
    actions = [item for sublist in [allSamples[i]['actions'] for i in goodInds] for item in sublist]
    rewards = [item for sublist in [allSamples[i]['rewards'] for i in goodInds] for item in sublist]
    nextStates = [item for sublist in [allSamples[i]['nextStates'] for i in goodInds] for item in sublist]
    
    tmp = (pd.get_dummies(pd.DataFrame(np.concatenate(currStates)), columns=[0, 1, 2]))
    for field in ['0_ELECTIVE', '0_URGENT', '1_ASIAN','1_BLACK','1_HISPANIC', '1_OTHER', '2_F']:
        del tmp[field]
    currStatesC = tmp.as_matrix()

    tmp = (pd.get_dummies(pd.DataFrame(np.concatenate(nextStates)), columns=[0, 1, 2]))
    for field in ['0_ELECTIVE', '0_URGENT', '1_ASIAN','1_BLACK','1_HISPANIC', '1_OTHER', '2_F']:
        del tmp[field]
    nextStatesC = tmp.as_matrix()
    
    #currStatesC =  = (pd.get_dummies(pd.DataFrame(np.concatenate(currStates)), columns=[0,1,2])).as_matrix()
    #nextStatesC = (pd.get_dummies(pd.DataFrame(np.concatenate(nextStates)), columns=[0,1,2])).as_matrix()
    
    a1 = (1*(np.logical_and(np.transpose(actions)[1]>0, np.transpose(actions)[1]<=2)) + 2*(np.transpose(actions)[1]>2))
    a2 = (1*(np.logical_and(np.transpose(actions)[2]>0, np.transpose(actions)[2]<=10)) + 2*(np.transpose(actions)[2]>10))
    a3 = (1*(np.logical_and(np.transpose(actions)[3]>0, np.transpose(actions)[3]<=200)) + 2*(np.transpose(actions)[3]>200))
    a4 = (1*(np.logical_and(np.transpose(actions)[4]>0, np.transpose(actions)[4]<=10)) + 2*(np.transpose(actions)[4]>10))
    a4 = (1*(np.logical_and(np.transpose(actions)[4]>0, np.transpose(actions)[4]<=25)) + 2*(np.transpose(actions)[4]>25))
    a5 = (1*(np.logical_and(np.transpose(actions)[5]>0, np.transpose(actions)[5]<=100)) + 2*(np.transpose(actions)[5]>100))
    a6 = (1*(np.logical_and(np.transpose(actions)[6]>0, np.transpose(actions)[6]<=2)) + 2*(np.transpose(actions)[6]>2))
    a7 = (1*(np.logical_and(np.transpose(actions)[7]>0, np.transpose(actions)[7]<=1)) + 2*(np.transpose(actions)[7]>1))
    a8 = (1*(np.logical_and(np.transpose(actions)[8]>0, np.transpose(actions)[8]<=1)) + 2*(np.transpose(actions)[8]>1))

    discretizedActions = np.transpose([np.transpose(actions)[0], a1, a2, a3, a4, a5, a6, a7, a8])
    discretizedActionSum = np.transpose([np.transpose(actions)[0], a1 + a2 + a3 + a4 + a5 + a6 + a7 + a8])
    tmp = np.transpose([np.transpose(discretizedActionSum)[0], np.ceil(np.transpose(discretizedActionSum)[1]/3)])
    discretizedActionSum = tmp
    # (np.vstack({tuple(row) for row in discretizedActionSum}))
    
    with open("pickles/preppedSamples19Mar.pkl",'wb') as f:
        pickle.dump((currStatesC, nextStatesC, actions, rewards, discretizedActions, discretizedActionSum), f)

if __name__ == '__main__':
    main()