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

with open("pickles/baseFrames.pkl",'rb') as f: extffn, sclv = pickle.load(f)
    
static = ['admittype', 'ethnicity', 'gender', 'age', 'weight']
vitals = ['Heart Rate', 'Respiratory Rate','O2 saturation pulseoxymetry', 'Non Invasive Blood Pressure mean', 
        'O2 Flow', 'Inspired O2 Fraction', 'Arterial CO2 Pressure', 'PH (Arterial)', 'Arterial O2 pressure', 
        'Mean Airway Pressure', 'Ventilator Mode', 'Peak Insp. Pressure', 'Plateau Pressure', 'Minute Volume', 
        'Tidal Volume (observed)', 'PEEP set', 'Creatinine', 'Hematocrit (serum)', 'BUN']
inputs = ['Fentanyl (Concentrate)', 'Midazolam (Versed)', 'Propofol','Fentanyl', 'Dexmedetomidine (Precedex)', 
        'Morphine Sulfate','Hydromorphone (Dilaudid)', 'Lorazepam (Ativan)']
extras = ['duration', 'intubations']
sbts = ['SBT Started', 'SBT Stopped', 'SBT Successfully Completed', 'SBT Deferred']

def transform(scl, val):
    return scl.transform(np.array([val]).reshape(-1,1))

regulateVitals ={}
v = 'Heart Rate'
regulateVitals[v] = [np.nan, transform(sclv[v], 60), transform(sclv[v], 100), transform(sclv[v], 130), 1] 
v = 'Respiratory Rate'
regulateVitals[v] = [np.nan, transform(sclv[v], 12), transform(sclv[v], 20), transform(sclv[v], 30), 1]
v = 'O2 saturation pulseoxymetry'
regulateVitals[v] = [transform(sclv[v], 88), transform(sclv[v],92), transform(sclv[v],100), transform(sclv[v],100), 1]
v = 'Non Invasive Blood Pressure mean'
regulateVitals[v] = [np.nan, transform(sclv[v], 70), transform(sclv[v], 100), np.nan, 1]
v = 'Inspired O2 Fraction'
regulateVitals[v] = [transform(sclv[v], 0), transform(sclv[v], 0), transform(sclv[v], 70), transform(sclv[v], 100), 1]
v = 'Arterial CO2 Pressure'
regulateVitals[v] = [np.nan, transform(sclv[v], 30), transform(sclv[v], 50), transform(sclv[v], 60), 1]
v = 'PH (Arterial)'
regulateVitals[v] = [transform(sclv[v], 7.3), transform(sclv[v], 7.35), transform(sclv[v], 7.45), np.nan, 1]
v = 'Arterial O2 pressure'
regulateVitals[v] = [np.nan, transform(sclv[v], 45), transform(sclv[v], 75), np.nan, 1]
v = 'Peak Insp. Pressure'
regulateVitals[v] = [np.nan, transform(sclv[v], 16), transform(sclv[v], 20), np.nan, 1]
v = 'PEEP set'
regulateVitals[v] = [np.nan, transform(sclv[v], 4), transform(sclv[v], 6), transform(sclv[v], 8), 1] 

def getVitReward(vit, val, regulateVitals):
    reward = 0
    lower = regulateVitals[vit][1]
    upper = regulateVitals[vit][2]
    mid = (upper - lower)/2.0
    importance = regulateVitals[vit][4]
    diff2 = float(upper-lower)/2
    scale = 1/((math.exp(diff2)-1)/(1+math.exp(diff2)))
    if (val > upper) or (val < (lower)): 
                reward = -0.125*importance*scale*(1/(1+math.exp(lower-val)) - 1/(1+math.exp(upper-val)) - (1/scale))
    return reward
            
def getSamples(output, h):
    
    frame = cp.deepcopy(extffn)
    currStates = []; actions = []; nextStates = []; rewards = []
        
    for t in range(len(frame[h]) - 1):
        
        # Initialize states:
        currState = frame[h].iloc[[t]]
        nextState = frame[h].iloc[[t+1]]
        reward = 0
        # Initialize actions:
        action = np.zeros(10)                                 # action[0] is ventOn/ventOff; action[1-8] are 7 sed dosages
        if (nextState.ventilated.item() == 1):                      # if vent on in next, action was turn/keep vent on in curr
            action[0] = 1
        s = 1
        for sedative in inputs:
            if sedative != 'Propofol':
                if (currState[sedative].item() < nextState[sedative].item()):
                    action[s] = 1
                elif (currState[sedative].item() > nextState[sedative].item()):
                    action[s] = -1
                else:
                    action[s] = 0
            else:
                if (currState[sedative].item() == 0) and (nextState[sedative].item() > 0):
                    action[s] = 0.5
                elif (currState[sedative].item() > 0) and (nextState[sedative].item() == 0):
                    action[s] = -0.5
                else:
                    action[s] = 0
#             if ((t==0) and (currState[sedative].item() != 0)):
#                 action[s] = 1
            s += 1
        action[s] = sum(action[1:9])
        
        # Penalize vent duration: no penalty for first 6 hours ### MAX = -1 ###
        scaleFactor = 0.5*currState.intubations.item()
        threshold = 6                                     
        if (currState.duration.item() >= threshold) and (nextState.ventilated.item() == 1):
            reward -= scaleFactor*float(1 - math.exp(-0.005 * (currState.duration.item() - threshold)))
            
        intubating = ((currState.ventilated.item() == 0) and (action[0] == 1))
        stayingVented = ((currState.ventilated.item() == 1) and (action[0] == 1))
        extubating = ((currState.ventilated.item() == 1) and (action[0] == 0))
        stayingOff = ((currState.ventilated.item() == 0) and (action[0] == 0))
        
        # Reward from vitals outside normal ranges (at any time) ### MAX = -1.2 ###
        for vital in regulateVitals.keys():
            value = currState[vital].item()
            scaleFactor = 1.0
            if (stayingVented or intubating): 
                reward -= scaleFactor * getVitReward(vital, value, regulateVitals)
            else:
                reward -= scaleFactor * 1.2*getVitReward(vital, value, regulateVitals)
            
        # Reward for sharp changes in key vitals ### MAX = -1.2 ###
        for vital in ['Heart Rate', 'Respiratory Rate', 'Non Invasive Blood Pressure mean']:
            currval = currState[vital].item()
            nextval = nextState[vital].item()
            #print vital, currval
            if currval == 0: currval = nextval # buggy
            if nextval != 0:
                change = abs(currval - nextval)/float(currval)
            else: change = 0
            scaleFactor = 0.25
            if change > 0.2:
                if (stayingVented or intubating): 
                    reward -= min(1, scaleFactor * change)
                else: 
                    reward -= min(1.2, 1.2 * scaleFactor * change)
        
        # Reward from vitals outside min/max at extubation: ### MIN = 2, MAX < -1.6 ###
        if extubating:
            if max(frame[h][t+1:].ventilated) == 0:
                reward += 2
            else:
                reward -= 1.5
        if (extubating or stayingOff):
            for vital in regulateVitals.keys():
                value = currState[vital].item()
                maxv = float(regulateVitals[vital][4])
                minv = float(regulateVitals[vital][0])
                if ((math.isnan(minv) != 1) and value < vital[0]):
                    reward -= 0.1
                if ((math.isnan(maxv) != 1) and value > vital[4]):
                    reward -= 0.1
                    
        # For staying off the vent:
        if (stayingOff and (currState.intubations.item() >= 1)): ### MIN = 1 ###
            reward += 0.5
        # Penalize reintubation: ### MAX = -2 ###
        if (intubating and currState.intubations.item() > 1):
            reward -= 0.5
        # Bonus reward for successful extubation (with no reintubation within admission) -- cheating?
        if max(frame[h][t+1:].ventilated) == 0:
            reward += 0
        
        #print '-------Total reward:', round(reward.item()), '-------'
        
        # Remove irrelevant entries
        for entry in ['timestamp', 'hadm', 'subject', 'icudays', 'ventilated', 'sbt']:
            del currState[entry], nextState[entry]
        currStates.append(currState.values)
        nextStates.append(nextState.values)
        actions.append(action)
        rewards.append(reward)
    
    output[h] = {"hadm" : h, "currStates" : currStates, "nextStates" : nextStates, "actions": actions, "rewards": rewards}
    #print output[h]
    return output[h]

def produceSamples(samples, h):
    #try:
    samples[h] = getSamples(extffn, h)        
    #except BaseException:
     #   samples[h] = [0]
    #return samples[h]

def main():
    
    allSamples = {}
    
    #for h in extffn.keys():
    #    allSamples[h] = getSamples(extffn, h)
    #    print allSamples[h]['hadm']
    #print allSamples[extffn.keys()[0]]
    
    allSamples = Parallel(n_jobs=16, verbose=100)(delayed(getSamples)(allSamples, i) for i in extffn.keys())
    #print allSamples[extffn.keys()[0]]
    #print allSamples
    
    goodInds = []
    hadms = []
    for i in range(len(allSamples)):
        if len(allSamples[i]) > 1: 
            goodInds.append(i)
            hadms.append(allSamples[i]['hadm'])
    
    hadmsList = np.concatenate([np.ones(len(allSamples[i]['rewards'])) * allSamples[i]['hadm'] for i in goodInds])
    currStates = [item for sublist in [allSamples[i]['currStates'] for i in goodInds] for item in sublist]
    actions = [item for sublist in [allSamples[i]['actions'] for i in goodInds] for item in sublist]
    rewards = [item for sublist in [allSamples[i]['rewards'] for i in goodInds] for item in sublist]
    nextStates = [item for sublist in [allSamples[i]['nextStates'] for i in goodInds] for item in sublist]
    
    with open("pickles/preppedBaseFrames2.pkl",'wb') as f:
        pickle.dump((allSamples, hadmsList, currStates, nextStates, actions, rewards), f)

if __name__ == '__main__':
    main()