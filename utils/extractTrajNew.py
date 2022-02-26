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
import math
from datetime import date, datetime, timedelta
from sklearn import svm, preprocessing
from collections import Counter

ventd_lenfiltered = pd.read_pickle("ventd_lenfiltered.pkl")
vitd_lenfiltered = pd.read_pickle("vitd_lenfiltered.pkl")
sbtd_lenfiltered = pd.read_pickle("sbtd_lenfiltered.pkl")
inputsd_lenfiltered = pd.read_pickle("inputsd_lenfiltered.pkl")
    
vitals_list = ['Heart Rate', 'Respiratory Rate','O2 saturation pulseoxymetry', 'Non Invasive Blood Pressure mean', 
               'O2 Flow', 'Inspired O2 Fraction', 'Arterial CO2 Pressure', 'PH (Arterial)', 'Arterial O2 pressure', 
               'Mean Airway Pressure', 'Ventilator Mode', 'Peak Insp. Pressure', 'Plateau Pressure', 'Minute Volume', 
               'Tidal Volume (observed)', 'PEEP set', 'Creatinine', 'Hematocrit (serum)', 'BUN', 
               'Admission Weight (Kg)']

inputs_list = ['Fentanyl (Concentrate)', 'Midazolam (Versed)', 'Propofol','Fentanyl', 'Dexmedetomidine (Precedex)', 
               'Morphine Sulfate','Hydromorphone (Dilaudid)', 'Lorazepam (Ativan)']

sbt_list = ['SBT Started', 'SBT Stopped', 'SBT Successfully Completed', 'SBT Deferred']

hadms = ventd_lenfiltered.hadm.unique()

def buildTimeFrame(start, end, delta):
    times = []
    curr = start
    while curr < end:
        times.append(curr)
        curr += delta
    timeFrame = DataFrame(data={'timestamp': times}, index=times)
    return timeFrame
        
def getChartFrame(h, ventTable, sedTable, vitTable, sbtTable, vitalsList, sedationList, sbtList):    

    ### TIMEFRAME ###
    admitTime =  pd.to_datetime(ventTable[ventTable.hadm == h].admit_time.unique().item())
    admitTime -= dt.timedelta(minutes=admitTime.minute, seconds=admitTime.second, microseconds=admitTime.microsecond)
    dischTime = pd.to_datetime(ventTable[ventTable.hadm == h].discharge_time.unique().item())
    dischTime -= dt.timedelta(minutes=dischTime.minute, seconds=dischTime.second, microseconds=dischTime.microsecond)
    dischTime += dt.timedelta(hours = 1)
    chartFrame =  buildTimeFrame(admitTime, dischTime, timedelta(hours=1))
    
    ### IDENTIFIERS ###
    chartFrame['hadm'] = h
    chartFrame['subject'] = ventTable[ventTable.hadm == h].subject.head(1).item()
    
    #### STATIC VARIABLES ####
    chartFrame['icudays'] = (dischTime - admitTime).days 
    chartFrame['admittype'] = int(ventTable[ventTable.hadm == h].admittype.head(1).str.contains('EMERGENCY').item())   
    chartFrame['ethnicity'] = int(ventTable[ventTable.hadm == h].subj_ethnicity.head(1).str.contains('WHITE').item())
    chartFrame['gender'] = int(ventTable[ventTable.hadm == h].gender.head(1).str.contains('F').item())
    chartFrame['age'] = (ventTable[ventTable.hadm == h].admit_time.head(1).dt.year - 
                       ventTable[ventTable.hadm == h].dateofbirth.head(1).dt.year).item() % 210 # for censored >90s
    chartFrame['weight'] = vitTable[(vitTable.hadm == h) & 
                                    (vitTable.label == 'Admission Weight (Kg)')].head(1).value.item()
    
    ### VENT STATUS ###
    chartFrame['ventilated'] = 0
    for i,row in ventTable[ventTable.hadm == h].iterrows():
        ventStart = pd.to_datetime(row.vent_starttime)
        ventStart -= dt.timedelta(minutes=ventStart.minute, seconds=ventStart.second, microseconds=ventStart.microsecond)
        ventStart += dt.timedelta(hours = 1)
        ventEnd = pd.to_datetime(row.vent_endtime)
        ventEnd -= dt.timedelta(minutes=ventEnd.minute, seconds=ventEnd.second, microseconds=ventEnd.microsecond)
        for t in chartFrame.timestamp:
            if (pd.to_datetime(t) >= ventStart) and (pd.to_datetime(t) <= ventEnd): 
                chartFrame.loc[chartFrame.timestamp == t,'ventilated'] = 1

    ### SBT TIMES ###
    chartFrame['sbt'] = np.nan
    if len(sbtTable[(sbtTable.hadm == h)]) != 0:
        for v in sbtList:
            sbt_t = sbtTable[(sbtTable.hadm == h) & (sbtTable.label == v)].sort_values(by='charttime')
            sbt_t.set_index('charttime',inplace=True,drop=False)
            sbt_t = sbt_t.resample('1h').mean()
            sbt_t = sbt_t[sbt_t['subject'].notnull()]
            sbt_t['timestamp'] = sbt_t.index
            for t in chartFrame.timestamp:
                if sbt_t[sbt_t.timestamp == t].empty == False:
                    chartFrame.loc[chartFrame.timestamp == t,'sbt'] = v # overwrites if multiple things happen
                        
    ### DYNAMIC VARIABLES ###
    for v in vitalsList[:19]:
        chartFrame[v] = np.nan
        vitals_v = vitTable[(vitTable.hadm == h) & (vitTable.label == v)].sort_values(by='charttime')
        vitals_v.set_index('charttime',inplace=True,drop=False)
        vitals_v = vitals_v.resample('1h').mean().fillna(method="ffill")    
        vitals_v['timestamp'] = vitals_v.index
        for t in chartFrame.timestamp:
            if vitals_v[vitals_v.timestamp == t].empty == False:
                chartFrame.loc[chartFrame.timestamp == t,v] = vitals_v[vitals_v.timestamp == t].value.item()

    ### SEDATIVES ###    
    sedValue = {}
    for s in sedationList:
        chartFrame[s] = 0
        for t in chartFrame.timestamp: sedValue[s,t] = 0
        for i,row in sedTable[(sedTable.hadm==h) & (sedTable.label==s)].iterrows():
            if not row.empty:
                sedStart = pd.to_datetime(row.input_start)
                sedEnd = pd.to_datetime(row.input_end)
                sedDur = (sedEnd - sedStart).seconds/3600.0
                nextTS = pd.to_datetime(t) + dt.timedelta(hours = 1)
                #print pd.to_datetime(t), sedStart, sedEnd
                for t in chartFrame.timestamp:  
                    
                    if (pd.to_datetime(t) <= sedStart) and (pd.to_datetime(t) <= sedEnd): 
                        if (pd.to_datetime(t) + dt.timedelta(hours = 1) > sedStart): #sed starts, finishes after t, before t+1
                            if row.ordercat == 'Continuous Med': 
                                if (sedEnd < nextTS):
                                    sedValue[s,t] += float(row.amount)/sedDur
                                else: 
                                    sedValue[s,t] += float(row.amount)/sedDur*((nextTS - sedStart).seconds/3600.0) 
                        elif row.ordercat == 'Drug Push':
                            sedValue[s,t] += float(row.amount)
                            
                    if (pd.to_datetime(t) >= sedStart) and (pd.to_datetime(t) <= sedEnd): #sed starts before, finishes after t
                        if row.ordercat == 'Continuous Med': 
                            if (pd.to_datetime(t) + dt.timedelta(hours = 1) > sedEnd):
                                sedValue[s,t] += float(row.amount)/sedDur*((sedEnd - pd.to_datetime(t)).seconds/3600.0) 
                            else: 
                                sedValue[s,t] += float(row.amount)/sedDur*((nextTS - pd.to_datetime(t)).seconds/3600.0) 
                        
                    chartFrame.loc[chartFrame.timestamp == t,s] = int(round(sedValue[s,t]))
                        
        
    tmp = chartFrame[chartFrame['Ventilator Mode'].notnull()].head(4)
    chartFrame = tmp.append(chartFrame.loc[chartFrame.loc[chartFrame['Ventilator Mode'].notnull()].index 
                                           + timedelta(hours=4)])
    return chartFrame 



def produceFrames(output, h):
    try:
        output[h] = getChartFrame(h, ventd_lenfiltered, inputsd_lenfiltered, vitd_lenfiltered, sbtd_lenfiltered,
                                       vitals_list, inputs_list, sbt_list)
    except BaseException:
        output[h] = [0]
    return output[h]

def main():
    
    outputFrames = {}
    outputFrames = Parallel(n_jobs=12, verbose=50)(delayed(produceFrames)(outputFrames, h) for h in hadms)

    filteredOutputFrames = {}
    for i in range(len(outputFrames)):
        if len(outputFrames[i])>=24:
            hadm = outputFrames[i].hadm.head(1).item()
            filteredOutputFrames[hadm] = outputFrames[i]

    with open("filteredFramesNew2.pkl",'wb') as f:
        pickle.dump(filteredOutputFrames,f)
        
    extffn = {}
    for h in filteredFramesNew.keys(): 
        if (filteredFramesNew[h].ventilated.tail(1).item() == 0):
            extffn[h] = filteredFramesNew[h]

    maxv={}; minv={}; meanv = {}
    for v in vitals_list[:19]:
        all = np.concatenate([extffn[h][v] for h in extffn.keys()])
        print v, (nanmean(all)), nanmedian(all), (nanmean(all)-10*nanstd(all)), (nanmean(all)+10*nanstd(all))
        maxv[v] = (nanmean(all)+10*nanstd(all))
        minv[v] = (nanmean(all)-10*nanstd(all))
        meanv[v] = (nanmean(all))

    for h in extffn.keys():
        for v in vitals_list[:19]:
            extffn[h].ix[extffn[h][v] > maxv[v], v] = float('NaN')
            extffn[h].ix[extffn[h][v] < minv[v], v] = float('NaN')

    maxv={}; minv={}; meanv = {}
    for v in vitals_list[:19]:
        all = np.concatenate([extffn[h][v] for h in extffn.keys()])
        print v, (nanmean(all)), nanmedian(all), (nanmean(all)-3*nanstd(all)), (nanmean(all)+3*nanstd(all))
        maxv[v] = (nanmean(all)+3*nanstd(all))
        minv[v] = (nanmean(all)-3*nanstd(all))
        meanv[v] = (nanmean(all))

    for h in extffn.keys():
        for v in vitals_list[:19]:
            extffn[h].ix[extffn[h][v] > maxv[v], v] = float('NaN')
            extffn[h].ix[extffn[h][v] < minv[v], v] = float('NaN')

    def cleanup(frame):
        frame.dropna(subset=["timestamp"], inplace=True)
        frame.sbt.fillna(value='unknown', inplace=True)
        frame.fillna(method="ffill",inplace=True)
        frame.fillna(method="bfill",inplace=True) 
        for v in vitals_list[:19]:
            frame[v].fillna(value=meanv[v], inplace=True)
        return frame

    for h in extffn.keys():
        cleanup(extffn[h])
        
    with open("extffn_rr.pkl",'wb') as f:
        pickle.dump(extffn,f)    

if __name__ == '__main__':
    main()