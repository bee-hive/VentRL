import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import datetime as dt
import os, sys, pickle, json, time, math, re
from joblib import Parallel, delayed
import argparse
import mimicConcepts as mc

############################################################################################################## 
###                                 Extract cohort from database                                           ###
##############################################################################################################

def getParamLists():
    
    vitals_list = ['Heart Rate', 'Respiratory Rate', 'O2 saturation pulseoxymetry', 'Non Invasive Blood Pressure mean',
                   'Non Invasive Blood Pressure systolic', 'Non Invasive Blood Pressure diastolic','Inspired O2 Fraction',
                   'PEEP set', 'Mean Airway Pressure','Ventilator Mode', 'Tidal Volume (observed)','PH (Arterial)',
                   'Respiratory Rate (spontaneous)','Richmond-RAS Scale','Peak Insp. Pressure', 'O2 Flow',
                   'Plateau Pressure','Arterial O2 pressure','Arterial CO2 Pressure']

    sedatives_list = ['Propofol', 'Fentanyl (Concentrate)', 'Midazolam (Versed)', 'Fentanyl', 
                      'Dexmedetomidine (Precedex)', 'Morphine Sulfate', 'Hydromorphone (Dilaudid)', 'Lorazepam (Ativan)']

    sbt_list = ['SBT Started', 'SBT Stopped', 'SBT Successfully Completed', 'SBT Deferred']

    misc_list = ['Admission Weight (Kg)', 'Height (cm)']

    return vitals_list, sedatives_list, sbt_list, misc_list


def generateTables(save=False, savepath='../processed_data/allTables.pkl'):
    
    # Define covariates of interest
    vitals_list, sedatives_list, sbt_list, misc_list = getParamLists()
    
    # Extract tables from database
    vent_table = mc.ventilation()
    adms_table = mc.admissions()
    adms_table = adms_table[adms_table.hadm.isin(vent_table.hadm.unique()) & (adms_table.h_exp==0)]
    cohort_hadms = adms_table.hadm.unique()
    cohort_stays = adms_table.icustay.unique()
    print '# Unique icu stays:', len(cohort_stays), '| # Unique hadms:', len(cohort_hadms)
    
    vent_table = vent_table[vent_table.hadm.isin(cohort_hadms)]
    vitals_table = mc.charts(cohort_hadms, vitals_list)
    seds_table = mc.inputs(cohort_hadms, sedatives_list)
    sbt_table = mc.charts(cohort_hadms, sbt_list)
    misc_table = mc.charts(cohort_hadms, misc_list)
    
    # Quality control on measurements recorded in charts
    vstats_qc, vitals_qc = qualityControl(vitals_table, ref_cov_list=vitals_list, fig_dir='../processed_data/qc/', plot=False, 
                            savepath='../processed_data/qc_vitals.pkl')
    
    mstats_qc, misc_qc = qualityControl(misc_table, ref_cov_list=misc_list, fig_dir='../processed_data/qc/', plot=False, 
                            savepath='../processed_data/qc_misc.pkl')
    
    stats = pd.concat((vstats_qc, mstats_qc)).reset_index(drop=True)
    measures_table = pd.concat((vitals_qc, misc_qc)).reset_index(drop=True)
    
    if save:
        pickle.dump((adms_table, vent_table, measures_table, seds_table, sbt_table), open(savepath, 'wb'))
    return adms_table, vent_table, measures_table, seds_table, sbt_table
 
    
def loadTables(filepath='../processed_data/allTables.pkl'):
    
    adms_table, vent_table, measures_table, seds_table, sbt_table = pickle.load(open(filepath, 'rb'))
    
    return adms_table, vent_table, measures_table, seds_table, sbt_table


def qualityControl(raw_df, time_col='charttime', name_col='label', val_col='value', ref_cov_list=None, 
                 fig_dir='.', fig_format='pdf', plot=False, savepath=None):
    # Filter for NaNs, any vitals measurements more than 3 standard deviations from the population mean.
    
    if ref_cov_list is None:
        ref_cov_list = raw_df[name_col].unique()
    cov_stat_df = pd.DataFrame(columns=['covariate', 'raw_mean', 'raw_std', 'raw_min', 'raw_max', 
                                        'qc_mean', 'qc_std', 'qc_min', 'qc_max'])
    sub_data_df = pd.DataFrame(columns=raw_df.columns)
    
    for cov_name, cov_df in raw_df.groupby(name_col):
        print(cov_name)
        
        if(cov_name in ref_cov_list):
            raw_cov_val = cov_df[val_col].values
            cov_mean = np.nanmean(raw_cov_val)
            cov_std = np.nanstd(raw_cov_val)
            
            qc_cov_df = cov_df[~np.isnan(cov_df[val_col])]
            qc_cov_df = cov_df[cov_df[val_col] > 0]
            qc_cov_df = cov_df[cov_df[val_col] <= 999.0]
            qc_cov_df = qc_cov_df[cov_df[val_col] >= (cov_mean-3*cov_std)]
            qc_cov_df = qc_cov_df[qc_cov_df[val_col] <= (cov_mean+3*cov_std)]
            qc_cov_val = qc_cov_df[val_col].values
            cov_stat_df = cov_stat_df.append({'covariate': cov_name,
                                              'raw_mean': np.nanmean(raw_cov_val), 
                                              'raw_std': np.nanstd(raw_cov_val), 
                                              'raw_min': np.nanmin(raw_cov_val), 
                                              'raw_max': np.nanmax(raw_cov_val),
                                              'qc_mean': np.nanmean(qc_cov_val), 
                                              'qc_std': np.nanstd(qc_cov_val), 
                                              'qc_min': np.nanmin(qc_cov_val), 
                                              'qc_max': np.nanmax(qc_cov_val)
                                             }, ignore_index=True)
            if plot:
                plt.figure(figsize=(12, 6))
                plt.subplot(1, 2, 1)
                sns.distplot(raw_cov_val[~np.isnan(raw_cov_val)])
                plt.title(cov_name + ': before qc')

                plt.subplot(1, 2, 2)
                sns.distplot(qc_cov_val)
                plt.title(cov_name + ': after qc')
                plt.savefig(os.path.join(fig_dir, 'qc_hist_{}.{}'.format(cov_name, fig_format)))
            
            sub_data_df = sub_data_df.append(qc_cov_df)
                
            if (savepath!=None):
                 pickle.dump((cov_stat_df, sub_data_df), open(savepath, 'wb'))
    return cov_stat_df, sub_data_df

############################################################################################################## 
###                                   Build admission timeframes                                           ###
##############################################################################################################

def buildTimeFrame(table_h, delta):
    # Get admit and discharge time in numeric form, round down/up respectively to the nearest hour:
    
    start =  pd.to_datetime(table_h.admit_time.unique().item())
    start -= dt.timedelta(minutes=start.minute, seconds=start.second, microseconds=start.microsecond)
    end = pd.to_datetime(table_h.discharge_time.unique().item())
    end -= dt.timedelta(minutes=end.minute, seconds=end.second, microseconds=end.microsecond)
    end += dt.timedelta(hours=1)
    
    times = []
    curr = start
    while curr < end:
        times.append(curr)
        curr += delta
    timeFrame = pd.DataFrame(data={'timestamp': times}, index=times)
    return timeFrame


def getChartFrame(h, adms_table, vent_table, measures_table, seds_table, sbt_table):    
    # Generate admission dataframe

    vits_list, seds_list, sbt_list, misc_list = getParamLists()
    adms_table = adms_table[adms_table.hadm == h]
    chartFrame =  buildTimeFrame(adms_table, dt.timedelta(hours=1))
    chartFrame['hadm'] = h
    chartFrame['firstICU'] = adms_table.icustay.head(1).item()
    chartFrame['subject'] = adms_table.subject.head(1).item()
    chartFrame['Admittype'] = int(adms_table.head(1).admittype.item()=='EMERGENCY') # 0 - elective/urgent; 1 - emergency
    chartFrame['Admdays'] = adms_table.adm_los.head(1).item()
    chartFrame['Ethnicity'] = int('WHITE' not in adms_table.head(1).ethnicity.item()) # 0 - white; 1 - non-white 
    chartFrame['Gender'] = int(adms_table.head(1).gender.item()=='F') # 0 - male, 1 - female
    chartFrame['Age'] = adms_table.head(1).age.item()
    
    measures_table = measures_table[measures_table.hadm == h]
    for v in (misc_list + vits_list):
        chartFrame[v] = np.nan
        vitals_v = measures_table[(measures_table.label == v)].sort_values(by='charttime')
        vitals_v.set_index('charttime',inplace=True,drop=False)
        vitals_v = vitals_v.resample('1h').mean().fillna(method="ffill")    
        vitals_v['timestamp'] = vitals_v.index
        for t in chartFrame.timestamp:
            if vitals_v[vitals_v.timestamp == t].empty == False:
                chartFrame.loc[chartFrame.timestamp == t,v] = vitals_v[vitals_v.timestamp == t].value.item() 
                
    seds_table = seds_table[seds_table.hadm == h]
    sedValue = {}
    for s in seds_list:
        chartFrame[s] = 0
        for t in chartFrame.timestamp: sedValue[t] = 0
        for i,row in seds_table[(seds_table.label == s)].iterrows():
            if not row.empty:
                sedStart = (row.input_start).to_pydatetime()
                sedEnd = (row.input_end).to_pydatetime()
                sedDur = (sedEnd - sedStart).seconds/3600.0
                nextTS = t.to_pydatetime() + dt.timedelta(hours = 1)

                for t in chartFrame.timestamp:
                    if bool(re.match('05|06', row.ordercat)): 
                        if ((sedStart >= t.to_pydatetime()) and (sedEnd <= t.to_pydatetime() + dt.timedelta(hours = 1))):
                                sedValue[t] += float(row.amount)

                    elif bool(re.match('01|02', row.ordercat)):
                        if (t.to_pydatetime() <= sedStart) and (t.to_pydatetime() <= sedEnd): 
                            if (t.to_pydatetime() + dt.timedelta(hours = 1) >= sedStart):
                                if (sedEnd <= nextTS):
                                    sedValue[t] += float(row.amount)/sedDur
                                else: 
                                    sedValue[t] += float(row.amount)/sedDur*((nextTS - sedStart).seconds/3600.0)                                                                         
                        elif (t.to_pydatetime() >= sedStart) and (t.to_pydatetime() <= sedEnd):
                            if (t.to_pydatetime() + dt.timedelta(hours = 1) > sedEnd):
                                sedValue[t] += float(row.amount)/sedDur*((sedEnd - t.to_pydatetime()).seconds/3600.0) 
                            else: 
                                sedValue[t] += float(row.amount)/sedDur*((nextTS - t.to_pydatetime()).seconds/3600.0) 

                    chartFrame.loc[chartFrame.timestamp == t,s] = round(sedValue[t], 2)
                                
    vent_table = vent_table[vent_table.hadm == h]
    chartFrame['Vented'] = 0
    for i,row in vent_table.iterrows():
        ventStart = row.vent_starttime.to_pydatetime()
        ventStart -= dt.timedelta(minutes=ventStart.minute, seconds=ventStart.second, microseconds=ventStart.microsecond)
        ventStart += dt.timedelta(hours=1)
        ventEnd = row.vent_endtime.to_pydatetime()
        ventEnd -= dt.timedelta(minutes=ventEnd.minute, seconds=ventEnd.second, microseconds=ventEnd.microsecond)
        for t in chartFrame.timestamp:
            if (t.to_pydatetime() >= ventStart) and (t.to_pydatetime() <= ventEnd): 
                chartFrame.loc[chartFrame.timestamp == t,'Vented'] = 1
                
    sbt_table = sbt_table[sbt_table.hadm == h]
    chartFrame['SBT'] = 'None'
    for v in sbt_list:
        sbt_t = sbt_table[(sbt_table.label == v)].sort_values(by='charttime')
        sbt_t.set_index('charttime',inplace=True,drop=False)
        sbt_t = sbt_t.resample('1h').mean()
        sbt_t = sbt_t[sbt_t['subject'].notnull()]
        sbt_t['timestamp'] = sbt_t.index
        for t in chartFrame.timestamp:
            if sbt_t[sbt_t.timestamp == t].empty == False:
                chartFrame.loc[chartFrame.timestamp == t,'SBT'] = v # overwrites if multiple things happen
    
    tmp = chartFrame[chartFrame['Ventilator Mode'].notnull()].head(4)
    chartFrame = tmp.append(chartFrame.loc[chartFrame.loc[chartFrame['Ventilator Mode'].notnull()].index +
                                           dt.timedelta(hours=4)])
    
    chartFrame = chartFrame.reset_index(drop=True)
    chartFrame = chartFrame.fillna(method='ffill').fillna(method='bfill')
    chartFrame = chartFrame[~np.isnat(chartFrame.timestamp)]   
            
    return chartFrame
    
def produceFrames(output, h, adms_df, vent_df, measures_df, seds_df, sbt_df):
    try:
        output[h] = getChartFrame(h, adms_df, vent_df, measures_df, seds_df, sbt_df)
    except BaseException:
        output[h] = [0]
    return output[h]
  
def main(hadms=None):

    tables='../processed_data/allTables.pkl'
    adms_df, vent_df, measures_df, seds_df, sbt_df = pickle.load(open(tables, 'rb'))
        
    if hadms==None:
        hadms = (list(set(adms_df.hadm.unique()) & set(measures_df.hadm.unique()) & set(seds_df.hadm.unique())))
        outputname = "../processed_data/processedFrames.pkl" 
    else:
        outputname = "../processed_data/h_frames/set-" + str(hadms[0]) +".pkl"  
    
    outputFrames = {}
    outputFrames = Parallel(n_jobs=10, verbose=50)(delayed(produceFrames)(outputFrames, h, adms_df[adms_df.hadm == h],
                                                                          vent_df[vent_df.hadm == h],
                                                                          measures_df[measures_df.hadm == h],
                                                                          seds_df[seds_df.hadm == h], 
                                                                          sbt_df[sbt_df.hadm == h]) for h in hadms)
    filteredOutputFrames = {}
    for i in range(len(outputFrames)):
        if (len(outputFrames[i]) > 1):
            hadm = outputFrames[i].hadm.head(1).item()
            filteredOutputFrames[hadm] = outputFrames[i]
            
    pickle.dump(filteredOutputFrames, open(outputname,'wb'))   

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--h', type=str, default=None, help='hadm IDs')
    args = parser.parse_args()
    
    main(hadms=[int(i) for i in open(args.h, 'r').read().split()])