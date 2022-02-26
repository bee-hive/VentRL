import pandas as pd
import psycopg2

# create a database connection
sqluser = 'bee_mimic_admin'
dbname = 'bee_mimic'
schema_name = 'mimiciii'
sqlpwd = "2OKF0dr@czOrD0suS4GyN"

# Connect to local postgres version of mimic
con = psycopg2.connect(dbname=dbname, user=sqluser, password=sqlpwd)
cur = con.cursor()
cur.execute('SET search_path to ' + schema_name)

# ventilation + admission info

vent_id = 225792
query = """
SELECT ad.subject_id as subject
  , ad.hadm_id as hadm
  , ad.admission_type as admittype  
  , ad.ethnicity as subj_ethnicity
  , pa.gender
  , pa.dob as dateofbirth
  , ad.diagnosis
  , ad.admittime as admit_time
  , ad.dischtime as discharge_time
  , ad.deathtime as death_time
  , pr.starttime as vent_starttime
  , pr.endtime as vent_endtime
  , ad.hospital_expire_flag as h_exp
  , pa.expire_flag as exp
FROM admissions ad
INNER JOIN patients pa
ON ad.subject_id = pa.subject_id
INNER JOIN procedureevents_mv pr
ON ad.hadm_id = pr.hadm_id
WHERE pr.itemid = """ + str(vent_id) + """
ORDER BY ad.subject_id, pr.starttime
"""

ventilation = pd.read_sql_query(query,con)

# table of inputevents_mv times + doses

cur.execute('SET search_path to ' + schema_name)

query = """
SELECT ad.subject_id as subject
  , ad.hadm_id as hadm
  , mv.starttime as input_start
  , mv.endtime as input_end
  , mv.itemid as item
  , it.label as label
  , mv.amount as amount
  , mv.amountuom as amountuom
  , mv.rate as rate
  , mv.rateuom as rateuom
  , mv.ordercategorydescription as ordercat
  , mv.patientweight as ptweight
  , mv.totalamount as totalamount
  , mv.totalamountuom as totalamountuom
  , mv.statusdescription as status
FROM admissions ad
INNER JOIN procedureevents_mv pr
ON ad.subject_id = pr.subject_id
INNER JOIN inputevents_mv mv 
ON ad.hadm_id = mv.hadm_id
INNER JOIN d_items it
ON mv.itemid = it.itemid
WHERE pr.itemid = """ + str(vent_id) + """ AND pr.hadm_id = ad.hadm_id 
ORDER BY ad.subject_id, input_start
"""

inputevents_mv = pd.read_sql_query(query,con)

# table of sedations times + doses

cur.execute('SET search_path to ' + schema_name)

query = """
SELECT ad.subject_id as subject
  , ad.hadm_id as hadm
  , dr.drug as drug
  , dr.formulary_drug_cd
  , dr.startdate as sed_starttime
  , dr.enddate as sed_endtime
  , dr.prod_strength as strength
  , dr.dose_val_rx as dose
  , dr.dose_unit_rx as unitd
  , dr.form_val_disp as form
  , dr.form_unit_disp as unitf
  , dr.route
FROM admissions ad
INNER JOIN procedureevents_mv pr
ON ad.subject_id = pr.subject_id
INNER JOIN prescriptions dr 
ON ad.hadm_id = dr.hadm_id
WHERE pr.itemid = """ + str(vent_id) + """ AND pr.hadm_id = ad.hadm_id 
ORDER BY ad.subject_id, sed_starttime
"""

sedation = pd.read_sql_query(query,con)


#AND (dr.drug LIKE '%Propofol%' OR dr.drug LIKE '%Midazolam%' 
#OR dr.drug LIKE '%Dexmedetomidine%' OR dr.drug LIKE '%Lorazepam%' 
#OR dr.drug LIKE '%Fentanyl%' OR dr.drug LIKE '%HYDROmorphone%' OR dr.drug LIKE '%Morphine%' )

# Respiratory rate: 618, 619, 220210(*), 224688(*), 224689(*), 224690(*)
# Heart rate: 211, 220045(*)
# Arterial pH: 780, 223830(*)
# FiO2: 190, 3420, 3422, 7570, 227010, 227009, 223835(*), 226754
# SpO2: 646, 220277(*)
# PEEP: 505, 506, 686, 224700, 220339(*), 6350
# Tidal volume: 681, 224684, 224685, 224686
# Ventilator mode: 720, 223849
# Non invasive BP (systolic, diastolic,mean): 220179, 220180, 220181
# Admission weight (kg): 226512
# Height (cm): 226730
# BIS index: 228444; Richmond RAS (not available): 228096
# SBT: 224715, 224716, 224717, 224833
# Arterial o2/co2: 220224, 220227, 220235,
# Misc: 

#AND ch.itemid in (618, 619, 220210, 224688, 224689, 224690, 211, 220045, 780, 223830, 
#190, 3420, 3422, 7570, 227010, 227009, 223835, 226754, 646, 220277, 505, 506, 686, 224700, 220339, 6350,
#681, 224684, 224685, 224686, 720, 223849, 220179, 220180, 220181, 226730, 226512, 
#228444, 220224, 220227, 220235, 
#223762, 226329, 220052, 228640, 224359, 
#224715, 224716, 224717, 224833)

cur.execute('SET search_path to ' + schema_name)

query = """
SELECT ad.subject_id as subject
  , ad.hadm_id as hadm
  , ch.itemid
  , di.label
  , di.unitname as unit
  , ch.charttime
  , ch.valuenum as value
  , ad.hospital_expire_flag as h_exp
FROM admissions ad
INNER JOIN procedureevents_mv pr
ON ad.subject_id = pr.subject_id
INNER JOIN chartevents ch 
ON ad.hadm_id = ch.hadm_id
INNER join d_items di
ON ch.itemid = di.itemid
WHERE pr.itemid = """ + str(vent_id) + """ AND pr.hadm_id = ad.hadm_id 
AND ch.itemid in (220210,3337,224422,618,3603,615,619,224688,224689,
211,220045,
780,223830,1126,4753,3839,
223835,3420,2981,727,3422,227009,
646,220277,
220339,505,506,224700,686,
224684,683,224685,682,224686,684,224421,654,3050,2566,681,3083,2311,
223849,720,
220179,455,
220180,8441,
220181,456,
1394,226707, 226730,
763,224639,3580,3583,226512,581,580,
676,223762,
224687,448,450,445,
470,471,223834,227287,194,224691,
224697,444,
224695,535,
224696,543,
220224,3785,3837,
220235,3784,3835,
220227,834,
30056,226452,225797,
30058,30065,225944,
40055,43175,40069,40094,40715,40473,40085,40057,40056,40405,40428,40096,40,651,226560,227510,226561,227489,226584,226563,226564,226565,226557,226558,226559,
224715, 224716, 224717, 224833,
228444,
228096,
198,227013,226755,
1090,
225059,225811,
225624,220615,225170,227428,220545,226996,
227054,227000,227005,227017)
ORDER BY ad.subject_id, ch.charttime
"""
vitals = pd.read_sql_query(query,con)

cur.execute('SET search_path to ' + schema_name)

query = """
SELECT ad.subject_id as subject
  , ad.hadm_id as hadm
  , ne.chartdate as note_date
  , ne.category as category
  , ne.description as description
  , ne.cgid as cgid
  , ne.iserror as iserror
  , ne.text as text
FROM admissions ad
INNER JOIN procedureevents_mv pr
ON ad.subject_id = pr.subject_id
INNER JOIN noteevents ne
ON ad.hadm_id = ne.hadm_id
WHERE pr.itemid = """ + str(vent_id) + """ AND pr.hadm_id = ad.hadm_id 
AND (ne.category NOT LIKE '%Radiology%')
ORDER BY ad.subject_id
"""

notes = pd.read_sql_query(query,con)

cur.execute('SET search_path to ' + schema_name)

query = """
SELECT ad.subject_id as subject
  , ad.hadm_id as hadm
  , di.icd9_code as diag_code
  , pi.icd9_code as proc_code
  , did.short_title as short_diag
  , did.long_title as long_diag
  , pid.short_title as short_proc
  , pid.long_title as long_proc
FROM admissions ad
INNER JOIN procedureevents_mv pr
ON ad.subject_id = pr.subject_id
INNER JOIN diagnoses_icd di
ON ad.hadm_id = di.hadm_id
INNER JOIN procedures_icd pi
ON ad.hadm_id = pi.hadm_id
INNER JOIN d_icd_diagnoses did
ON di.icd9_code = did.icd9_code
INNER JOIN d_icd_procedures pid
ON pi.icd9_code = pid.icd9_code
WHERE pr.itemid = """ + str(vent_id) + """ AND pr.hadm_id = ad.hadm_id 
ORDER BY ad.subject_id
"""

diagnoses = pd.read_sql_query(query,con)
