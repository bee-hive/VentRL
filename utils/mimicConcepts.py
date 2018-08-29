####################################################################

### Code: Niranjani Prasad - Princeton University (2018)

### Concept queries (eg. Elixhauser, OASIS, etc) taken from: 
### https://github.com/MIT-LCP/mimic-code/tree/master/concepts

####################################################################

import psycopg2
import pandas as pd

# Create a database connection (**changing sql database credentials as appropriate**)
sqluser = 'bee_mimic_admin'
dbname = 'bee_mimic'
schema_name = 'mimiciii'
sqlpwd = "2OKF0dr@czOrD0suS4GyN"
con = psycopg2.connect(dbname=dbname, user=sqluser, password=sqlpwd)
cur = con.cursor()
cur.execute('SET search_path to ' + schema_name)

def q(query):
    # Query function
    con = psycopg2.connect(dbname=dbname, user=sqluser, password=sqlpwd)
    cur = con.cursor()
    cur.execute('SET search_path to ' + schema_name)
    return pd.read_sql_query(query,con)

def admissions():
    # Get demographic info from ADMISSIONS table
    query = """
SELECT DISTINCT on (ie.icustay_id)
    ad.subject_id as subject
  , ad.hadm_id as hadm
  , ie.icustay_id as icustay
  , ROUND( (CAST(EXTRACT(epoch FROM ad.admittime - pa.dob)/(60*60*24*365.242) AS numeric)), 1) AS age
  , pa.gender
  , ad.ethnicity as ethnicity
  , ad.admission_type as admittype 
  , ad.diagnosis
  , ie.first_careunit
--
  , ad.admittime as admit_time
  , ie.intime as icu_admit
  , ie.outtime as icu_discharge  
  , ad.dischtime as discharge_time
  , ROUND(CAST(ie.los AS numeric), 2) AS icu_los
  , ROUND( (CAST(EXTRACT(epoch FROM ad.dischtime - ad.admittime)/(60*60*24) AS numeric)), 2) AS adm_los
  , ad.hospital_expire_flag as h_exp
  , pa.expire_flag as exp
--
--  , ad.deathtime as death_time  
--  , ad.admission_location as adm_loc
--  , ad.discharge_location as disch_loc
--  , ad.insurance as insurance
--  , ad.language as language
--  , ad.religion as religion
--  , ad.marital_status as marital
--  , ad.edregtime as emerg_admit
--  , ad.edouttime as emerg_disch
--  , ie.dbsource as dbsource
--  , ie.last_careunit
--  , ie.first_wardid
--  , ie.last_wardid
--  , pa.dob as dob
--  , pa.dod as dod
--
FROM admissions ad
INNER JOIN icustays ie
ON ad.hadm_id = ie.hadm_id
INNER JOIN patients pa
ON ad.subject_id = pa.subject_id
WHERE ad.has_chartevents_data = 1
-- ORDER BY ad.subject_id, ad.admittime, ie.inttime
"""
    
    return q(query).drop_duplicates()

def comorbidities():
    # Get ICD9 codes, corresponding descriptions for admission diagnoses from DIAGNOSES_ICD table
    
    query = """
SELECT  ad.subject_id as subject
  , ad.hadm_id as hadm
  , ie.icustay_id as icustay
  , di.icd9_code as code
  , did.short_title as short_desc
  , did.long_title as long_desc
FROM admissions ad
INNER JOIN icustays ie
ON ad.hadm_id = ie.hadm_id
INNER JOIN diagnoses_icd di
ON ad.hadm_id = di.hadm_id
INNER JOIN d_icd_diagnoses did
ON di.icd9_code = did.icd9_code
"""
     
    return q(query).drop_duplicates()

def procedures():
    # Get ICD9 codes, corresponding descriptions for admission procedures from PROCEDURES_ICD table 
    
    query = """
SELECT  ad.subject_id as subject
  , ad.hadm_id as hadm
  , ie.icustay_id as icustay
  , pi.icd9_code as code
  , pid.short_title as short_desc
  , pid.long_title as long_desc
FROM admissions ad
INNER JOIN icustays ie
ON ad.hadm_id = ie.hadm_id
INNER JOIN procedures_icd pi
ON ad.hadm_id = pi.hadm_id
INNER JOIN d_icd_procedures pid
ON pi.icd9_code = pid.icd9_code
"""
    
    return q(query).drop_duplicates()

def inputs(hadms, drugs):
    # Get drugs administered for a given set of admissions, drugs from INPUTEVENTS_MV    
    # Considers only data from the Metavision system (patients from 2008 onwards) 

    # Takes as input a list of hadms, list of drugs names to be extracted

    l = ','.join('\'{0}\''.format(h) for h in hadms)
    dl = ','.join('\'{0}\''.format(d) for d in drugs)
    
    query = """
SELECT  ad.subject_id as subject
  , ad.hadm_id as hadm
  , ie.icustay_id as icustay
  , ad.diagnosis
  , mv.itemid as item
  , it.label as label
  , mv.ordercategoryname as ordercat
  , mv.starttime as input_start
  , mv.endtime as input_end
  , mv.amount as amount
  , mv.amountuom as amountuom
  , mv.rate as rate
  , mv.rateuom as rateuom
  , mv.patientweight as ptweight
  , mv.totalamount as totalamount
  , mv.totalamountuom as totalamountuom
FROM admissions ad
INNER JOIN icustays ie
ON ad.hadm_id = ie.hadm_id
INNER JOIN diagnoses_icd di
ON ad.hadm_id = di.hadm_id
INNER JOIN d_icd_diagnoses did
ON di.icd9_code = did.icd9_code
INNER JOIN inputevents_mv mv 
ON ad.hadm_id = mv.hadm_id
INNER JOIN d_items it
ON mv.itemid = it.itemid
WHERE ad.hadm_id in (""" + l + """)
AND it.label in (""" + dl + """)
ORDER BY ad.subject_id, mv.starttime
"""

    return q(query).drop_duplicates()

def inputs_cv(hadms, drugs):
    # Get drugs administered for a given set of admissions, drugs from INPUTEVENTS_CV  - not used  
    # Considers only data from the CareVue system (patients from 2001-08)

    # Takes as input a list of hadms, list of drugs names to be extracted

    l = ','.join('\'{0}\''.format(h) for h in hadms)
    dl = ','.join('\'{0}\''.format(d) for d in drugs)
    
    query = """
SELECT  ad.subject_id as subject
  , ad.hadm_id as hadm
  , ie.icustay_id as icustay
  , ad.diagnosis
  , cv.itemid as item
  , it.label as label
  , cv.orderid as ordercat
  , cv.charttime as input_start
  , cv.charttime as input_end
  , cv.amount as amount
  , cv.amountuom as amountuom
  , cv.rate as rate
  , cv.rateuom as rateuom
  , cv.patientweight as ptweight
  , cv.originalamount as totalamount
  , cv.originalamountuom as totalamountuom
FROM admissions ad
INNER JOIN icustays ie
ON ad.hadm_id = ie.hadm_id
INNER JOIN diagnoses_icd di
ON ad.hadm_id = di.hadm_id
INNER JOIN d_icd_diagnoses did
ON di.icd9_code = did.icd9_code
INNER JOIN inputevents_cv cv 
ON ad.hadm_id = mv.hadm_id
INNER JOIN d_items it
ON mv.itemid = it.itemid
WHERE ad.hadm_id in (""" + l + """)
AND it.label in (""" + dl + """)
ORDER BY ad.subject_id, mv.starttime
"""

    return q(query).drop_duplicates()

def charts(hadms, vits):
    # Gets recorded vitals measurements from CHARTEVENTS table
    # Takes as input a list of hadms, list of vitals to be extracted
    
    l = ','.join('\'{0}\''.format(h) for h in hadms)
    cl = ','.join('\'{0}\''.format(d) for d in vits)
    
    query = """
SELECT ad.subject_id as subject
  , ad.hadm_id as hadm
  , ch.icustay_id as icustay
  , ch.itemid
  , di.label
  , di.unitname as unit
  , ch.charttime
  , ch.valuenum as value
FROM admissions ad
INNER JOIN chartevents ch 
ON ad.hadm_id = ch.hadm_id
INNER join d_items di
ON ch.itemid = di.itemid
WHERE ad.hadm_id in (""" + l + """)
AND di.label in (""" + cl + """)
ORDER BY ad.subject_id, ch.charttime
""" 
    
    return q(query).drop_duplicates()

def labs(hadms, labs):
    # Gets recorded lab measurements from LABEVENTS table
    # Takes as input a list of hadms, list of labs to be extracted
    
    l = ','.join('\'{0}\''.format(h) for h in hadms)
    ll = ','.join('\'{0}\''.format(d) for d in labs)
    
    query = """
SELECT ad.subject_id as subject
  , ad.hadm_id as hadm
  , ie.icustay_id as icustay
  , lb.itemid
  , dl.label
  , lb.charttime
  , lb.valuenum as value
FROM admissions ad
INNER JOIN icustays ie
ON ad.hadm_id = ie.hadm_id
INNER JOIN labevents lb 
ON ad.hadm_id = lb.hadm_id
INNER join d_labitems dl
ON lb.itemid = dl.itemid
WHERE ad.hadm_id in (""" + l + """)
AND dl.label in (""" + ll + """)
ORDER BY ad.subject_id, lb.charttime
"""
    
    return q(query).drop_duplicates()

def notes():
    # Gets admission notes from NOTEEVENTS table
    
    query = """
SELECT ad.subject_id as subject
  , ad.hadm_id as hadm
  , ie.icustay_id as icustay
  , ne.chartdate as note_date
  , ne.category as category
  , ne.description as description
  , ne.cgid as cgid
  , ne.iserror as iserror
  , ne.text as text
FROM admissions ad
INNER JOIN icustays ie
ON ad.hadm_id = ie.hadm_id
INNER JOIN noteevents ne
ON ad.hadm_id = ne.hadm_id
WHERE (ne.category NOT LIKE '%Radiology%')
ORDER BY ad.subject_id
"""

    return q(query).drop_duplicates()

def caregivers():
    # Get caregiver ids for admissions from CAREGIVERS table  
    
    query = """
select ce.icustay_id, ce.charttime, ce.cgid, cg.label
from mimiciii.chartevents ce
inner join caregivers cg
on ce.cgid = cg.cgid
where value is not null 
order by icustay_id, cgid
"""
    
    return q(query)

def ventilation():
    # Gets admissions with mechanical ventilation (by procedure ID),
    # with start and end times.
    
    vent_id = 225792
    query = """
SELECT pr.subject_id as subject
  , pr.hadm_id as hadm
  , pr.icustay_id as icu_stay
  , pr.starttime as vent_starttime
  , pr.endtime as vent_endtime
--  , extract(epoch from pr.starttime) as vent_starttime_epoch
--  , extract(epoch from pr.endtime) as vent_endtime_epoch
FROM  procedureevents_mv pr
WHERE pr.itemid = """ + str(vent_id) + """
ORDER BY pr.icustay_id, pr.starttime
"""
    
    return q(query).drop_duplicates().dropna()

####################################################################
###                     CLINICAL CONCEPTS                        ###
####################################################################

def elixhauser():
    # Comorbidity score
    
    query =  """

-- This code uses the latest version of Elixhauser provided by AHRQ

with
icd as
(
  select hadm_id, subject_id, seq_num
    , cast(icd9_code as char(5)) as icd9_code
  from diagnoses_icd
  where seq_num != 1 -- we do not include the primary icd-9 code
)
,
eliflg as
(
select hadm_id, subject_id, seq_num, icd9_code
-- note that these codes will seem incomplete at first
-- for example, CHF is missing a lot of codes referenced in the literature (402.11, 402.91, etc)
-- these codes are captured by hypertension flags instead
-- later there are some complicated rules which confirm/reject those codes as CHF
, CASE
  when icd9_code = '39891' then 1
  when icd9_code between '4280 ' and '4289 ' then 1
		end as CHF       /* Congestive heart failure */

-- cardiac arrhythmias is removed in up to date versions
, case
    when icd9_code = '42610' then 1
    when icd9_code = '42611' then 1
    when icd9_code = '42613' then 1
    when icd9_code between '4262 ' and '42653' then 1
    when icd9_code between '4266 ' and '42689' then 1
    when icd9_code = '4270 ' then 1
    when icd9_code = '4272 ' then 1
    when icd9_code = '42731' then 1
    when icd9_code = '42760' then 1
    when icd9_code = '4279 ' then 1
    when icd9_code = '7850 ' then 1
    when icd9_code between 'V450 ' and 'V4509' then 1
    when icd9_code between 'V533 ' and 'V5339' then 1
  end as ARYTHM /* Cardiac arrhythmias */

, CASE
  when icd9_code between '09320' and '09324' then 1
  when icd9_code between '3940 ' and '3971 ' then 1
  when icd9_code = '3979 ' then 1
  when icd9_code between '4240 ' and '42499' then 1
  when icd9_code between '7463 ' and '7466 ' then 1
  when icd9_code = 'V422 ' then 1
  when icd9_code = 'V433 ' then 1
		end as VALVE     /* Valvular disease */

, CASE
  when icd9_code between '41511' and '41519' then 1
  when icd9_code between '4160 ' and '4169 ' then 1
  when icd9_code = '4179 ' then 1
		end as PULMCIRC  /* Pulmonary circulation disorder */

, CASE
  when icd9_code between '4400 ' and '4409 ' then 1
  when icd9_code between '44100' and '4419 ' then 1
  when icd9_code between '4420 ' and '4429 ' then 1
  when icd9_code between '4431 ' and '4439 ' then 1
  when icd9_code between '44421' and '44422' then 1
  when icd9_code = '4471 ' then 1
  when icd9_code = '449  ' then 1
  when icd9_code = '5571 ' then 1
  when icd9_code = '5579 ' then 1
  when icd9_code = 'V434 ' then 1
		end as PERIVASC  /* Peripheral vascular disorder */

, CASE
  when icd9_code = '4011 ' then 1
  when icd9_code = '4019 ' then 1
  when icd9_code between '64200' and '64204' then 1
		end as HTN       /* Hypertension, uncomplicated */

, CASE
  when icd9_code = '4010 ' then 1
  when icd9_code = '4372 ' then 1
		end as HTNCX     /* Hypertension, complicated */


      /******************************************************************/
      /* The following are special, temporary formats used in the       */
      /* creation of the hypertension complicated comorbidity when      */
      /* overlapping with congestive heart failure or renal failure     */
      /* occurs. These temporary formats are referenced in the program  */
      /* called comoanaly2009.txt.                                      */
      /******************************************************************/
, CASE
  when icd9_code between '64220' and '64224' then 1
		end as HTNPREG   /* Pre-existing hypertension complicating pregnancy */

, CASE
  when icd9_code = '40200' then 1
  when icd9_code = '40210' then 1
  when icd9_code = '40290' then 1
  when icd9_code = '40509' then 1
  when icd9_code = '40519' then 1
  when icd9_code = '40599'         then 1
		end as HTNWOCHF  /* Hypertensive heart disease without heart failure */

, CASE
  when icd9_code = '40201' then 1
  when icd9_code = '40211' then 1
  when icd9_code = '40291'         then 1
		end as HTNWCHF   /* Hypertensive heart disease with heart failure */

, CASE
  when icd9_code = '40300' then 1
  when icd9_code = '40310' then 1
  when icd9_code = '40390' then 1
  when icd9_code = '40501' then 1
  when icd9_code = '40511' then 1
  when icd9_code = '40591' then 1
  when icd9_code between '64210' and '64214' then 1
		end as HRENWORF  /* Hypertensive renal disease without renal failure */

, CASE
  when icd9_code = '40301' then 1
  when icd9_code = '40311' then 1
  when icd9_code = '40391'         then 1
		end as HRENWRF   /* Hypertensive renal disease with renal failure */

, CASE
  when icd9_code = '40400' then 1
  when icd9_code = '40410' then 1
  when icd9_code = '40490'         then 1
		end as HHRWOHRF  /* Hypertensive heart and renal disease without heart or renal failure */

, CASE
  when icd9_code = '40401' then 1
  when icd9_code = '40411' then 1
  when icd9_code = '40491'         then 1
		end as HHRWCHF   /* Hypertensive heart and renal disease with heart failure */

, CASE
  when icd9_code = '40402' then 1
  when icd9_code = '40412' then 1
  when icd9_code = '40492'         then 1
		end as HHRWRF    /* Hypertensive heart and renal disease with renal failure */

, CASE
  when icd9_code = '40403' then 1
  when icd9_code = '40413' then 1
  when icd9_code = '40493'         then 1
		end as HHRWHRF   /* Hypertensive heart and renal disease with heart and renal failure */

, CASE
  when icd9_code between '64270' and '64274' then 1
  when icd9_code between '64290' and '64294' then 1
		end as OHTNPREG  /* Other hypertension in pregnancy */

      /******************** End Temporary Formats ***********************/

, CASE
  when icd9_code between '3420 ' and '3449 ' then 1
  when icd9_code between '43820' and '43853' then 1
  when icd9_code = '78072'         then 1
		end as PARA      /* Paralysis */

, CASE
  when icd9_code between '3300 ' and '3319 ' then 1
  when icd9_code = '3320 ' then 1
  when icd9_code = '3334 ' then 1
  when icd9_code = '3335 ' then 1
  when icd9_code = '3337 ' then 1
  when icd9_code in ('33371','33372','33379','33385','33394') then 1
  when icd9_code between '3340 ' and '3359 ' then 1
  when icd9_code = '3380 ' then 1
  when icd9_code = '340  ' then 1
  when icd9_code between '3411 ' and '3419 ' then 1
  when icd9_code between '34500' and '34511' then 1
  when icd9_code between '3452 ' and '3453 ' then 1
  when icd9_code between '34540' and '34591' then 1
  when icd9_code between '34700' and '34701' then 1
  when icd9_code between '34710' and '34711' then 1
  when icd9_code = '3483' then 1 -- discontinued icd-9
  when icd9_code between '64940' and '64944' then 1
  when icd9_code = '7687 ' then 1
  when icd9_code between '76870' and '76873' then 1
  when icd9_code = '7803 ' then 1
  when icd9_code = '78031' then 1
  when icd9_code = '78032' then 1
  when icd9_code = '78033' then 1
  when icd9_code = '78039' then 1
  when icd9_code = '78097' then 1
  when icd9_code = '7843 '         then 1
		end as NEURO     /* Other neurological */

, CASE
  when icd9_code between '490  ' and '4928 ' then 1
  when icd9_code between '49300' and '49392' then 1
  when icd9_code between '494  ' and '4941 ' then 1
  when icd9_code between '4950 ' and '505  ' then 1
  when icd9_code = '5064 '         then 1
		end as CHRNLUNG  /* Chronic pulmonary disease */

, CASE
  when icd9_code between '25000' and '25033' then 1
  when icd9_code between '64800' and '64804' then 1
  when icd9_code between '24900' and '24931' then 1
		end as DM        /* Diabetes w/o chronic complications*/

, CASE
  when icd9_code between '25040' and '25093' then 1
  when icd9_code = '7751 ' then 1
  when icd9_code between '24940' and '24991' then 1
		end as DMCX      /* Diabetes w/ chronic complications */

, CASE
  when icd9_code between '243  ' and '2442 ' then 1
  when icd9_code = '2448 ' then 1
  when icd9_code = '2449 '         then 1
		end as HYPOTHY   /* Hypothyroidism */

, CASE
  when icd9_code = '585  ' then 1 -- discontinued code
  when icd9_code = '5853 ' then 1
  when icd9_code = '5854 ' then 1
  when icd9_code = '5855 ' then 1
  when icd9_code = '5856 ' then 1
  when icd9_code = '5859 ' then 1
  when icd9_code = '586  ' then 1
  when icd9_code = 'V420 ' then 1
  when icd9_code = 'V451 ' then 1
  when icd9_code between 'V560 ' and 'V5632' then 1
  when icd9_code = 'V568 ' then 1
  when icd9_code between 'V4511' and 'V4512' then 1
		end as RENLFAIL  /* Renal failure */

, CASE
  when icd9_code = '07022' then 1
  when icd9_code = '07023' then 1
  when icd9_code = '07032' then 1
  when icd9_code = '07033' then 1
  when icd9_code = '07044' then 1
  when icd9_code = '07054' then 1
  when icd9_code = '4560 ' then 1
  when icd9_code = '4561 ' then 1
  when icd9_code = '45620' then 1
  when icd9_code = '45621' then 1
  when icd9_code = '5710 ' then 1
  when icd9_code = '5712 ' then 1
  when icd9_code = '5713 ' then 1
  when icd9_code between '57140' and '57149' then 1
  when icd9_code = '5715 ' then 1
  when icd9_code = '5716 ' then 1
  when icd9_code = '5718 ' then 1
  when icd9_code = '5719 ' then 1
  when icd9_code = '5723 ' then 1
  when icd9_code = '5728 ' then 1
  when icd9_code = '5735 ' then 1
  when icd9_code = 'V427 '         then 1
		end as LIVER     /* Liver disease */

, CASE
  when icd9_code = '53141' then 1
  when icd9_code = '53151' then 1
  when icd9_code = '53161' then 1
  when icd9_code = '53170' then 1
  when icd9_code = '53171' then 1
  when icd9_code = '53191' then 1
  when icd9_code = '53241' then 1
  when icd9_code = '53251' then 1
  when icd9_code = '53261' then 1
  when icd9_code = '53270' then 1
  when icd9_code = '53271' then 1
  when icd9_code = '53291' then 1
  when icd9_code = '53341' then 1
  when icd9_code = '53351' then 1
  when icd9_code = '53361' then 1
  when icd9_code = '53370' then 1
  when icd9_code = '53371' then 1
  when icd9_code = '53391' then 1
  when icd9_code = '53441' then 1
  when icd9_code = '53451' then 1
  when icd9_code = '53461' then 1
  when icd9_code = '53470' then 1
  when icd9_code = '53471' then 1
  when icd9_code = '53491'         then 1
		end as ULCER     /* Chronic Peptic ulcer disease (includes bleeding only if obstruction is also present) */

, CASE
  when icd9_code between '042  ' and '0449 ' then 1
		end as AIDS      /* HIV and AIDS */

, CASE
  when icd9_code between '20000' and '20238' then 1
  when icd9_code between '20250' and '20301' then 1
  when icd9_code = '2386 ' then 1
  when icd9_code = '2733 ' then 1
  when icd9_code between '20302' and '20382' then 1
		end as LYMPH     /* Lymphoma */

, CASE
  when icd9_code between '1960 ' and '1991 ' then 1
  when icd9_code between '20970' and '20975' then 1
  when icd9_code = '20979' then 1
  when icd9_code = '78951'         then 1
		end as METS      /* Metastatic cancer */

, CASE
  when icd9_code between '1400 ' and '1729 ' then 1
  when icd9_code between '1740 ' and '1759 ' then 1
  when icd9_code between '179  ' and '1958 ' then 1
  when icd9_code between '20900' and '20924' then 1
  when icd9_code between '20925' and '2093 ' then 1
  when icd9_code between '20930' and '20936' then 1
  when icd9_code between '25801' and '25803' then 1
		end as TUMOR     /* Solid tumor without metastasis */

, CASE
  when icd9_code = '7010 ' then 1
  when icd9_code between '7100 ' and '7109 ' then 1
  when icd9_code between '7140 ' and '7149 ' then 1
  when icd9_code between '7200 ' and '7209 ' then 1
  when icd9_code = '725  ' then 1
		end as ARTH              /* Rheumatoid arthritis/collagen vascular diseases */

, CASE
  when icd9_code between '2860 ' and '2869 ' then 1
  when icd9_code = '2871 ' then 1
  when icd9_code between '2873 ' and '2875 ' then 1
  when icd9_code between '64930' and '64934' then 1
  when icd9_code = '28984'         then 1
		end as COAG      /* Coagulation deficiency */

, CASE
  when icd9_code = '2780 ' then 1
  when icd9_code = '27800' then 1
  when icd9_code = '27801' then 1
  when icd9_code = '27803' then 1
  when icd9_code between '64910' and '64914' then 1
  when icd9_code between 'V8530' and 'V8539' then 1
  when icd9_code = 'V854 ' then 1 -- hierarchy used for AHRQ v3.6 and earlier
  when icd9_code between 'V8541' and 'V8545' then 1
  when icd9_code = 'V8554' then 1
  when icd9_code = '79391'         then 1
		end as OBESE     /* Obesity      */

, CASE
  when icd9_code between '260  ' and '2639 ' then 1
  when icd9_code between '78321' and '78322' then 1
		end as WGHTLOSS  /* Weight loss */

, CASE
  when icd9_code between '2760 ' and '2769 ' then 1
		end as LYTES     /* Fluid and electrolyte disorders - note:
                                      this comorbidity should be dropped when
                                      used with the AHRQ Patient Safety Indicators*/
, CASE
  when icd9_code = '2800 ' then 1
  when icd9_code between '64820' and '64824' then 1
		end as BLDLOSS   /* Blood loss anemia */

, CASE
  when icd9_code between '2801 ' and '2819 ' then 1
  when icd9_code between '28521' and '28529' then 1
  when icd9_code = '2859 '         then 1
		end as ANEMDEF  /* Deficiency anemias */

, CASE
  when icd9_code between '2910 ' and '2913 ' then 1
  when icd9_code = '2915 ' then 1
  when icd9_code = '2918 ' then 1
  when icd9_code = '29181' then 1
  when icd9_code = '29182' then 1
  when icd9_code = '29189' then 1
  when icd9_code = '2919 ' then 1
  when icd9_code between '30300' and '30393' then 1
  when icd9_code between '30500' and '30503' then 1
		end as ALCOHOL   /* Alcohol abuse */

, CASE
  when icd9_code = '2920 ' then 1
  when icd9_code between '29282' and '29289' then 1
  when icd9_code = '2929 ' then 1
  when icd9_code between '30400' and '30493' then 1
  when icd9_code between '30520' and '30593' then 1
  when icd9_code between '64830' and '64834' then 1
		end as DRUG      /* Drug abuse */

, CASE
  when icd9_code between '29500' and '2989 ' then 1
  when icd9_code = '29910' then 1
  when icd9_code = '29911'         then 1
		end as PSYCH    /* Psychoses */

, CASE
  when icd9_code = '3004 ' then 1
  when icd9_code = '30112' then 1
  when icd9_code = '3090 ' then 1
  when icd9_code = '3091 ' then 1
  when icd9_code = '311  '         then 1
		end as DEPRESS  /* Depression */
from icd
)
-- collapse the icd9_code specific flags into hadm_id specific flags
-- this groups comorbidities together for a single patient admission
, eligrp as
(
  select hadm_id, subject_id
  , max(chf) as chf
  , max(arythm) as arythm
  , max(valve) as valve
  , max(pulmcirc) as pulmcirc
  , max(perivasc) as perivasc
  , max(htn) as htn
  , max(htncx) as htncx
  , max(htnpreg) as htnpreg
  , max(htnwochf) as htnwochf
  , max(htnwchf) as htnwchf
  , max(hrenworf) as hrenworf
  , max(hrenwrf) as hrenwrf
  , max(hhrwohrf) as hhrwohrf
  , max(hhrwchf) as hhrwchf
  , max(hhrwrf) as hhrwrf
  , max(hhrwhrf) as hhrwhrf
  , max(ohtnpreg) as ohtnpreg
  , max(para) as para
  , max(neuro) as neuro
  , max(chrnlung) as chrnlung
  , max(dm) as dm
  , max(dmcx) as dmcx
  , max(hypothy) as hypothy
  , max(renlfail) as renlfail
  , max(liver) as liver
  , max(ulcer) as ulcer
  , max(aids) as aids
  , max(lymph) as lymph
  , max(mets) as mets
  , max(tumor) as tumor
  , max(arth) as arth
  , max(coag) as coag
  , max(obese) as obese
  , max(wghtloss) as wghtloss
  , max(lytes) as lytes
  , max(bldloss) as bldloss
  , max(anemdef) as anemdef
  , max(alcohol) as alcohol
  , max(drug) as drug
  , max(psych) as psych
  , max(depress) as depress
from eliflg
group by hadm_id, subject_id
)

-- DRG FILTER --
, msdrg as
(
select
  hadm_id, subject_id
/**** V29 MS-DRG Formats ****/

/* Cardiac */
, case
    when d.drg_code between 001 and 002 then 1
    when d.drg_code between 215 and 238 then 1
    when d.drg_code between 242 and 252 then 1
    when d.drg_code between 253 and 254 then 1
    when d.drg_code between 258 and 262 then 1
    when d.drg_code between 265 and 267 then 1
    when d.drg_code between 280 and 293 then 1
    when d.drg_code between 296 and 298 then 1
    when d.drg_code between 302 and 303 then 1
    when d.drg_code between 306 and 313 then 1
else 0 end as CARDDRG

/* Peripheral vascular */
, case
    when d.drg_code between 299 and 301 then 1
else 0 end as PERIDRG

/* Renal */
, case
    when d.drg_code = 652 then 1
    when d.drg_code between 656 and 661 then 1
    when d.drg_code between 673 and 675 then 1
    when d.drg_code between 682 and 700 then 1
else 0 end as RENALDRG

/* Nervous system */
, case
    when d.drg_code between 020 and 042 then 1
    when d.drg_code between 052 and 103 then 1
else 0 end as NERVDRG

/* Cerebrovascular */
, case
    when d.drg_code between 020 and 022 then 1
    when d.drg_code between 034 and 039 then 1
    when d.drg_code between 064 and 072 then 1
else 0 end as CEREDRG

/* COPD asthma */
, case
    when d.drg_code between 190 and 192 then 1
    when d.drg_code between 202 and 203 then 1
else 0 end as PULMDRG

/* Diabetes */
, case
    when d.drg_code between 637 and 639 then 1
else 0 end as  DIABDRG

/* Thyroid endocrine */
, case
    when d.drg_code between 625 and 627 then 1
    when d.drg_code between 643 and 645 then 1
else 0 end as HYPODRG

/* Kidney transp, renal fail/dialysis */
, case
    when d.drg_code = 652 then 1
    when d.drg_code between 682 and 685 then 1
else 0 end as RENFDRG

/* Liver */
, case
    when d.drg_code between 420 and 425 then 1
    when d.drg_code between 432 and 434 then 1
    when d.drg_code between 441 and 446 then 1
else 0 end as LIVERDRG

/* GI hemorrhage or ulcer */
, case
    when d.drg_code between 377 and 384 then 1
else 0 end as ULCEDRG

/* Human immunodeficiency virus */
, case
    when d.drg_code between 969 and 970 then 1
    when d.drg_code between 974 and 977 then 1
else 0 end as HIVDRG

/* Leukemia/lymphoma */
, case
    when d.drg_code between 820 and 830 then 1
    when d.drg_code between 834 and 849 then 1
else 0 end as LEUKDRG

/* Cancer, lymphoma */
, case
    when d.drg_code = 054 then 1
    when d.drg_code = 055 then 1
    when d.drg_code between 146 and 148 then 1
    when d.drg_code between 180 and 182 then 1
    when d.drg_code between 374 and 376 then 1
    when d.drg_code between 435 and 437 then 1
    when d.drg_code between 542 and 544 then 1
    when d.drg_code between 582 and 585 then 1
    when d.drg_code between 597 and 599 then 1
    when d.drg_code between 656 and 658 then 1
    when d.drg_code between 686 and 688 then 1
    when d.drg_code between 715 and 716 then 1
    when d.drg_code between 722 and 724 then 1
    when d.drg_code between 736 and 741 then 1
    when d.drg_code between 754 and 756 then 1
    when d.drg_code between 826 and 830 then 1
    when d.drg_code between 843 and 849 then 1
else 0 end as CANCDRG

/* Connective tissue */
, case
    when d.drg_code between 545 and 547 then 1
else 0 end as ARTHDRG

/* Nutrition/metabolic */
, case
    when d.drg_code between 640 and 641 then 1
else 0 end as NUTRDRG

/* Anemia */
, case
    when d.drg_code between 808 and 812 then 1
else 0 end as ANEMDRG

/* Alcohol drug */
, case
    when d.drg_code between 894 and 897 then 1
else 0 end as ALCDRG

/*Coagulation disorders*/
, case
    when d.drg_code = 813 then 1
else 0 end as COAGDRG

/*Hypertensive Complicated  */
, case
    when d.drg_code = 077 then 1
    when d.drg_code = 078 then 1
    when d.drg_code = 304 then 1
else 0 end as HTNCXDRG

/*Hypertensive Uncomplicated  */
, case
    when d.drg_code = 079 then 1
    when d.drg_code = 305 then 1
else 0 end as HTNDRG

/* Psychoses */
, case
    when d.drg_code = 885 then 1
else 0 end as PSYDRG

/* Obesity */
, case
    when d.drg_code between 619 and 621 then 1
else 0 end as OBESEDRG

/* Depressive Neuroses */
, case
    when d.drg_code = 881 then 1
else 0 end as DEPRSDRG

from
(
  select hadm_id, subject_id, drg_type, cast(drg_code as numeric) as drg_code from drgcodes where drg_type = 'MS'
) d

)
, hcfadrg as
(
select
  hadm_id, subject_id

  /** V24 DRG Formats  **/

  /* Cardiac */
  , case
      when d.drg_code between 103 and 112 then 1
      when d.drg_code between 115 and 118 then 1
      when d.drg_code between 121 and 127 then 1
      when d.drg_code = 129 then 1
      when d.drg_code = 132 then 1
      when d.drg_code = 133 then 1
      when d.drg_code between 135 and 143 then 1
      when d.drg_code between 514 and 518 then 1
      when d.drg_code between 525 and 527 then 1
      when d.drg_code between 535 and 536 then 1
      when d.drg_code between 547 and 550 then 1
      when d.drg_code between 551 and 558 then 1
  else 0 end as CARDDRG

  /* Peripheral vascular */
  , case
      when d.drg_code = 130 then 1
      when d.drg_code = 131 then 1
  else 0 end as PERIDRG

  /* Renal */
  , case
      when d.drg_code between 302 and 305 then 1
      when d.drg_code between 315 and 333 then 1

  else 0 end as RENALDRG

  /* Nervous system */
  , case
      when d.drg_code between 1 and 35 then 1
      when d.drg_code = 524 then 1
      when d.drg_code between 528 and 534 then 1
      when d.drg_code = 543 then 1
      when d.drg_code between 559 and 564 then 1
      when d.drg_code = 577 then 1

  else 0 end as NERVDRG

   /* Cerebrovascular */
  , case
      when d.drg_code = 5 then 1
      when d.drg_code between 14 and 17 then 1
      when d.drg_code = 524 then 1
      when d.drg_code = 528 then 1
      when d.drg_code between 533 and 534 then 1
      when d.drg_code = 577 then 1
  else 0 end as CEREDRG

  /* COPD asthma */
  , case
      when d.drg_code = 88 then 1
      when d.drg_code between 96 and 98 then 1

  else 0 end as PULMDRG

  /* Diabetes */
  , case
      when d.drg_code = 294 then 1
      when d.drg_code = 295 then 1
  else 0 end as DIABDRG

  /* Thyroid endocrine */
  , case
      when d.drg_code = 290 then 1
      when d.drg_code = 300 then 1
      when d.drg_code = 301 then 1

  else 0 end as HYPODRG

  /* Kidney transp, renal fail/dialysis */
  , case
      when d.drg_code = 302 then 1
      when d.drg_code = 316 then 1
      when d.drg_code = 317 then 1
  else 0 end as RENFDRG

  /* Liver */
  , case
      when d.drg_code between 199 and 202 then 1
      when d.drg_code between 205 and 208 then 1

  else 0 end as LIVERDRG

  /* GI hemorrhage or ulcer */
  , case
      when d.drg_code between 174 and 178 then 1
  else 0 end as ULCEDRG

  /* Human immunodeficiency virus */
  , case
      when d.drg_code = 488 then 1
      when d.drg_code = 489 then 1
      when d.drg_code = 490 then 1

  else 0 end as HIVDRG

  /* Leukemia/lymphoma */
  , case
      when d.drg_code between 400 and 414 then 1
      when d.drg_code = 473 then 1
      when d.drg_code = 492 then 1
      when d.drg_code between 539 and 540 then 1

  else 0 end as LEUKDRG

  /* Cancer, lymphoma */
  , case
      when d.drg_code = 10 then 1
      when d.drg_code = 11 then 1
      when d.drg_code = 64 then 1
      when d.drg_code = 82 then 1
      when d.drg_code = 172 then 1
      when d.drg_code = 173 then 1
      when d.drg_code = 199 then 1
      when d.drg_code = 203 then 1
      when d.drg_code = 239 then 1

      when d.drg_code between 257 and 260 then 1
      when d.drg_code = 274 then 1
      when d.drg_code = 275 then 1
      when d.drg_code = 303 then 1
      when d.drg_code = 318 then 1
      when d.drg_code = 319 then 1

      when d.drg_code = 338 then 1
      when d.drg_code = 344 then 1
      when d.drg_code = 346 then 1
      when d.drg_code = 347 then 1
      when d.drg_code = 354 then 1
      when d.drg_code = 355 then 1
      when d.drg_code = 357 then 1
      when d.drg_code = 363 then 1
      when d.drg_code = 366 then 1

      when d.drg_code = 367 then 1
      when d.drg_code between 406 and 414 then 1
  else 0 end as CANCDRG

  /* Connective tissue */
  , case
      when d.drg_code = 240 then 1
      when d.drg_code = 241 then 1
  else 0 end as ARTHDRG

  /* Nutrition/metabolic */
  , case
      when d.drg_code between 296 and 298 then 1
  else 0 end as NUTRDRG

  /* Anemia */
  , case
      when d.drg_code = 395 then 1
      when d.drg_code = 396 then 1
      when d.drg_code = 574 then 1
  else 0 end as ANEMDRG

  /* Alcohol drug */
  , case
      when d.drg_code between 433 and 437 then 1
      when d.drg_code between 521 and 523 then 1
  else 0 end as ALCDRG

  /* Coagulation disorders */
  , case
      when d.drg_code = 397 then 1
  else 0 end as COAGDRG

  /* Hypertensive Complicated */
  , case
      when d.drg_code = 22 then 1
      when d.drg_code = 134 then 1
  else 0 end as HTNCXDRG

  /* Hypertensive Uncomplicated */
  , case
      when d.drg_code = 134 then 1
  else 0 end as HTNDRG

  /* Psychoses */
  , case
      when d.drg_code = 430 then 1
  else 0 end as PSYDRG

  /* Obesity */
  , case
      when d.drg_code = 288 then 1
  else 0 end as OBESEDRG

  /* Depressive Neuroses */
  , case
      when d.drg_code = 426 then 1
  else 0 end as DEPRSDRG

  from
  (
    select hadm_id, subject_id, drg_type, cast(drg_code as numeric) as drg_code from drgcodes where drg_type = 'HCFA'
  ) d
)
-- merge DRG groups together
, drggrp as
(
  select hadm_id, subject_id
, max(carddrg) as carddrg
, max(peridrg) as peridrg
, max(renaldrg) as renaldrg
, max(nervdrg) as nervdrg
, max(ceredrg) as ceredrg
, max(pulmdrg) as pulmdrg
, max(diabdrg) as diabdrg
, max(hypodrg) as hypodrg
, max(renfdrg) as renfdrg
, max(liverdrg) as liverdrg
, max(ulcedrg) as ulcedrg
, max(hivdrg) as hivdrg
, max(leukdrg) as leukdrg
, max(cancdrg) as cancdrg
, max(arthdrg) as arthdrg
, max(nutrdrg) as nutrdrg
, max(anemdrg) as anemdrg
, max(alcdrg) as alcdrg
, max(coagdrg) as coagdrg
, max(htncxdrg) as htncxdrg
, max(htndrg) as htndrg
, max(psydrg) as psydrg
, max(obesedrg) as obesedrg
, max(deprsdrg) as deprsdrg
from
(
  select d1.* from msdrg d1
  UNION
  select d1.* from hcfadrg d1
) d
group by d.hadm_id, d.subject_id
)
-- now merge these flags together to define elixhauser
-- most are straightforward.. but hypertension flags are a bit more complicated
select adm.subject_id, adm.hadm_id
, case
    when carddrg = 1 then 0 -- DRG filter

    when chf     = 1 then 1
    when htnwchf = 1 then 1
    when hhrwchf = 1 then 1
    when hhrwhrf = 1 then 1
  else 0 end as CONGESTIVE_HEART_FAILURE
, case
    when carddrg = 1 then 0 -- DRG filter
    when arythm = 1 then 1
  else 0 end as CARDIAC_ARRHYTHMIAS
, case
    when carddrg = 1 then 0
    when valve = 1 then 1
  else 0 end as VALVULAR_DISEASE
, case
    when carddrg = 1 or pulmdrg = 1 then 0
    when pulmcirc = 1 then 1
    else 0 end as PULMONARY_CIRCULATION
, case
    when peridrg  = 1 then 0
    when perivasc = 1 then 1
    else 0 end as PERIPHERAL_VASCULAR

-- we combine 'htn' and 'htncx' into 'HYPERTENSION'
-- note 'htn' (hypertension) is only 1 if 'htncx' (complicated hypertension) is 0
-- also if htncxdrg = 1, then htndrg = 1

-- In the original SAS code, it appears that:
--  HTN can be 1
--  HTNCX is set to 0 by DRGs
--  but HTN_C is still 1, because HTN is 1
-- so we have to do this complex addition.
,
case
  when
(
-- first hypertension
case
  when htndrg = 0 then 0
  when htn = 1 then 1
else 0 end
)
+
(
-- next complicated hypertension
case
    when htncx    = 1 and htncxdrg = 1 then 0

    when htnpreg  = 1 and htncxdrg = 1 then 0
    when htnwochf = 1 and (htncxdrg = 1 OR carddrg = 1) then 0
    when htnwchf  = 1 and htncxdrg = 1 then 0
    when htnwchf  = 1 and carddrg = 1 then 0
    when hrenworf = 1 and (htncxdrg = 1 or renaldrg = 1) then 0
    when hrenwrf  = 1 and htncxdrg = 1 then 0
    when hrenwrf  = 1 and renaldrg = 1 then 0
    when hhrwohrf = 1 and (htncxdrg = 1 or carddrg = 1 or renaldrg = 1) then 0
    when hhrwchf  = 1 and (htncxdrg = 1 or carddrg = 1 or renaldrg = 1) then 0
    when hhrwrf   = 1 and (htncxdrg = 1 or carddrg = 1 or renaldrg = 1) then 0
    when hhrwhrf  = 1 and (htncxdrg = 1 or carddrg = 1 or renaldrg = 1) then 0
    when ohtnpreg = 1 and (htncxdrg = 1 or carddrg = 1 or renaldrg = 1) then 0

    when htncx = 1 then 1
    when htnpreg = 1 then 1
    when htnwochf = 1 then 1
    when htnwchf = 1 then 1
    when hrenworf = 1 then 1
    when hrenwrf = 1 then 1
    when hhrwohrf = 1 then 1
    when hhrwchf = 1 then 1
    when hhrwrf = 1 then 1
    when hhrwhrf = 1 then 1
    when ohtnpreg = 1 then 1
  else 0 end
)
  > 0 then 1 else 0 end as HYPERTENSION

, case when ceredrg = 1 then 0 when para      = 1 then 1 else 0 end as PARALYSIS
, case when nervdrg = 1 then 0 when neuro     = 1 then 1 else 0 end as OTHER_NEUROLOGICAL
, case when pulmdrg = 1 then 0 when chrnlung  = 1 then 1 else 0 end as CHRONIC_PULMONARY
, case
    -- only the more severe comorbidity (complicated diabetes) is kept
    when diabdrg = 1 then 0
    when dmcx = 1 then 0
    when dm = 1 then 1
  else 0 end as DIABETES_UNCOMPLICATED
, case when diabdrg = 1 then 0 when dmcx    = 1 then 1 else 0 end as DIABETES_COMPLICATED
, case when hypodrg = 1 then 0 when hypothy = 1 then 1 else 0 end as HYPOTHYROIDISM
, case
    when renaldrg = 1 then 0
    when renlfail = 1 then 1
    when hrenwrf  = 1 then 1
    when hhrwrf   = 1 then 1
    when hhrwhrf  = 1 then 1
  else 0 end as RENAL_FAILURE

, case when liverdrg  = 1 then 0 when liver = 1 then 1 else 0 end as LIVER_DISEASE
, case when ulcedrg   = 1 then 0 when ulcer = 1 then 1 else 0 end as PEPTIC_ULCER
, case when hivdrg    = 1 then 0 when aids = 1 then 1 else 0 end as AIDS
, case when leukdrg   = 1 then 0 when lymph = 1 then 1 else 0 end as LYMPHOMA
, case when cancdrg   = 1 then 0 when mets = 1 then 1 else 0 end as METASTATIC_CANCER
, case
    when cancdrg = 1 then 0
    -- only the more severe comorbidity (metastatic cancer) is kept
    when mets = 1 then 0
    when tumor = 1 then 1
  else 0 end as SOLID_TUMOR
, case when arthdrg   = 1 then 0 when arth = 1 then 1 else 0 end as RHEUMATOID_ARTHRITIS
, case when coagdrg   = 1 then 0 when coag = 1 then 1 else 0 end as COAGULOPATHY
, case when nutrdrg   = 1
         OR obesedrg  = 1 then 0 when obese = 1 then 1 else 0 end as OBESITY
, case when nutrdrg   = 1 then 0 when wghtloss = 1 then 1 else 0 end as WEIGHT_LOSS
, case when nutrdrg   = 1 then 0 when lytes = 1 then 1 else 0 end as FLUID_ELECTROLYTE
, case when anemdrg   = 1 then 0 when bldloss = 1 then 1 else 0 end as BLOOD_LOSS_ANEMIA
, case when anemdrg   = 1 then 0 when anemdef = 1 then 1 else 0 end as DEFICIENCY_ANEMIAS
, case when alcdrg    = 1 then 0 when alcohol = 1 then 1 else 0 end as ALCOHOL_ABUSE
, case when alcdrg    = 1 then 0 when drug = 1 then 1 else 0 end as DRUG_ABUSE
, case when psydrg    = 1 then 0 when psych = 1 then 1 else 0 end as PSYCHOSES
, case when deprsdrg  = 1 then 0 when depress = 1 then 1 else 0 end as DEPRESSION


from admissions adm
left join eligrp eli
  on adm.hadm_id = eli.hadm_id and adm.subject_id = eli.subject_id
left join drggrp d
  on adm.hadm_id = d.hadm_id and adm.subject_id = d.subject_id
order by adm.hadm_id;
"""
        
    elix = q(query)
    elixhauser_vanwalraven = (
        0 * elix.aids + 
        0 * elix.alcohol_abuse +
        -2 * elix.blood_loss_anemia +
        7 * elix.congestive_heart_failure +
        3 * elix.chronic_pulmonary +
        3 * elix.coagulopathy +
        -2 * elix.deficiency_anemias +
        -3 * elix.depression +
        0 * elix.diabetes_complicated +
        0 * elix.diabetes_uncomplicated +
        -7 * elix.drug_abuse +
        5 * elix.fluid_electrolyte +
        0 * elix.hypertension +
        0 * elix.hypothyroidism +
        11 * elix.liver_disease +
        9 * elix.lymphoma +
        12 * elix.metastatic_cancer +
        6 * elix.other_neurological +
        -4 * elix.obesity +
        7 * elix.paralysis +
        2 * elix.peripheral_vascular +
        0 * elix.peptic_ulcer +
        0 * elix.psychoses +
        4 * elix.pulmonary_circulation +
        0 * elix.rheumatoid_arthritis +
        5 * elix.renal_failure +
        4 * elix.solid_tumor +
        -1 * elix.valvular_disease +
        6 * elix.weight_loss
    )

    elixhauser_sid29 = (
       0 * elix.aids +
      -2 * elix.alcohol_abuse +
      -2 * elix.blood_loss_anemia +
       9 * elix.congestive_heart_failure +
       3 * elix.chronic_pulmonary +
       9 * elix.coagulopathy +
       0 * elix.deficiency_anemias +
      -4 * elix.depression +
       0 * elix.diabetes_complicated +
      -1 * elix.diabetes_uncomplicated +
      -8 * elix.drug_abuse +
       9 * elix.fluid_electrolyte +
      -1 * elix.hypertension +
       0 * elix.hypothyroidism +
       5 * elix.liver_disease +
       6 * elix.lymphoma +
       13 * elix.metastatic_cancer +
       4 * elix.other_neurological +
      -4 * elix.obesity +
       3 * elix.paralysis +
       0 * elix.peptic_ulcer +
       4 * elix.peripheral_vascular +
      -4 * elix.psychoses +
       5 * elix.pulmonary_circulation +
       6 * elix.renal_failure +
       0 * elix.rheumatoid_arthritis +
       8 * elix.solid_tumor +
       0 * elix.valvular_disease +
       8 * elix.weight_loss
    )

    elixhauser_sid30 = (
       0 * elix.aids +
       0 * elix.alcohol_abuse +
      -3 * elix.blood_loss_anemia +
       8 * elix.cardiac_arrhythmias +
       9 * elix.congestive_heart_failure +
       3 * elix.chronic_pulmonary +
      12 * elix.coagulopathy +
       0 * elix.deficiency_anemias +
      -5 * elix.depression +
       1 * elix.diabetes_complicated +
       0 * elix.diabetes_uncomplicated +
     -11 * elix.drug_abuse +
      11 * elix.fluid_electrolyte +
      -2 * elix.hypertension +
       0 * elix.hypothyroidism +
       7 * elix.liver_disease +
       8 * elix.lymphoma +
      17 * elix.metastatic_cancer +
       5 * elix.other_neurological +
      -5 * elix.obesity +
       4 * elix.paralysis +
       0 * elix.peptic_ulcer +
       4 * elix.peripheral_vascular +
      -6 * elix.psychoses +
       5 * elix.pulmonary_circulation +
       7 * elix.renal_failure +
       0 * elix.rheumatoid_arthritis +
      10 * elix.solid_tumor +
       0 * elix.valvular_disease +
      10 * elix.weight_loss
    )    
    elix['vwr'] = elixhauser_vanwalraven
    elix['sid29'] = elixhauser_sid29
    elix['sid30'] = elixhauser_sid30
        
    return elix 

def oasis():
    # Severity of illness score        
    
    query = """
-- ITEMIDs used:

-- CAREVUE
--    723 as GCSVerbal
--    454 as GCSMotor
--    184 as GCSEyes

-- METAVISION
--    223900 GCS - Verbal Response
--    223901 GCS - Motor Response
--    220739 GCS - Eye Opening

-- The code combines the ITEMIDs into the carevue itemids, then pivots those
-- So 223900 is changed to 723, then the ITEMID 723 is pivoted to form GCSVerbal

-- Note:
--  The GCS for sedated patients is defaulted to 15 in this code.
--  This is in line with how the data is meant to be collected.
--  e.g., from the SAPS II publication:
--    For sedated patients, the Glasgow Coma Score before sedation was used.
--    This was ascertained either from interviewing the physician who ordered the sedation,
--    or by reviewing the patient's medical record.

with base as
(
  SELECT pvt.ICUSTAY_ID
  , pvt.charttime

  -- Easier names - note we coalesced Metavision and CareVue IDs below
  , max(case when pvt.itemid = 454 then pvt.valuenum else null end) as GCSMotor
  , max(case when pvt.itemid = 723 then pvt.valuenum else null end) as GCSVerbal
  , max(case when pvt.itemid = 184 then pvt.valuenum else null end) as GCSEyes

  -- If verbal was set to 0 in the below select, then this is an intubated patient
  , case
      when max(case when pvt.itemid = 723 then pvt.valuenum else null end) = 0
    then 1
    else 0
    end as EndoTrachFlag

  , ROW_NUMBER ()
          OVER (PARTITION BY pvt.ICUSTAY_ID ORDER BY pvt.charttime ASC) as rn

  FROM  (
  select l.ICUSTAY_ID
  -- merge the ITEMIDs so that the pivot applies to both metavision/carevue data
  , case
      when l.ITEMID in (723,223900) then 723
      when l.ITEMID in (454,223901) then 454
      when l.ITEMID in (184,220739) then 184
      else l.ITEMID end
    as ITEMID

  -- convert the data into a number, reserving a value of 0 for ET/Trach
  , case
      -- endotrach/vent is assigned a value of 0, later parsed specially
      when l.ITEMID = 723 and l.VALUE = '1.0 ET/Trach' then 0 -- carevue
      when l.ITEMID = 223900 and l.VALUE = 'No Response-ETT' then 0 -- metavision

      else VALUENUM
      end
    as VALUENUM
  , l.CHARTTIME
  from CHARTEVENTS l

  -- get intime for charttime subselection
  inner join icustays b
    on l.icustay_id = b.icustay_id

  -- Isolate the desired GCS variables
  where l.ITEMID in
  (
    -- 198 -- GCS
    -- GCS components, CareVue
    184, 454, 723
    -- GCS components, Metavision
    , 223900, 223901, 220739
  )
  -- Only get data for the first 24 hours
  and l.charttime between b.intime and b.intime + interval '1' day
  -- exclude rows marked as error
  and l.error IS DISTINCT FROM 1
  ) pvt
  group by pvt.ICUSTAY_ID, pvt.charttime
)
, gcs as (
  select b.*
  , b2.GCSVerbal as GCSVerbalPrev
  , b2.GCSMotor as GCSMotorPrev
  , b2.GCSEyes as GCSEyesPrev
  -- Calculate GCS, factoring in special case when they are intubated and prev vals
  -- note that the coalesce are used to implement the following if:
  --  if current value exists, use it
  --  if previous value exists, use it
  --  otherwise, default to normal
  , case
      -- replace GCS during sedation with 15
      when b.GCSVerbal = 0
        then 15
      when b.GCSVerbal is null and b2.GCSVerbal = 0
        then 15
      -- if previously they were intub, but they aren't now, do not use previous GCS values
      when b2.GCSVerbal = 0
        then
            coalesce(b.GCSMotor,6)
          + coalesce(b.GCSVerbal,5)
          + coalesce(b.GCSEyes,4)
      -- otherwise, add up score normally, imputing previous value if none available at current time
      else
            coalesce(b.GCSMotor,coalesce(b2.GCSMotor,6))
          + coalesce(b.GCSVerbal,coalesce(b2.GCSVerbal,5))
          + coalesce(b.GCSEyes,coalesce(b2.GCSEyes,4))
      end as GCS

  from base b
  -- join to itself within 6 hours to get previous value
  left join base b2
    on b.ICUSTAY_ID = b2.ICUSTAY_ID and b.rn = b2.rn+1 and b2.charttime > b.charttime - interval '6' hour
)
, gcs_final as (
  select gcs.*
  -- This sorts the data by GCS, so rn=1 is the the lowest GCS values to keep
  , ROW_NUMBER ()
          OVER (PARTITION BY gcs.ICUSTAY_ID
                ORDER BY gcs.GCS
               ) as IsMinGCS
  from gcs
)
, gcsfirstday as (
select ie.SUBJECT_ID, ie.HADM_ID, ie.ICUSTAY_ID
-- The minimum GCS is determined by the above row partition, we only join if IsMinGCS=1
, GCS as MinGCS
, coalesce(GCSMotor,GCSMotorPrev) as GCSMotor
, coalesce(GCSVerbal,GCSVerbalPrev) as GCSVerbal
, coalesce(GCSEyes,GCSEyesPrev) as GCSEyes
, EndoTrachFlag as EndoTrachFlag

-- subselect down to the cohort of eligible patients
from icustays ie
left join gcs_final gs
  on ie.ICUSTAY_ID = gs.ICUSTAY_ID and gs.IsMinGCS = 1
ORDER BY ie.ICUSTAY_ID
), ventsettings AS
(select
  icustay_id, charttime
  -- case statement determining whether it is an instance of mech vent
  , max(
    case
      when itemid is null or value is null then 0 -- can't have null values
      when itemid = 720 and value != 'Other/Remarks' THEN 1  -- VentTypeRecorded
      when itemid = 223848 and value != 'Other' THEN 1
      when itemid = 223849 then 1 -- ventilator mode
      when itemid = 467 and value = 'Ventilator' THEN 1 -- O2 delivery device == ventilator
      when itemid in
        (
        445, 448, 449, 450, 1340, 1486, 1600, 224687 -- minute volume
        , 639, 654, 681, 682, 683, 684,224685,224684,224686 -- tidal volume
        , 218,436,535,444,459,224697,224695,224696,224746,224747 -- High/Low/Peak/Mean/Neg insp force ("RespPressure")
        , 221,1,1211,1655,2000,226873,224738,224419,224750,227187 -- Insp pressure
        , 543 -- PlateauPressure
        , 5865,5866,224707,224709,224705,224706 -- APRV pressure
        , 60,437,505,506,686,220339,224700 -- PEEP
        , 3459 -- high pressure relief
        , 501,502,503,224702 -- PCV
        , 223,667,668,669,670,671,672 -- TCPCV
        , 224701 -- PSVlevel
        )
        THEN 1
      else 0
    end
    ) as MechVent
    , max(
      case
        -- initiation of oxygen therapy indicates the ventilation has ended
        when itemid = 226732 and value in
        (
          'Nasal cannula', -- 153714 observations
          'Face tent', -- 24601 observations
          'Aerosol-cool', -- 24560 observations
          'Trach mask ', -- 16435 observations
          'High flow neb', -- 10785 observations
          'Non-rebreather', -- 5182 observations
          'Venti mask ', -- 1947 observations
          'Medium conc mask ', -- 1888 observations
          'T-piece', -- 1135 observations
          'High flow nasal cannula', -- 925 observations
          'Ultrasonic neb', -- 9 observations
          'Vapomist' -- 3 observations
        ) then 1
        when itemid = 467 and value in
        (
          'Cannula', -- 278252 observations
          'Nasal Cannula', -- 248299 observations
          'None', -- 95498 observations
          'Face Tent', -- 35766 observations
          'Aerosol-Cool', -- 33919 observations
          'Trach Mask', -- 32655 observations
          'Hi Flow Neb', -- 14070 observations
          'Non-Rebreather', -- 10856 observations
          'Venti Mask', -- 4279 observations
          'Medium Conc Mask', -- 2114 observations
          'Vapotherm', -- 1655 observations
          'T-Piece', -- 779 observations
          'Hood', -- 670 observations
          'Hut', -- 150 observations
          'TranstrachealCat', -- 78 observations
          'Heated Neb', -- 37 observations
          'Ultrasonic Neb' -- 2 observations
        ) then 1
      else 0
      end
    ) as OxygenTherapy
    , max(
      case when itemid is null or value is null then 0
        -- extubated indicates ventilation event has ended
        when itemid = 640 and value = 'Extubated' then 1
        when itemid = 640 and value = 'Self Extubation' then 1
      else 0
      end
      )
      as Extubated
    , max(
      case when itemid is null or value is null then 0
        when itemid = 640 and value = 'Self Extubation' then 1
      else 0
      end
      )
      as SelfExtubated
from chartevents ce
where ce.value is not null
-- exclude rows marked as error
and ce.error IS DISTINCT FROM 1
and itemid in
(
    -- the below are settings used to indicate ventilation
      720, 223849 -- vent mode
    , 223848 -- vent type
    , 445, 448, 449, 450, 1340, 1486, 1600, 224687 -- minute volume
    , 639, 654, 681, 682, 683, 684,224685,224684,224686 -- tidal volume
    , 218,436,535,444,224697,224695,224696,224746,224747 -- High/Low/Peak/Mean ("RespPressure")
    , 221,1,1211,1655,2000,226873,224738,224419,224750,227187 -- Insp pressure
    , 543 -- PlateauPressure
    , 5865,5866,224707,224709,224705,224706 -- APRV pressure
    , 60,437,505,506,686,220339,224700 -- PEEP
    , 3459 -- high pressure relief
    , 501,502,503,224702 -- PCV
    , 223,667,668,669,670,671,672 -- TCPCV
    , 224701 -- PSVlevel

    -- the below are settings used to indicate extubation
    , 640 -- extubated

    -- the below indicate oxygen/NIV, i.e. the end of a mechanical vent event
    , 468 -- O2 Delivery Device#2
    , 469 -- O2 Delivery Mode
    , 470 -- O2 Flow (lpm)
    , 471 -- O2 Flow (lpm) #2
    , 227287 -- O2 Flow (additional cannula)
    , 226732 -- O2 Delivery Device(s)
    , 223834 -- O2 Flow

    -- used in both oxygen + vent calculation
    , 467 -- O2 Delivery Device
)
group by icustay_id, charttime
UNION
-- add in the extubation flags from procedureevents_mv
-- note that we only need the start time for the extubation
-- (extubation is always charted as ending 1 minute after it started)
select
  icustay_id, starttime as charttime
  , 0 as MechVent
  , 0 as OxygenTherapy
  , 1 as Extubated
  , case when itemid = 225468 then 1 else 0 end as SelfExtubated
from procedureevents_mv
where itemid in
(
  227194 -- "Extubation"
, 225468 -- "Unplanned Extubation (patient-initiated)"
, 225477 -- "Unplanned Extubation (non-patient initiated)"
)
), ventdurations as
(with vd0 as
(
  select
    icustay_id
    -- this carries over the previous charttime which had a mechanical ventilation event
    , case
        when MechVent=1 then
          LAG(CHARTTIME, 1) OVER (partition by icustay_id, MechVent order by charttime)
        else null
      end as charttime_lag
    , charttime
    , MechVent
    , OxygenTherapy
    , Extubated
    , SelfExtubated
  from ventsettings
)
, vd1 as
(
  select
      icustay_id
      , charttime_lag
      , charttime
      , MechVent
      , OxygenTherapy
      , Extubated
      , SelfExtubated

      -- if this is a mechanical ventilation event, we calculate the time since the last event
      , case
          -- if the current observation indicates mechanical ventilation is present
          -- calculate the time since the last vent event
          when MechVent=1 then
            CHARTTIME - charttime_lag
          else null
        end as ventduration

      , LAG(Extubated,1)
      OVER
      (
      partition by icustay_id, case when MechVent=1 or Extubated=1 then 1 else 0 end
      order by charttime
      ) as ExtubatedLag

      -- now we determine if the current mech vent event is a "new", i.e. they've just been intubated
      , case
        -- if there is an extubation flag, we mark any subsequent ventilation as a new ventilation event
          --when Extubated = 1 then 0 -- extubation is *not* a new ventilation event, the *subsequent* row is
          when
            LAG(Extubated,1)
            OVER
            (
            partition by icustay_id, case when MechVent=1 or Extubated=1 then 1 else 0 end
            order by charttime
            )
            = 1 then 1
          -- if patient has initiated oxygen therapy, and is not currently vented, start a newvent
          when MechVent = 0 and OxygenTherapy = 1 then 1
            -- if there is less than 8 hours between vent settings, we do not treat this as a new ventilation event
          when (CHARTTIME - charttime_lag) > interval '8' hour
            then 1
        else 0
        end as newvent
  -- use the staging table with only vent settings from chart events
  FROM vd0 ventsettings
)
, vd2 as
(
  select vd1.*
  -- create a cumulative sum of the instances of new ventilation
  -- this results in a monotonic integer assigned to each instance of ventilation
  , case when MechVent=1 or Extubated = 1 then
      SUM( newvent )
      OVER ( partition by icustay_id order by charttime )
    else null end
    as ventnum
  --- now we convert CHARTTIME of ventilator settings into durations
  from vd1
)
-- create the durations for each mechanical ventilation instance
select icustay_id
  -- regenerate ventnum so it's sequential
  , ROW_NUMBER() over (partition by icustay_id order by ventnum) as ventnum
  , min(charttime) as starttime
  , max(charttime) as endtime
  , extract(epoch from max(charttime)-min(charttime))/60/60 AS duration_hours
from vd2
group by icustay_id, ventnum
having min(charttime) != max(charttime)
-- patient had to be mechanically ventilated at least once
-- i.e. max(mechvent) should be 1
-- this excludes a frequent situation of NIV/oxygen before intub
-- in these cases, ventnum=0 and max(mechvent)=0, so they are ignored
and max(mechvent) = 1
order by icustay_id, ventnum
), ventfirstday AS
(select
  ie.subject_id, ie.hadm_id, ie.icustay_id
  -- if vd.icustay_id is not null, then they have a valid ventilation event
  -- in this case, we say they are ventilated
  -- otherwise, they are not
  , max(case
      when vd.icustay_id is not null then 1
    else 0 end) as vent
from icustays ie
left join ventdurations vd
  on ie.icustay_id = vd.icustay_id
  and
  (
    -- ventilation duration overlaps with ICU admission -> vented on admission
    (vd.starttime <= ie.intime and vd.endtime >= ie.intime)
    -- ventilation started during the first day
    OR (vd.starttime >= ie.intime and vd.starttime <= ie.intime + interval '1' day)
  )
group by ie.subject_id, ie.hadm_id, ie.icustay_id
order by ie.subject_id, ie.hadm_id, ie.icustay_id
), vitalsfirstday as
(SELECT pvt.subject_id, pvt.hadm_id, pvt.icustay_id

-- Easier names
, min(case when VitalID = 1 then valuenum else null end) as HeartRate_Min
, max(case when VitalID = 1 then valuenum else null end) as HeartRate_Max
, avg(case when VitalID = 1 then valuenum else null end) as HeartRate_Mean
, min(case when VitalID = 2 then valuenum else null end) as SysBP_Min
, max(case when VitalID = 2 then valuenum else null end) as SysBP_Max
, avg(case when VitalID = 2 then valuenum else null end) as SysBP_Mean
, min(case when VitalID = 3 then valuenum else null end) as DiasBP_Min
, max(case when VitalID = 3 then valuenum else null end) as DiasBP_Max
, avg(case when VitalID = 3 then valuenum else null end) as DiasBP_Mean
, min(case when VitalID = 4 then valuenum else null end) as MeanBP_Min
, max(case when VitalID = 4 then valuenum else null end) as MeanBP_Max
, avg(case when VitalID = 4 then valuenum else null end) as MeanBP_Mean
, min(case when VitalID = 5 then valuenum else null end) as RespRate_Min
, max(case when VitalID = 5 then valuenum else null end) as RespRate_Max
, avg(case when VitalID = 5 then valuenum else null end) as RespRate_Mean
, min(case when VitalID = 6 then valuenum else null end) as TempC_Min
, max(case when VitalID = 6 then valuenum else null end) as TempC_Max
, avg(case when VitalID = 6 then valuenum else null end) as TempC_Mean
, min(case when VitalID = 7 then valuenum else null end) as SpO2_Min
, max(case when VitalID = 7 then valuenum else null end) as SpO2_Max
, avg(case when VitalID = 7 then valuenum else null end) as SpO2_Mean
, min(case when VitalID = 8 then valuenum else null end) as Glucose_Min
, max(case when VitalID = 8 then valuenum else null end) as Glucose_Max
, avg(case when VitalID = 8 then valuenum else null end) as Glucose_Mean

FROM  (
  select ie.subject_id, ie.hadm_id, ie.icustay_id
  , case
    when itemid in (211,220045) and valuenum > 0 and valuenum < 300 then 1 -- HeartRate
    when itemid in (51,442,455,6701,220179,220050) and valuenum > 0 and valuenum < 400 then 2 -- SysBP
    when itemid in (8368,8440,8441,8555,220180,220051) and valuenum > 0 and valuenum < 300 then 3 -- DiasBP
    when itemid in (456,52,6702,443,220052,220181,225312) and valuenum > 0 and valuenum < 300 then 4 -- MeanBP
    when itemid in (615,618,220210,224690) and valuenum > 0 and valuenum < 70 then 5 -- RespRate
    when itemid in (223761,678) and valuenum > 70 and valuenum < 120  then 6 -- TempF, converted to degC in valuenum call
    when itemid in (223762,676) and valuenum > 10 and valuenum < 50  then 6 -- TempC
    when itemid in (646,220277) and valuenum > 0 and valuenum <= 100 then 7 -- SpO2
    when itemid in (807,811,1529,3745,3744,225664,220621,226537) and valuenum > 0 then 8 -- Glucose

    else null end as VitalID
      -- convert F to C
  , case when itemid in (223761,678) then (valuenum-32)/1.8 else valuenum end as valuenum

  from icustays ie
  left join chartevents ce
  on ie.subject_id = ce.subject_id and ie.hadm_id = ce.hadm_id and ie.icustay_id = ce.icustay_id
  and ce.charttime between ie.intime and ie.intime + interval '1' day
  -- exclude rows marked as error
  and ce.error IS DISTINCT FROM 1
  where ce.itemid in
  (
  -- HEART RATE
  211, --"Heart Rate"
  220045, --"Heart Rate"

  -- Systolic/diastolic

  51, --	Arterial BP [Systolic]
  442, --	Manual BP [Systolic]
  455, --	NBP [Systolic]
  6701, --	Arterial BP #2 [Systolic]
  220179, --	Non Invasive Blood Pressure systolic
  220050, --	Arterial Blood Pressure systolic

  8368, --	Arterial BP [Diastolic]
  8440, --	Manual BP [Diastolic]
  8441, --	NBP [Diastolic]
  8555, --	Arterial BP #2 [Diastolic]
  220180, --	Non Invasive Blood Pressure diastolic
  220051, --	Arterial Blood Pressure diastolic


  -- MEAN ARTERIAL PRESSURE
  456, --"NBP Mean"
  52, --"Arterial BP Mean"
  6702, --	Arterial BP Mean #2
  443, --	Manual BP Mean(calc)
  220052, --"Arterial Blood Pressure mean"
  220181, --"Non Invasive Blood Pressure mean"
  225312, --"ART BP mean"

  -- RESPIRATORY RATE
  618,--	Respiratory Rate
  615,--	Resp Rate (Total)
  220210,--	Respiratory Rate
  224690, --	Respiratory Rate (Total)


  -- SPO2, peripheral
  646, 220277,

  -- GLUCOSE, both lab and fingerstick
  807,--	Fingerstick Glucose
  811,--	Glucose (70-105)
  1529,--	Glucose
  3745,--	BloodGlucose
  3744,--	Blood Glucose
  225664,--	Glucose finger stick
  220621,--	Glucose (serum)
  226537,--	Glucose (whole blood)

  -- TEMPERATURE
  223762, -- "Temperature Celsius"
  676,	-- "Temperature C"
  223761, -- "Temperature Fahrenheit"
  678 --	"Temperature F"

  )
) pvt
group by pvt.subject_id, pvt.hadm_id, pvt.icustay_id
order by pvt.subject_id, pvt.hadm_id, pvt.icustay_id
), uofirstday as
(select
  -- patient identifiers
  ie.subject_id, ie.hadm_id, ie.icustay_id

  -- volumes associated with urine output ITEMIDs
  , sum(
      -- we consider input of GU irrigant as a negative volume
      case when oe.itemid = 227488 then -1*VALUE
      else VALUE end
  ) as UrineOutput
from icustays ie
-- Join to the outputevents table to get urine output
left join outputevents oe
-- join on all patient identifiers
on ie.subject_id = oe.subject_id and ie.hadm_id = oe.hadm_id and ie.icustay_id = oe.icustay_id
-- and ensure the data occurs during the first day
and oe.charttime between ie.intime and (ie.intime + interval '1' day) -- first ICU day
where itemid in
(
-- these are the most frequently occurring urine output observations in CareVue
40055, -- "Urine Out Foley"
43175, -- "Urine ."
40069, -- "Urine Out Void"
40094, -- "Urine Out Condom Cath"
40715, -- "Urine Out Suprapubic"
40473, -- "Urine Out IleoConduit"
40085, -- "Urine Out Incontinent"
40057, -- "Urine Out Rt Nephrostomy"
40056, -- "Urine Out Lt Nephrostomy"
40405, -- "Urine Out Other"
40428, -- "Urine Out Straight Cath"
40086,--	Urine Out Incontinent
40096, -- "Urine Out Ureteral Stent #1"
40651, -- "Urine Out Ureteral Stent #2"

-- these are the most frequently occurring urine output observations in MetaVision
226559, -- "Foley"
226560, -- "Void"
226561, -- "Condom Cath"
226584, -- "Ileoconduit"
226563, -- "Suprapubic"
226564, -- "R Nephrostomy"
226565, -- "L Nephrostomy"
226567, --	Straight Cath
226557, -- R Ureteral Stent
226558, -- L Ureteral Stent
227488, -- GU Irrigant Volume In
227489  -- GU Irrigant/Urine Volume Out
)
group by ie.subject_id, ie.hadm_id, ie.icustay_id
order by ie.subject_id, ie.hadm_id, ie.icustay_id
), surgflag as
(
  select ie.icustay_id
    , max(case
        when lower(curr_service) like '%surg%' then 1
        when curr_service = 'ORTHO' then 1
    else 0 end) as surgical
  from icustays ie
  left join services se
    on ie.hadm_id = se.hadm_id
    and se.transfertime < ie.intime + interval '1' day
  group by ie.icustay_id
)
, cohort as
(
select ie.subject_id, ie.hadm_id, ie.icustay_id
      , ie.intime
      , ie.outtime
      , adm.deathtime
      , cast(ie.intime as timestamp) - cast(adm.admittime as timestamp) as PreICULOS
      , floor( ( cast(ie.intime as date) - cast(pat.dob as date) ) / 365.242 ) as age
      , gcs.mingcs
      , vital.heartrate_max
      , vital.heartrate_min
      , vital.meanbp_max
      , vital.meanbp_min
      , vital.resprate_max
      , vital.resprate_min
      , vital.tempc_max
      , vital.tempc_min
      , vent.vent as mechvent
      , uo.urineoutput

      , case
          when adm.ADMISSION_TYPE = 'ELECTIVE' and sf.surgical = 1
            then 1
          when adm.ADMISSION_TYPE is null or sf.surgical is null
            then null
          else 0
        end as ElectiveSurgery

      -- age group
      , case
        when ( ( cast(ie.intime as date) - cast(pat.dob as date) ) / 365.242 ) <= 1 then 'neonate'
        when ( ( cast(ie.intime as date) - cast(pat.dob as date) ) / 365.242 ) <= 15 then 'middle'
        else 'adult' end as ICUSTAY_AGE_GROUP

      -- mortality flags
      , case
          when adm.deathtime between ie.intime and ie.outtime
            then 1
          when adm.deathtime <= ie.intime -- sometimes there are typographical errors in the death date
            then 1
          when adm.dischtime <= ie.outtime and adm.discharge_location = 'DEAD/EXPIRED'
            then 1
          else 0 end
        as ICUSTAY_EXPIRE_FLAG
      , adm.hospital_expire_flag
from icustays ie
inner join admissions adm
  on ie.hadm_id = adm.hadm_id
inner join patients pat
  on ie.subject_id = pat.subject_id
left join surgflag sf
  on ie.icustay_id = sf.icustay_id
-- join to custom tables to get more data....
left join gcsfirstday gcs
  on ie.icustay_id = gcs.icustay_id
left join vitalsfirstday vital
  on ie.icustay_id = vital.icustay_id
left join uofirstday uo
  on ie.icustay_id = uo.icustay_id
left join ventfirstday vent
  on ie.icustay_id = vent.icustay_id
)
, scorecomp as
(
select co.subject_id, co.hadm_id, co.icustay_id
, co.ICUSTAY_AGE_GROUP
, co.icustay_expire_flag
, co.hospital_expire_flag

-- Below code calculates the component scores needed for OASIS
, case when preiculos is null then null
     when preiculos < '0 0:10:12' then 5
     when preiculos < '0 4:57:00' then 3
     when preiculos < '1 0:00:00' then 0
     when preiculos < '12 23:48:00' then 1
     else 2 end as preiculos_score
,  case when age is null then null
      when age < 24 then 0
      when age <= 53 then 3
      when age <= 77 then 6
      when age <= 89 then 9
      when age >= 90 then 7
      else 0 end as age_score
,  case when mingcs is null then null
      when mingcs <= 7 then 10
      when mingcs < 14 then 4
      when mingcs = 14 then 3
      else 0 end as gcs_score
,  case when heartrate_max is null then null
      when heartrate_max > 125 then 6
      when heartrate_min < 33 then 4
      when heartrate_max >= 107 and heartrate_max <= 125 then 3
      when heartrate_max >= 89 and heartrate_max <= 106 then 1
      else 0 end as heartrate_score
,  case when meanbp_min is null then null
      when meanbp_min < 20.65 then 4
      when meanbp_min < 51 then 3
      when meanbp_max > 143.44 then 3
      when meanbp_min >= 51 and meanbp_min < 61.33 then 2
      else 0 end as meanbp_score
,  case when resprate_min is null then null
      when resprate_min <   6 then 10
      when resprate_max >  44 then  9
      when resprate_max >  30 then  6
      when resprate_max >  22 then  1
      when resprate_min <  13 then 1 else 0
      end as resprate_score
,  case when tempc_max is null then null
      when tempc_max > 39.88 then 6
      when tempc_min >= 33.22 and tempc_min <= 35.93 then 4
      when tempc_max >= 33.22 and tempc_max <= 35.93 then 4
      when tempc_min < 33.22 then 3
      when tempc_min > 35.93 and tempc_min <= 36.39 then 2
      when tempc_max >= 36.89 and tempc_max <= 39.88 then 2
      else 0 end as temp_score
,  case when UrineOutput is null then null
      when UrineOutput < 671.09 then 10
      when UrineOutput > 6896.80 then 8
      when UrineOutput >= 671.09
       and UrineOutput <= 1426.99 then 5
      when UrineOutput >= 1427.00
       and UrineOutput <= 2544.14 then 1
      else 0 end as UrineOutput_score
,  case when mechvent is null then null
      when mechvent = 1 then 9
      else 0 end as mechvent_score
,  case when ElectiveSurgery is null then null
      when ElectiveSurgery = 1 then 0
      else 6 end as electivesurgery_score


-- The below code gives the component associated with each score
-- This is not needed to calculate OASIS, but provided for user convenience.
-- If both the min/max are in the normal range (score of 0), then the average value is stored.
, preiculos
, age
, mingcs as gcs
,  case when heartrate_max is null then null
      when heartrate_max > 125 then heartrate_max
      when heartrate_min < 33 then heartrate_min
      when heartrate_max >= 107 and heartrate_max <= 125 then heartrate_max
      when heartrate_max >= 89 and heartrate_max <= 106 then heartrate_max
      else (heartrate_min+heartrate_max)/2 end as heartrate
,  case when meanbp_min is null then null
      when meanbp_min < 20.65 then meanbp_min
      when meanbp_min < 51 then meanbp_min
      when meanbp_max > 143.44 then meanbp_max
      when meanbp_min >= 51 and meanbp_min < 61.33 then meanbp_min
      else (meanbp_min+meanbp_max)/2 end as meanbp
,  case when resprate_min is null then null
      when resprate_min <   6 then resprate_min
      when resprate_max >  44 then resprate_max
      when resprate_max >  30 then resprate_max
      when resprate_max >  22 then resprate_max
      when resprate_min <  13 then resprate_min
      else (resprate_min+resprate_max)/2 end as resprate
,  case when tempc_max is null then null
      when tempc_max > 39.88 then tempc_max
      when tempc_min >= 33.22 and tempc_min <= 35.93 then tempc_min
      when tempc_max >= 33.22 and tempc_max <= 35.93 then tempc_max
      when tempc_min < 33.22 then tempc_min
      when tempc_min > 35.93 and tempc_min <= 36.39 then tempc_min
      when tempc_max >= 36.89 and tempc_max <= 39.88 then tempc_max
      else (tempc_min+tempc_max)/2 end as temp
,  UrineOutput
,  mechvent
,  ElectiveSurgery
from cohort co
)
, score as
(
select s.*
    , coalesce(age_score,0)
    + coalesce(preiculos_score,0)
    + coalesce(gcs_score,0)
    + coalesce(heartrate_score,0)
    + coalesce(meanbp_score,0)
    + coalesce(resprate_score,0)
    + coalesce(temp_score,0)
    + coalesce(urineoutput_score,0)
    + coalesce(mechvent_score,0)
    + coalesce(electivesurgery_score,0)
    as OASIS
from scorecomp s
)
select
  subject_id, hadm_id, icustay_id
  , ICUSTAY_AGE_GROUP
  , hospital_expire_flag
  , icustay_expire_flag
  , OASIS
  -- Calculate the probability of in-hospital mortality
  , 1 / (1 + exp(- (-6.1746 + 0.1275*(OASIS) ))) as OASIS_PROB
  , age, age_score
  , preiculos, preiculos_score
  , gcs, gcs_score
  , heartrate, heartrate_score
  , meanbp, meanbp_score
  , resprate, resprate_score
  , temp, temp_score
  , urineoutput, UrineOutput_score
  , mechvent, mechvent_score
  , electivesurgery, electivesurgery_score
from score
order by icustay_id;

""" 
    return q(query)