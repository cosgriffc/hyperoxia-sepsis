SET search_path = 'mimiciii';

DROP MATERIALIZED VIEW IF EXISTS hyperoxia_sepsis_initial CASCADE;

CREATE MATERIALIZED VIEW hyperoxia_sepsis_initial AS
WITH co AS
(
SELECT icu.subject_id, icu.hadm_id, icu.icustay_id, adm.hospital_expire_flag
, EXTRACT(EPOCH FROM outtime - intime)/60.0/60.0/24.0 as icu_length_of_stay
, EXTRACT('epoch' from icu.intime - pat.dob) / 60.0 / 60.0 / 24.0 / 365.242 as age
, CASE
        WHEN adm.deathtime BETWEEN icu.intime and icu.outtime
            THEN 1
        -- sometimes there are typographical errors in the death date, so check before intime
        WHEN adm.deathtime <= icu.intime
            THEN 1
        WHEN adm.dischtime <= icu.outtime
            AND adm.discharge_location = 'DEAD/EXPIRED'
            THEN 1
        ELSE 0
        END AS icustay_expire_flag

FROM icustays icu
INNER JOIN patients pat
  ON icu.subject_id = pat.subject_id
LEFT JOIN admissions adm
    ON icu.hadm_id = adm.hadm_id
)
, ventdur AS
(
SELECT vd.icustay_id, vd.duration_hours
FROM ventdurations vd
)
SELECT
  co.subject_id, co.hadm_id, co.icustay_id, co.icu_length_of_stay
  , co.age
  , COALESCE(vd.duration_hours, 0) AS vent_duration_hours
  , CASE
        WHEN co.icu_length_of_stay < (4/24) THEN 1
    ELSE 0 END
    AS exclusion_los
  , CASE
        WHEN co.age < 16 then 1
    ELSE 0 END
    AS exclusion_age
  , co.icustay_expire_flag
  , co.hospital_expire_flag
  , CASE
  		WHEN COALESCE(vd.duration_hours, 0) = 0 THEN 1
    ELSE 0 END
    AS exclusion_no_vent
  , s.sepsis
FROM co 
LEFT JOIN ventdur vd ON co.icustay_id = vd.icustay_id
LEFT JOIN martin_sepsis s on co.hadm_id = s.hadm_id;

DROP MATERIALIZED VIEW IF EXISTS hyperoxia_sepsis_step2 CASCADE;
CREATE MATERIALIZED VIEW hyperoxia_sepsis_step2 AS
SELECT subject_id, hadm_id, icustay_id, age, icu_length_of_stay 
FROM hyperoxia_sepsis_initial
WHERE exclusion_age = 0
AND exclusion_los = 0
AND exclusion_no_vent = 0
AND sepsis = 1;

DROP MATERIALIZED VIEW IF EXISTS hypoxic_spo2 CASCADE;
CREATE MATERIALIZED VIEW hypoxic_spo2 AS
(
	SELECT ce.* 
    FROM chartevents ce
    WHERE (ce.itemid = 646 OR ce.itemid = 220277)
    AND ce.valuenum < 94
    AND ce.icustay_id IN (SELECT inc.icustay_id FROM hyperoxia_sepsis_step2 inc)
)



DROP MATERIALIZED VIEW IF EXISTS hyperoxia_sepsis_step3 CASCADE;
CREATE MATERIALIZED VIEW hyperoxia_sepsis_step3 AS
WITH inc AS
(
SELECT subject_id, hadm_id, icustay_id, age, icu_length_of_stay 
FROM hyperoxia_sepsis_step2
)
, ventdur AS
(
SELECT vd.icustay_id, vd.starttime, vd.endtime
FROM ventdurations vd
)
, hypox AS
(
SELECT hp.*
FROM hypoxic_spo2 hp
)
SELECT inc.subject_id, inc.hadm_id, inc.icustay_id, inc.age, inc.icu_length_of_stay 
  , CASE
  		WHEN 
        (
            SELECT count(s.itemid)
            FROM hypox s
            WHERE s.charttime BETWEEN ventdur.starttime AND ventdur.endtime
            AND s.icustay_id = inc.icustay_id
        ) > 2 THEN 1
        ELSE 0 END
        AS exlcusion_hypoxia
FROM inc
LEFT JOIN ventdur ON inc.icustay_id = ventdur.icustay_id
LIMIT 1000;



