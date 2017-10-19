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
  , CASE 
   		WHEN (SELECT COALESCE(SUM(vd.duration_hours), 0) FROM ventdur vd WHERE co.icustay_id = vd.icustay_id GROUP BY vd.icustay_id) < 4 THEN 1
        ELSE 0 END
        AS exclusion_no_vent
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
  , s.sepsis
FROM co 
LEFT JOIN martin_sepsis s on co.hadm_id = s.hadm_id;

DROP MATERIALIZED VIEW IF EXISTS hyperoxia_sepsis_step2 CASCADE;
CREATE MATERIALIZED VIEW hyperoxia_sepsis_step2 AS
SELECT hs.subject_id, hs.hadm_id, hs.icustay_id
, hs.age, hs.icustay_expire_flag, hs.icu_length_of_stay
, hs.hospital_expire_flag 
FROM hyperoxia_sepsis_initial hs
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
);

DROP MATERIALIZED VIEW IF EXISTS hyperoxia_sepsis_step3 CASCADE;
CREATE MATERIALIZED VIEW hyperoxia_sepsis_step3 AS
WITH inc AS
(
SELECT hs.subject_id, hs.hadm_id, hs.icustay_id
    , hs.age, hs.icu_length_of_stay, hs.icustay_expire_flag
    , hs.hospital_expire_flag 
FROM hyperoxia_sepsis_step2 hs
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
, desat AS
(
SELECT inc.icustay_id
  , CASE
  		WHEN 
        (
            SELECT count(s.itemid)
            FROM hypox s
            WHERE s.charttime BETWEEN ventdur.starttime AND ventdur.endtime
            AND s.icustay_id = inc.icustay_id
        ) > 2 THEN 1
        ELSE 0 END
        AS hypoxia_flag
FROM inc
LEFT JOIN ventdur ON inc.icustay_id = ventdur.icustay_id
)
, icudeath AS
(
SELECT hs.hadm_id, SUM(hs.icustay_expire_flag) AS icu_expire
FROM hyperoxia_sepsis_step2 hs
GROUP BY hs.hadm_id
)
SELECT inc.subject_id, inc.hadm_id, inc.icustay_id, inc.age, inc.icu_length_of_stay, inc.icustay_expire_flag, inc.hospital_expire_flag
	, CASE
    	WHEN
        (
            (SELECT SUM(desat.hypoxia_flag) FROM desat WHERE inc.icustay_id = desat.icustay_id GROUP BY desat.icustay_id) > 0
        ) THEN 1
        ELSE 0 END
        AS exclude_hypoxia_flag
     , CASE
     	WHEN
        ((SELECT icudeath.icu_expire FROM icudeath WHERE icudeath.hadm_id = inc.hadm_id) = 0) THEN 0
            ELSE 1 END
            AS exclude_icu_expire
FROM inc;

DROP MATERIALIZED VIEW IF EXISTS hyperoxia_sepsis_step4 CASCADE;
CREATE MATERIALIZED VIEW hyperoxia_sepsis_step4 AS
SELECT hs.subject_id, hs.hadm_id, hs.icustay_id
, hs.age, hs.icu_length_of_stay
, hs.hospital_expire_flag 
FROM hyperoxia_sepsis_step3 hs
WHERE hs.exclude_hypoxia_flag = 0
AND hs.exclude_icu_expire = 0;

DROP MATERIALIZED VIEW IF EXISTS hyperoxia_sepsis_covariates;
CREATE MATERIALIZED VIEW hyperoxia_sepsis_covariates AS
WITH co AS
(
SELECT hs.subject_id, hs.hadm_id, hs.icustay_id
, hs.age, hs.icu_length_of_stay
, hs.hospital_expire_flag
FROM hyperoxia_sepsis_step4 hs
)
, ventdur AS
(
SELECT vd.icustay_id, sum(vd.duration_hours) AS vent_duration
FROM ventdurations vd
GROUP BY icustay_id
)
, oasisscore AS
(
SELECT oa.icustay_id, oa.oasis
FROM oasis oa
)
, elixhauser AS
(
SELECT el.hadm_id
    , (COALESCE(el.congestive_heart_failure, 0) + 
       COALESCE(el.cardiac_arrhythmias, 0) + 
       COALESCE(el.valvular_disease, 0) +
       COALESCE(el.pulmonary_circulation, 0) +
       COALESCE(el.peripheral_vascular, 0) +
       COALESCE(el.hypertension, 0) +
       COALESCE(el.paralysis, 0) +
       COALESCE(el.other_neurological, 0) +
       COALESCE(el.chronic_pulmonary, 0) +
       COALESCE(el.diabetes_uncomplicated, 0) +
       COALESCE(el.diabetes_complicated, 0) +
       COALESCE(el.hypothyroidism, 0) +
       COALESCE(el.renal_failure, 0) +
       COALESCE(el.liver_disease, 0) +
       COALESCE(el.peptic_ulcer, 0) +
       COALESCE(el.aids, 0) +
       COALESCE(el.lymphoma, 0) +
       COALESCE(el.metastatic_cancer, 0) +
       COALESCE(el.solid_tumor, 0) +
       COALESCE(el.rheumatoid_arthritis, 0) +
       COALESCE(el.coagulopathy, 0) +
       COALESCE(el.obesity, 0) +
       COALESCE(el.weight_loss, 0) +
       COALESCE(el.fluid_electrolyte, 0) +
       COALESCE(el.blood_loss_anemia, 0) +
       COALESCE(el.deficiency_anemias, 0) +
       COALESCE(el.alcohol_abuse, 0) +
       COALESCE(el.drug_abuse, 0) +
       COALESCE(el.psychoses, 0) +
       COALESCE(el.depression, 0)) AS score
    FROM elixhauser_ahrq_no_drg_all_icd el
)
SELECT  co.subject_id, co.hadm_id, co.icustay_id, co.age
, ventdur.vent_duration
, oasisscore.oasis
, elixhauser.score as elixhauser
, CASE
	WHEN (SELECT count(vpd.icustay_id) FROM vasopressordurations vpd WHERE co.icustay_id = vpd.icustay_id AND vpd.duration_hours > 0) > 0 THEN 1
    ELSE 0 END
    AS vasopressor_flag
, co.hospital_expire_flag
FROM co
LEFT JOIN ventdur on co.icustay_id = ventdur.icustay_id
LEFT JOIN oasisscore on co.icustay_id = oasisscore.icustay_id
LEFT JOIN elixhauser on co.hadm_id = elixhauser.hadm_id
ORDER BY co.subject_id;

DROP MATERIALIZED VIEW IF EXISTS hyperoxia_sepsis_spO2;
CREATE MATERIALIZED VIEW hyperoxia_sepsis_spO2 AS
WITH co AS
(
SELECT hs.subject_id, hs.hadm_id, hs.icustay_id
FROM hyperoxia_sepsis_covariates hs
)
, ventdur AS
(
SELECT vd.icustay_id, vd.starttime, vd.endtime
FROM ventdurations vd
)
SELECT ce.row_id, ce.subject_id, ce.hadm_id, ce.icustay_id, ce.charttime, ventdur.starttime, ventdur.endtime, ce.valuenum
FROM chartevents ce
INNER JOIN ventdur ON ce.icustay_id = ventdur.icustay_id
INNER JOIN co ON ce.icustay_id = co.icustay_id AND ce.charttime BETWEEN ventdur.starttime AND ventdur.endtime
WHERE ce.icustay_id = co.icustay_id
AND (ce.itemid = 646 OR ce.itemid = 220277);

DROP MATERIALIZED VIEW IF EXISTS hyperoxia_sepsis_covariates2;
CREATE MATERIALIZED VIEW hyperoxia_sepsis_covariates2 AS
SELECT co.subject_id, co.hadm_id, co.icustay_id, co.age, co.vent_duration, co.oasis, co.elixhauser, co.vasopressor_flag, co.hospital_expire_flag
, CASE WHEN (p.dod BETWEEN a.dischtime AND (a.dischtime + '365 day'::interval)) THEN 1
	ELSE 0 END AS one_year_expire
FROM hyperoxia_sepsis_covariates co
LEFT JOIN patients p ON co.subject_id = p.subject_id
LEFT JOIN admissions a ON co.hadm_id = a.hadm_id;