SET search_path = 'mimiciii';

/*CREATE MATERIALIZED VIEW hyperoxia_sepsis_initial AS*/
WITH co AS
(
SELECT icu.subject_id, icu.hadm_id, icu.icustay_id, adm.hospital_expire_flag
, EXTRACT(EPOCH FROM outtime - intime)/60.0/60.0/24.0 as icu_length_of_stay
, EXTRACT('epoch' from icu.intime - pat.dob) / 60.0 / 60.0 / 24.0 / 365.242 as age
, pat.gender
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
SELECT vd.icustay_id, vd.duration_hours, vd.starttime, vd.endtime
FROM ventdurations vd
)
, icudeath AS
(
SELECT co.hadm_id, SUM(co.icustay_expire_flag) AS icu_expire
FROM co
GROUP BY co.hadm_id
)
, ini AS
(
SELECT
  co.subject_id, co.hadm_id, co.icustay_id, co.icu_length_of_stay
  , co.age, co.gender
  , CASE 
   		WHEN (SELECT COALESCE(SUM(vd.duration_hours), 0) FROM ventdur vd WHERE co.icustay_id = vd.icustay_id GROUP BY vd.icustay_id) >= 4 THEN 0
        ELSE 1 END
        AS exclusion_no_vent
  , CASE
        WHEN co.icu_length_of_stay <= 0.1666667 THEN 1
    ELSE 0 END
    AS exclusion_los
  , CASE
        WHEN co.age < 16 then 1
    ELSE 0 END
    AS exclusion_age
  , co.icustay_expire_flag
  , co.hospital_expire_flag
   , CASE WHEN ((SELECT icudeath.icu_expire FROM icudeath WHERE icudeath.hadm_id = co.hadm_id) = 0) THEN 0
	ELSE 1 END
    AS exclude_icu_expire
  , s.angus AS sepsis
FROM co 
LEFT JOIN angus_sepsis s on co.hadm_id = s.hadm_id
)
, notExclude AS
(
SELECT hs.subject_id, hs.hadm_id, hs.icustay_id
, hs.age, hs.gender, hs.icustay_expire_flag, hs.icu_length_of_stay
, hs.hospital_expire_flag 
FROM ini hs
WHERE exclusion_age = 0
AND exclusion_los = 0
AND exclusion_no_vent = 0
AND exclude_icu_expire = 0
AND sepsis = 1
)
, hypoxic_spo2 AS
(
	SELECT ce.*
    FROM sp02 ce
    LEFT JOIN notExclude ne
    ON ce.icustay_id = ne.icustay_id
    WHERE ce.valuenum < 94

)
, desat AS
(
SELECT hs.icustay_id
  , CASE
  		WHEN 
        (
            SELECT count(s.itemid)
            FROM hypoxic_spo2 s
            WHERE s.charttime BETWEEN ventdur.starttime AND ventdur.endtime
            AND s.icustay_id = hs.icustay_id
        ) > 2 THEN 1
        ELSE 0 END
        AS hypoxia_flag
FROM hypoxic_spo2 hs
LEFT JOIN ventdur ON hs.icustay_id = ventdur.icustay_id
)
SELECT ini.subject_id, ini.hadm_id, ini.icustay_id, ini.age, ini.gender, ini.icu_length_of_stay, ini.exclude_icu_expire, ini.hospital_expire_flag
, ini.exclusion_los AS los_flag
FROM ini
LEFT JOIN desat ON desat.icustay_id = ini.icustay_id
LIMIT 10;

