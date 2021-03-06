---
title: 'Hyperoxia & Sepsis Mortality in the ICU'
author: "C.V. Cosgriff, MD/MPH Student, Harvard Chan School"
output:
  html_document: default
---

## Setup

```r
library(tidyverse)
library(lubridate)
library(stringr)
library(RPostgreSQL)
library(MIMICbook) #JRaffa's MIMIC code book
library(epitools) # For calculating OR's
library(tableone) # Generating Table 1
library(sjstats) # Some useful stats tools e.g. hoslem_gof
library(sjPlot)

# These libs are for logistic regression diagnostics
library(rms)
library(quantreg)
library(pROC)
library(separationplot)
library(heatmapFit)
# End of logistic libs

library(rstudioapi)
library(dslabs) # Dr. Irrizary's library 

ds_theme_set() # Dr. Irrizary's theme

# Database access; you'll need to change 10.8.0.1 to the server you are using
# and you will also need to run ths in RStudio as I use the RStudio API to ask
# for a password. Edit this definition for your environment as need.
drv <- dbDriver("PostgreSQL")
mimic <- dbConnect(drv, dbname = "mimic", host = "10.8.0.1", port = 5432,
                   user = "mimicuser", 
                   password = askForPassword("MIMIC Password: "))
```


## Cohort Design
The initial cohort table was began in sql with the following code (adapted from
Alistair Johnson's code). 


```sql
SET search_path = 'mimiciii';

DROP MATERIALIZED VIEW IF EXISTS hyperoxia_sepsis_baseCohort CASCADE;
CREATE MATERIALIZED VIEW hyperoxia_sepsis_baseCohort AS

WITH icuStaysBase AS 
(
SELECT icu.subject_id, icu.hadm_id, icu.icustay_id, adm.hospital_expire_flag
, EXTRACT(EPOCH FROM outtime - intime)/60.0/60.0/24.0 as icu_length_of_stay
, EXTRACT('epoch' from icu.intime - pat.dob) / 60.0 / 60.0 / 24.0 / 365.242 as age
, pat.gender, adm.ethnicity
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
INNER JOIN patients pat /* Inner join because we only need patients who had an ICU stay */
  ON icu.subject_id = pat.subject_id
INNER JOIN admissions adm /* Inner join because we only care about admissions which have an ICU stay */
    ON icu.hadm_id = adm.hadm_id
)
, ventDurations AS
(
SELECT vd.icustay_id, vd.duration_hours
FROM ventdurations vd
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
SELECT b.*, vd.duration_hours AS vent_dur, s.angus
, CASE
	WHEN b.icu_length_of_stay <= 0.1666667 THEN 1
    ELSE 0 END
    AS exclusion_los
, CASE
	WHEN b.age < 16 then 1
    ELSE 0 END
    AS exclude_age
, el.score AS elixhauser, oa.oasis
, CASE
	WHEN (SELECT count(vpd.icustay_id) FROM vasopressordurations vpd WHERE b.icustay_id = vpd.icustay_id AND vpd.duration_hours > 0) > 0 THEN 1
    ELSE 0 END
    AS vasopressor_flag
FROM icuStaysBase b
INNER JOIN oasis oa on b.icustay_id = oa.icustay_id
INNER JOIN elixhauser el on b.hadm_id = el.hadm_id
INNER JOIN ventDurations vd ON b.icustay_id = vd.icustay_id /* Our cohort it will only include people who were ventilated */
INNER JOIN angus_sepsis s on b.hadm_id = s.hadm_id;
```

Next, the base cohort can be pulled from the database.

```r
cohort.df <- dbGetQuery(mimic, 
                        "SELECT * FROM mimiciii.hyperoxia_sepsis_baseCohort")
```

This cohort contains all of the ICU stays for which ventilation data existed
and each row actually corresponds to a ventilation duration recording. 
We will now build the subject cohort that will eventually be used for analysis
and as we go we will tidy the data.

First lets define a function to check the number of subjects in our cohort 
since currently our table is simply all ICU stays for the base cohort of 
ventilated patients.


```r
subjectCount <- function(cohort) {
  cohort %>% distinct(subject_id) %>%
    summarise(count = n())
}

subjectCount(cohort.df)
```

```
##   count
## 1 23256
```

We are starting with 23,256 patients. We have to be careful about inclusion and
exclusion because some flags apply to hospital admissions, and others apply to
ICU stays specifically; in the end we want to exclude any subject who has met
exclusion criteria at some point. We also have elected to not deal with 
integrating data across multiple admissions and so we'll first remove subjects
with multiple admissions. This step has to be done first because if we exclude 
an admission because, for example, sepsis didn't occur during it, we may 
inadverdently get rid of the 'second' admission; we'll have kept a patient with
more than one admission by accident.


```r
adm.count <- cohort.df %>% select(subject_id, hadm_id) %>% 
  distinct(hadm_id, .keep_all = TRUE) %>% group_by(subject_id) %>% 
  summarise (count = n()) %>% 
  arrange(-count)
adm.count <- adm.count %>% filter(count == 1)
cohort.df <- cohort.df %>% filter(subject_id %in% adm.count$subject_id)
subjectCount(cohort.df)
```

```
##   count
## 1 21335
```

Next we'll exclude patients with multiple ICU stays. Although we don't want to
throw away data, this is useful for the following reason: consider a patient
goes to the ICU, gets blasted with O2, and makes it to the floor, and then 
returns to the ICU the next day and dies. Since we are excluding patients who 
die in the ICU we will throw away this initial survival. Because all of this
challenges analysis (lots of competing risk) for this work we will focus on
single admission, single ICU stay patients.


```r
icu.count <- cohort.df %>% select(subject_id, icustay_id) %>% 
  distinct(icustay_id, .keep_all = TRUE) %>% group_by(subject_id) %>% 
  summarise(count = n()) %>% arrange(-count)
icu.count <- icu.count %>% filter(count == 1)
cohort.df <- cohort.df %>% filter(subject_id %in% icu.count$subject_id)
subjectCount(cohort.df)
```

```
##   count
## 1 20784
```


Now we have a data frame composed of all of the ventilation instances for patients
with 1 hadm_id and 1 icustay_id. Other covariates shouldn't vary with respect
to ventilation duration, and so we can now simply collapse the ventilation 
durations into single observations. If we do this right, the subject count
shouldn't change.


```r
cohort.tidy.df <- cohort.df %>% group_by(subject_id, hadm_id, icustay_id, gender, 
                                    ethnicity) %>%
  summarise(age = mean(age), icu_los = mean(icu_length_of_stay), 
            vent_duration = sum(vent_dur), angus = mean(angus), 
            elixhauser = mean(elixhauser), oasis = max(oasis), 
            exclusion_icu_expire = any(icustay_expire_flag == 1),
            exclusion_age = any(exclude_age == 1), exclusion_los = any(exclusion_los),
            vasopressor_flag = any(vasopressor_flag == 1), 
            hospital_expire = any(hospital_expire_flag == 1)) %>% ungroup() %>%
  select(subject_id, icustay_id, age, gender, ethnicity, elixhauser, oasis, vent_duration, 
         angus, vasopressor_flag, hospital_expire, exclusion_icu_expire, exclusion_age, 
         exclusion_los)
subjectCount(cohort.tidy.df)
```

```
## # A tibble: 1 x 1
##   count
##   <int>
## 1 20784
```

We can now take a tidied dataset and exclude patients based on inclusion/exclusion
criteria.


```r
cohort.tidy.df <- cohort.tidy.df %>% filter(angus == 1)
subjectCount(cohort.tidy.df)
```

```
## # A tibble: 1 x 1
##   count
##   <int>
## 1  6592
```

We have 6,592 patients in the dataset with sepsis by angus criteria.


```r
cohort.tidy.df <- cohort.tidy.df %>% filter(exclusion_age == 0)
subjectCount(cohort.tidy.df)
```

```
## # A tibble: 1 x 1
##   count
##   <int>
## 1  6465
```

Of thoese 6,465 meet age criteria of 16.


```r
cohort.tidy.df <- cohort.tidy.df %>% filter(exclusion_icu_expire == 0)
subjectCount(cohort.tidy.df)
```

```
## # A tibble: 1 x 1
##   count
##   <int>
## 1  4776
```

Of those 4,776 survived their ICU stay.


```r
cohort.tidy.df <- cohort.tidy.df %>% filter(exclusion_los == 0)
subjectCount(cohort.tidy.df)
```

```
## # A tibble: 1 x 1
##   count
##   <int>
## 1  4776
```

And of that set 4,776 had a long enough length of stay (none at this point had
to be excluded.)

Although we didn't genrate a flag for it initially, we also want to exclude
patients which do not have at least four hours of ventilation time in order for
them to have to have gotten enough exposure and in order to provide enough data.


```r
cohort.tidy.df <- cohort.tidy.df %>% filter(vent_duration > 4)
subjectCount(cohort.tidy.df)
```

```
## # A tibble: 1 x 1
##   count
##   <int>
## 1  4585
```

This leaves us with 4,585 subjects. We now have our baseline cohort, although
we have not taken into account any desaturations. This however cannot be done
until we have developed a profile of their oxygenation exposures. 

Before continuing let us just check if there is any missig data in our current
dataset.


```r
sapply(cohort.tidy.df, function(x) sum(is.na(x)))
```

```
##           subject_id           icustay_id                  age 
##                    0                    0                    0 
##               gender            ethnicity           elixhauser 
##                    0                    0                    0 
##                oasis        vent_duration                angus 
##                    0                    0                    0 
##     vasopressor_flag      hospital_expire exclusion_icu_expire 
##                    0                    0                    0 
##        exclusion_age        exclusion_los 
##                    0                    0
```

None, great! On to exposures.

## Exposure Data

The goal is to generate a table containing all of the necessary SpO2 data. This
is computationally challenging because not only are their millions of SpO2 data,
but we also only want to only grab SpO2 data that falls within our patients
ventilation times.

We begin by using the following code to generate a table containing all of our 
SpO2 data.


```sql
SET search_path = 'mimiciii';
DROP MATERIALIZED VIEW IF EXISTS spO2 CASCADE;
CREATE MATERIALIZED VIEW spO2 AS
SELECT ce.row_id,
    ce.subject_id,
    ce.hadm_id,
    ce.icustay_id,
    ce.itemid,
    ce.charttime,
    ce.storetime,
    ce.cgid,
    ce.value,
    ce.valuenum,
    ce.valueuom,
    ce.warning,
    ce.error,
    ce.resultstatus,
    ce.stopped
FROM mimiciii.chartevents ce
WHERE ce.itemid = 646 OR ce.itemid = 220277;
```

An attempt was then made to run the following query.


```sql
SET search_path = 'mimiciii';
DROP MATERIALIZED VIEW IF EXISTS spO2_vent CASCADE;
CREATE MATERIALIZED VIEW spO2_vent AS
SELECT ce.row_id,
    ce.subject_id,
    ce.hadm_id,
    ce.icustay_id,
    ce.itemid,
    ce.charttime,
    vd.starttime,
    vd.endtime,
    ce.storetime,
    ce.cgid,
    ce.value,
    ce.valuenum,
    ce.valueuom,
    ce.warning,
    ce.error,
    ce.resultstatus,
    ce.stopped
FROM mimiciii.spO2 ce
INNER JOIN ventdurations vd ON ce.icustay_id = vd.icustay_id
WHERE vd.starttime <= ce.charttime AND vd.endtime >= ce.charttime;
```

However, this query would not run on my server (which has numerous VMs) because
of a lack of memory. To handle this I elected to collect all of the SpO2 data
into a vector from which I could then filter it down to the current cohort.

An attempt was made to pulled the necessary data right into a vector, but this
also failed (as such, eval was set to false for knitr). This was not due to
memory, but because my server is remote and the transfer was too large for it
to be usable.


```r
spO2 <- dbGetQuery(mimic, 
                        "SELECT * FROM mimiciii.spO2")
```

The full SpO2 table was exported and gziped. It was then stored locally in 
./data which is not in the repo. If you wish to recreate this project you'll
need to run the SQL code above (the one to generate mimiciii.spO2) and then
export that file, gzip it, and store it in "./data/" where it can be accessed
locally.

`data.table` functions were used because of the size of the data.

**Note to reader:** You will see that this was coded on windows and so I used the
full path to zcat in order to unzip the file on the fly. You will have to modify
this for it to run on your machine. If you are on Mac or Linux simply put zcat.


```r
spO2 <- as.data.frame(data.table::fread(paste0("\"C:\\Program Files\\Git\\bin\\sh.exe\" zcat < ", "./data/spO2.csv.gz"), showProgress = FALSE))
```

Because we used fread we'll need to re-add the column names.


```r
spO2.columns <- c("row_id", "subject_id", "hadm_id", "icustay_id", "item_id", 
              "charttime", "storetime", "cgid", 
              "value", "valuenum", "valueom", "warning", "error", "resultstatus",
              "stopped")
colnames(spO2) <- spO2.columns
```

And now we can filter out everyone from spO2 who isn't in our cohort.


```r
spO2 <- spO2 %>% filter(spO2$subject_id %in% cohort.tidy.df$subject_id) %>%
  select(subject_id, icustay_id, charttime, valuenum)
```

Now to get this data such that we only have SpO2 data relating to vents, we will
begin by reloading Alistair's vent duration table.


```r
vent.df <- dbGetQuery(mimic, "SELECT * FROM mimiciii.ventdurations") %>%
  filter(icustay_id %in% cohort.tidy.df$icustay_id) %>% 
  select(icustay_id, starttime, endtime)
```

However, it was at this time that I discovered a bug in RPostgresSQL which causes
it to throw away the time associated with a POSIX datetime. There are discussions
about it on stack overflow, but needless to say it can be solved by directly 
downloading the CSV and loading it locally, and so once again this is what was
done.


```r
vent.df <- as.data.frame(data.table::fread(paste0("\"C:\\Program Files\\Git\\bin\\sh.exe\" zcat < ", "./data/ventdurations.csv.gz"), showProgress = FALSE)) 
vent.columns <- c("icustay_id", "ventnum", "starttime", "endtime", "duration_hours")
colnames(vent.df) <- vent.columns
vent.df <- vent.df %>%
  filter(icustay_id %in% cohort.tidy.df$icustay_id) %>% 
  select(icustay_id, starttime, endtime)
```

Before joining, lets just make sure we have our data columns as dates.

```r
vent.df$starttime <- as_datetime(vent.df$starttime, tz = "EST")
vent.df$endtime <- as_datetime(vent.df$endtime, tz = "EST")
spO2$charttime <- as_datetime(spO2$charttime, tz = "EST")
```



```r
spO2.merge <- inner_join(spO2, vent.df, by = c("icustay_id"))
```

And now to only include values that have the charttime between the start and end
of the ventilation period.


```r
spO2.merge <- spO2.merge %>% filter(charttime >= starttime & charttime <= endtime)
```


With that we now have all of our SpO2 data. Now we'll need to summarise it in a
way that it becomes an exposure. Before we can do that though we need to check it
for missing values.


```r
sapply(spO2.merge, function(x) sum(is.na(x)))
```

```
## subject_id icustay_id  charttime   valuenum  starttime    endtime 
##          0          0          0        320          0          0
```

We are missing a meager 320 single recordings of 897,879, and so these can be
disregarded. 


```r
spO2.merge <- spO2.merge %>% na.omit()
```

We'll now write a function that can sum up all of the times a patient has SpO2
recordings >98%. We will do this by using the time between chart times as an 
interval and assume that at each recording point the SpO2 constant over the 
interval. This of course an approximation, but because we are applying it the same
way to every patient it shouldn't introduce bias.


```r
hyperoxicTime <- function(times, value) {
  icustay.spO2 <- data.frame(time = times, value = value)
  icustay.spO2 <- icustay.spO2[order(icustay.spO2$time),]
  intervals <- as.numeric(icustay.spO2$time - lag(icustay.spO2$time))
  intervals <- lead(intervals)
  intervals[is.na(intervals)] <- 0
  
  # Intervals are usually ~60, we will tolerate as many as three missed entries
  # or other errors; beyond that the result will be tossed.
  intervals[intervals >= 3*60] <- 0
  
  icustay.spO2 <- icustay.spO2 %>% mutate(duration = intervals)
  o2levels <- icustay.spO2 %>% group_by(value) %>% 
    summarise(time_at = as.numeric(sum(duration))) %>% arrange(desc(value))
  o2levels[is.na(o2levels)] <- 0
  colnames(o2levels)[1] <- "satValue"
  dur.100 <- as.numeric(o2levels[o2levels$satValue == 100, 2])
  dur.99 <-  as.numeric(o2levels[o2levels$satValue == 99, 2])
  
  # Because some patients will never have any values at 99 or 100, we need to
  # return a 0 for these times; however they will be null. We need to clean 
  # this up.
  if (is.null(dur.100) | length(dur.100) <= 0) {
    dur.100 <- 0
  }
    if (is.null(dur.99) | length(dur.99) <= 0) {
    dur.99 <- 0
  }
  if (is.na(dur.100)) {
    dur.100 <- 0
  }
  if (is.na(dur.99)) {
    dur.99 <- 0
  }
  
  # Now we can calculate the hyperoxic time and return it
  hyperoxic.time <- (dur.100 + dur.99) / 60
  return(hyperoxic.time)
}
```

And now we can generate our hyperoxia profiles.


```r
hyperoxic.profile <- spO2.merge %>% 
  group_by(icustay_id) %>% 
  summarise(hyperoxic_duration = as.numeric(hyperoxicTime(charttime, valuenum)))
```

With that, we now have an exposure variable that we can use to add to our cohort.


```r
hs.df <- inner_join(cohort.tidy.df, hyperoxic.profile, by = c("icustay_id"))
```

*Of note, we have not yet excluded hypoxic patients; having done that in the*
*prelim, I'm going to try doing this without doing that and see what happens.*

Because we don't know what an important amount of hyperoxic time will be, we will
categorize the trend into quartiles and assign groups based on these quartiles.



```r
groups <- 4
hs.df$hyperoxic_quartile <- 0
hs.df$hyperoxic_group <- 0

hs.df$hyperoxic_quartile <- with(hs.df, cut(hyperoxic_duration, 
                                breaks = quantile(hyperoxic_duration, 
                                                  probs = seq(0,1, by = (1/groups)), 
                                                  na.rm=TRUE), 
                                include.lowest = TRUE))

hyperoxia.levels <- levels(hs.df$hyperoxic_quartile)

for (i in 1:groups) {
  hs.df$hyperoxic_group[hs.df$hyperoxic_quartile == hyperoxia.levels[i]] <- i
}
```



