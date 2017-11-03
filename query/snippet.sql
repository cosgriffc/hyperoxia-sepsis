  , CASE
  		WHEN (SELECT count(hpox.itemid) FROM hypoxic_spo2 hpox WHERE hpox.charttime BETWEEN vd.starttime AND vd.endtime) > 2 THEN 1
        ELSE 0 END
        AS exlcusion_hypoxia
        
        
        
        
        
  		WHEN (SELECT count(vpd.icustay_id) FROM vasopressordurations vpd WHERE co.icustay_id = vpd.icustay_id AND vpd.duration_hours > 0) > 0 THEN 1