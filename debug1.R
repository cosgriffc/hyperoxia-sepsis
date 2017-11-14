spo2.profiled <- spo2.tidy.df %>%
  group_by(icustay_id) %>% 
  summarize(hyperoxic_dur = hyperoxicTime(spO2Profile(charttime, valuenum)))


