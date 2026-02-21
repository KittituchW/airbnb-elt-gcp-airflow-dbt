{{ config(materialized='table', schema='silver') }}

-- Normalized G01 with stable 5-digit LGA key (replace original column)
with src as (
  select *
  from {{ source('bronze','r_d_2016census_g01_nsw_lga') }}
)
select
  -- replace original column with normalized 5-digit numeric version
  lpad(regexp_replace("LGA_CODE_2016"::text, '\D', '', 'g'), 5, '0') as "LGA_CODE_2016",

  "Tot_P_M","Tot_P_F","Tot_P_P",
  "Age_0_4_yr_M","Age_0_4_yr_F","Age_0_4_yr_P",
  "Age_5_14_yr_M","Age_5_14_yr_F","Age_5_14_yr_P",
  "Age_15_19_yr_M","Age_15_19_yr_F","Age_15_19_yr_P",
  "Age_20_24_yr_M","Age_20_24_yr_F","Age_20_24_yr_P",
  "Age_25_34_yr_M","Age_25_34_yr_F","Age_25_34_yr_P",
  "Age_35_44_yr_M","Age_35_44_yr_F","Age_35_44_yr_P",
  "Age_45_54_yr_M","Age_45_54_yr_F","Age_45_54_yr_P",
  "Age_55_64_yr_M","Age_55_64_yr_F","Age_55_64_yr_P",
  "Age_65_74_yr_M","Age_65_74_yr_F","Age_65_74_yr_P",
  "Age_75_84_yr_M","Age_75_84_yr_F","Age_75_84_yr_P",
  "Age_85ov_M","Age_85ov_F","Age_85ov_P",
  "Counted_Census_Night_home_M","Counted_Census_Night_home_F","Counted_Census_Night_home_P",
  "Count_Census_Nt_Ewhere_Aust_M","Count_Census_Nt_Ewhere_Aust_F","Count_Census_Nt_Ewhere_Aust_P",
  "Indigenous_psns_Aboriginal_M","Indigenous_psns_Aboriginal_F","Indigenous_psns_Aboriginal_P",
  "Indig_psns_Torres_Strait_Is_M","Indig_psns_Torres_Strait_Is_F","Indig_psns_Torres_Strait_Is_P",
  "Indig_Bth_Abor_Torres_St_Is_M","Indig_Bth_Abor_Torres_St_Is_F","Indig_Bth_Abor_Torres_St_Is_P",
  "Indigenous_P_Tot_M","Indigenous_P_Tot_F","Indigenous_P_Tot_P",
  "Birthplace_Australia_M","Birthplace_Australia_F","Birthplace_Australia_P",
  "Birthplace_Elsewhere_M","Birthplace_Elsewhere_F","Birthplace_Elsewhere_P",
  "Lang_spoken_home_Eng_only_M","Lang_spoken_home_Eng_only_F","Lang_spoken_home_Eng_only_P",
  "Lang_spoken_home_Oth_Lang_M","Lang_spoken_home_Oth_Lang_F","Lang_spoken_home_Oth_Lang_P",
  "Australian_citizen_M","Australian_citizen_F","Australian_citizen_P",
  "Age_psns_att_educ_inst_0_4_M","Age_psns_att_educ_inst_0_4_F","Age_psns_att_educ_inst_0_4_P",
  "Age_psns_att_educ_inst_5_14_M","Age_psns_att_educ_inst_5_14_F","Age_psns_att_educ_inst_5_14_P",
  "Age_psns_att_edu_inst_15_19_M","Age_psns_att_edu_inst_15_19_F","Age_psns_att_edu_inst_15_19_P",
  "Age_psns_att_edu_inst_20_24_M","Age_psns_att_edu_inst_20_24_F","Age_psns_att_edu_inst_20_24_P",
  "Age_psns_att_edu_inst_25_ov_M","Age_psns_att_edu_inst_25_ov_F","Age_psns_att_edu_inst_25_ov_P",
  "High_yr_schl_comp_Yr_12_eq_M","High_yr_schl_comp_Yr_12_eq_F","High_yr_schl_comp_Yr_12_eq_P",
  "High_yr_schl_comp_Yr_11_eq_M","High_yr_schl_comp_Yr_11_eq_F","High_yr_schl_comp_Yr_11_eq_P",
  "High_yr_schl_comp_Yr_10_eq_M","High_yr_schl_comp_Yr_10_eq_F","High_yr_schl_comp_Yr_10_eq_P",
  "High_yr_schl_comp_Yr_9_eq_M","High_yr_schl_comp_Yr_9_eq_F","High_yr_schl_comp_Yr_9_eq_P",
  "High_yr_schl_comp_Yr_8_belw_M","High_yr_schl_comp_Yr_8_belw_F","High_yr_schl_comp_Yr_8_belw_P",
  "High_yr_schl_comp_D_n_g_sch_M","High_yr_schl_comp_D_n_g_sch_F","High_yr_schl_comp_D_n_g_sch_P",
  "Count_psns_occ_priv_dwgs_M","Count_psns_occ_priv_dwgs_F","Count_psns_occ_priv_dwgs_P",
  "Count_Persons_other_dwgs_M","Count_Persons_other_dwgs_F","Count_Persons_other_dwgs_P"
from src
