{{ config(materialized='view', schema='nibrs_std') }}

with vt_2015_records as (
    select
        nibrs_year,
        segment,
        state_code,
        ori,
        incident_no,
        incident_date,
        arrestee_seq_no,
        (
            arrest_transaction_no
            || arrest_date
            || arrest_type
            || multi_arrestee_seg_ind
            || ucr_arrest_offense_code
            || arrestee_weapon1
            || arrestee_weapon1_automatic
            || arrestee_weapon2
            || arrestee_weapon2_automatic
            || arrestee_age
            || arrestee_sex
            || arrestee_race
            || arrestee_ethnicity
            || arrestee_residency
            || arrestee_under_18_disp
        ) as concat_col
    from {{ source('nibrs_raw', 'arrestee') }}
    where nibrs_year = 2015 and state_code = '44'
),
non_vt_2015_records as (
    select *
    from {{ source('nibrs_raw', 'arrestee') }}
    where not (nibrs_year = 2015 and state_code = '44')
),
fixed_vt_records as (
    select
        nibrs_year,
        segment,
        state_code,
        ori,
        incident_no,
        incident_date,
        arrestee_seq_no,
        substring(concat_col, 0, 13) as arrest_transaction_no,
        substring(concat_col, 14, 8) as arrest_date,
        substring(concat_col, 22, 1) as arrest_type,
        substring(concat_col, 23, 1) as multi_arrestee_seg_ind,
        substring(concat_col, 24, 3) as ucr_arrest_offense_code,
        substring(concat_col, 27, 2) as arrestee_weapon1,
        substring(concat_col, 29, 1) as arrestee_weapon1_automatic,
        substring(concat_col, 30, 2) as arrestee_weapon2,
        substring(concat_col, 32, 1) as arrestee_weapon2_automatic,
        substring(concat_col, 33, 2) as arrestee_age,
        substring(concat_col, 35, 1) as arrestee_sex,
        substring(concat_col, 36, 1) as arrestee_race,
        substring(concat_col, 37, 1) as arrestee_ethnicity,
        substring(concat_col, 38, 1) as arrestee_residency,
        substring(concat_col, 39, 1) as arrestee_under_18_disp
    from vt_2015_records
),
fixed_raw_arrestee_records as (
    select * from non_vt_2015_records
    union all
    select * from fixed_vt_records
),
arrestee_seg as (
    select
        nibrs_year::smallint                                         as nibrs_year,
        trim(segment)                                                as segment,
        trim(state_code)                                             as state_code,
        trim(ori)                                                    as ori,
        trim(incident_no)                                            as incident_no,
        {{ parse_nibrs_date('incident_date') }}                      as incident_date,
        {{ trim_and_stdize_nulls('arrestee_seq_no') }}::smallint     as arrestee_seq_no,
        {{ trim_and_stdize_nulls('arrest_transaction_no') }}         as arrest_transaction_no,
        {{ parse_nibrs_date('arrest_date') }}                        as arrest_date,
        {{ trim_and_stdize_nulls('arrest_type') }}                   as arrest_type,
        {{ trim_and_stdize_nulls('multi_arrestee_seg_ind') }}        as multi_arrestee_seg_ind,
        {{ trim_and_stdize_nulls('ucr_arrest_offense_code') }}       as ucr_arrest_offense_code,
        {{ trim_and_stdize_nulls('arrestee_weapon1') }}              as arrestee_weapon1,
        {{ trim_and_stdize_nulls('arrestee_weapon1_automatic') }}    as arrestee_weapon1_automatic,
        {{ trim_and_stdize_nulls('arrestee_weapon2') }}              as arrestee_weapon2,
        {{ trim_and_stdize_nulls('arrestee_weapon2_automatic') }}    as arrestee_weapon2_automatic,
        {{ trim_and_stdize_nulls('arrestee_age') }}                  as arrestee_age,
        {{ trim_and_stdize_nulls('arrestee_sex') }}                  as arrestee_sex,
        {{ trim_and_stdize_nulls('arrestee_race') }}                 as arrestee_race,
        {{ trim_and_stdize_nulls('arrestee_ethnicity') }}            as arrestee_ethnicity,
        {{ trim_and_stdize_nulls('arrestee_residency') }}            as arrestee_residency,
        {{ trim_and_stdize_nulls('arrestee_under_18_disp') }}        as arrestee_under_18_disp
    from fixed_raw_arrestee_records
)

select *
from arrestee_seg
