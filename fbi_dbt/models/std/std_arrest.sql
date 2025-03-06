{{ config(materialized='view', schema='nibrs_std') }}

with arrest_seg as (
    select
        nibrs_year::smallint                                      as nibrs_year,
        trim(segment)                                             as segment,
        trim(state_code)                                          as state_code,
        trim(ori)                                                 as ori,
        trim(incident_no)                                         as incident_no,
        {{ parse_nibrs_date('arrest_date') }}                     as arrest_date,
        {{ trim_and_stdize_nulls('arrestee_seq_no') }}            as arrestee_seq_no,
        {{ trim_and_stdize_nulls('city_submission') }}            as city_submission,
        {{ trim_and_stdize_nulls('arrest_type') }}                as arrest_type,
        {{ trim_and_stdize_nulls('ucr_offense') }}                as ucr_offense,
        {{ trim_and_stdize_nulls('arrestee_weapon1') }}           as arrestee_weapon1,
        {{ trim_and_stdize_nulls('arrestee_weapon1_automatic') }} as arrestee_weapon1_automatic,
        {{ trim_and_stdize_nulls('arrestee_weapon2') }}           as arrestee_weapon2,
        {{ trim_and_stdize_nulls('arrestee_weapon2_automatic') }} as arrestee_weapon2_automatic,
        {{ trim_and_stdize_nulls('arrestee_age') }}               as arrestee_age,
        {{ trim_and_stdize_nulls('arrestee_sex') }}               as arrestee_sex,
        {{ trim_and_stdize_nulls('arrestee_race') }}              as arrestee_race,
        {{ trim_and_stdize_nulls('arrestee_ethnicity') }}         as arrestee_ethnicity,
        {{ trim_and_stdize_nulls('arrestee_residency') }}         as arrestee_residency,
        {{ trim_and_stdize_nulls('arrestee_under_18_disp') }}     as arrestee_under_18_disp
    from {{ source('nibrs_raw', 'arrest') }}
)

select *
from arrest_seg
