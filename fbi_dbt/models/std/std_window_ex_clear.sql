{{ config(materialized='view', schema='nibrs_std') }}

with ex_clear_seg as (
    select
        nibrs_year::smallint                                   as nibrs_year,
        trim(segment)                                          as segment,
        trim(state_code)                                       as state_code,
        trim(ori)                                              as ori,
        trim(incident_no)                                      as incident_no,
        {{ parse_nibrs_date('incident_date') }}                as incident_date,
        {{ trim_and_stdize_nulls('report_date_ind') }}         as report_date_ind,
        {{ trim_and_stdize_nulls('incident_hour') }}::smallint as incident_hour,
        {{ trim_and_stdize_nulls('total_offenses') }}          as total_offenses,
        {{ trim_and_stdize_nulls('total_victims') }}           as total_victims,
        {{ trim_and_stdize_nulls('total_offenders') }}         as total_offenders,
        {{ trim_and_stdize_nulls('total_arrestees') }}         as total_arrestees,
        {{ trim_and_stdize_nulls('city_submission') }}         as city_submission,
        {{ trim_and_stdize_nulls('ex_cleared') }}              as ex_cleared,
        {{ parse_nibrs_date('ex_cleared_date') }}              as ex_cleared_date,
        {{ trim_and_stdize_nulls('ucr_offense1') }}            as ucr_offense1,
        {{ trim_and_stdize_nulls('ucr_offense2') }}            as ucr_offense2,
        {{ trim_and_stdize_nulls('ucr_offense3') }}            as ucr_offense3,
        {{ trim_and_stdize_nulls('ucr_offense4') }}            as ucr_offense4,
        {{ trim_and_stdize_nulls('ucr_offense5') }}            as ucr_offense5,
        {{ trim_and_stdize_nulls('ucr_offense6') }}            as ucr_offense6,
        {{ trim_and_stdize_nulls('ucr_offense7') }}            as ucr_offense7,
        {{ trim_and_stdize_nulls('ucr_offense8') }}            as ucr_offense8,
        {{ trim_and_stdize_nulls('ucr_offense9') }}            as ucr_offense9,
        {{ trim_and_stdize_nulls('ucr_offense10') }}           as ucr_offense10
    from {{ source('nibrs_raw', 'window_ex_clear') }}
)

select *
from ex_clear_seg
