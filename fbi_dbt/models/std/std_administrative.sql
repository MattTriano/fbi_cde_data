{{ config(materialized='view', schema='nibrs_std') }}

with admin_seg as (
    select
        nibrs_year::smallint                                              as nibrs_year,
        trim(segment)                                                     as segment,
        trim(state_code)                                                  as state_code,
        trim(ori)                                                         as ori,
        trim(incident_no)                                                 as incident_no,
        {{ parse_nibrs_date('incident_date') }}                           as incident_date,
        {{ trim_and_stdize_nulls('report_date_ind') }}                    as report_date_ind,
        {{ trim_and_stdize_nulls('incident_hour', '[\s\n-]') }}::smallint as incident_hour,
        {{ trim_and_stdize_nulls('total_offenses') }}::smallint           as total_offenses,
        {{ trim_and_stdize_nulls('total_victims') }}::smallint            as total_victims,
        {{ trim_and_stdize_nulls('total_offenders') }}::smallint          as total_offenders,
        {{ trim_and_stdize_nulls('total_arrestees') }}::smallint          as total_arrestees,
        {{ trim_and_stdize_nulls('city_submission') }}                    as city_submission,
        {{ trim_and_stdize_nulls('ex_cleared') }}                         as ex_cleared,
        {{ parse_nibrs_date('ex_cleared_date') }}                         as ex_cleared_date
    from {{ source('nibrs_raw', 'administrative') }}
)

select *
from admin_seg
