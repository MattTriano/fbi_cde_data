{{ config(materialized='view', schema='nibrs_std') }}

with offender_seg as (
    select
        nibrs_year::smallint                                         as nibrs_year,
        trim(segment)                                                as segment,
        trim(state_code)                                             as state_code,
        trim(ori)                                                    as ori,
        trim(incident_no)                                            as incident_no,
        {{ parse_nibrs_date('incident_date') }}                      as incident_date,
        nullif(
            regexp_replace(offender_seq_no, '[\s\n#]', '', 'g'), ''
        )::smallint                                                  as offender_seq_no,
        {{ trim_and_stdize_nulls('offender_age') }}::smallint        as offender_age,
        {{ trim_and_stdize_nulls('offender_sex') }}                  as offender_sex,
        {{ trim_and_stdize_nulls('offender_race') }}                 as offender_race

    from {{ source('nibrs_raw', 'offender') }}
)

select distinct on (nibrs_year, ori, incident_no, incident_date, offender_seq_no)
    *
from offender_seg
