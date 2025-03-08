{{ config(materialized='view', schema='nibrs_std') }}

with offense_seg as (
    select distinct on (nibrs_year, ori, incident_no, ucr_offense_code, incident_date)
        nibrs_year::smallint                                        as nibrs_year,
        trim(segment)                                               as segment,
        trim(state_code)                                            as state_code,
        trim(ori)                                                   as ori,
        trim(incident_no)                                           as incident_no,
        {{ parse_nibrs_date('incident_date') }}                     as incident_date,
        {{ trim_and_stdize_nulls('ucr_offense_code') }}             as ucr_offense_code,
        {{ trim_and_stdize_nulls('attempted_or_completed') }}       as attempted_or_completed,
        {{ trim_and_stdize_nulls('offender1_suspected_of_using') }} as offender1_suspected_of_using,
        {{ trim_and_stdize_nulls('offender2_suspected_of_using') }} as offender2_suspected_of_using,
        {{ trim_and_stdize_nulls('offender3_suspected_of_using') }} as offender3_suspected_of_using,
        {{ trim_and_stdize_nulls('location_type') }}                as location_type,
        {{ trim_and_stdize_nulls('premises_entered') }}::smallint   as premises_entered,
        {{ trim_and_stdize_nulls('forced_entry') }}                 as forced_entry,
        {{ trim_and_stdize_nulls('criminal_activity_type1') }}      as criminal_activity_type1,
        {{ trim_and_stdize_nulls('criminal_activity_type2') }}      as criminal_activity_type2,
        {{ trim_and_stdize_nulls('criminal_activity_type3') }}      as criminal_activity_type3,
        {{ trim_and_stdize_nulls('weapon_or_force_type1') }}        as weapon_or_force_type1,
        automatic_ind1 = 'A' as automatic_ind1,
        {{ trim_and_stdize_nulls('weapon_or_force_type2') }}        as weapon_or_force_type2,
        automatic_ind2 = 'A' as automatic_ind2,
        {{ trim_and_stdize_nulls('weapon_or_force_type3') }}        as weapon_or_force_type3,
        automatic_ind3 = 'A'                      as automatic_ind3,
        {{ trim_and_stdize_nulls('bias_motivation') }}              as bias_motivation
    from {{ source('nibrs_raw', 'offense') }}
)

select *
from offense_seg
