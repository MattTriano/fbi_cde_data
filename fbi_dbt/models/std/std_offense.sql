{{ config(materialized='view', schema='nibrs_std') }}

with offense_seg as (
    select distinct on (nibrs_year, ori, incident_no, ucr_offense_code, incident_date)
        nibrs_year::smallint                      as nibrs_year,
        trim(segment)                             as segment,
        trim(state_code)                          as state_code,
        trim(ori)                                 as ori,
        trim(incident_no)                         as incident_no,
        strptime(trim(incident_date), '%Y%m%d')   as incident_date,
        trim(ucr_offense_code)                    as ucr_offense_code,
        trim(attempted_or_completed)              as attempted_or_completed,
        nullif(offender1_suspected_of_using, ' ') as offender1_suspected_of_using,
        nullif(offender2_suspected_of_using, ' ') as offender2_suspected_of_using,
        nullif(offender3_suspected_of_using, ' ') as offender3_suspected_of_using,
        location_type                             as location_type,
        nullif(
            regexp_replace(premises_entered, '[\s\n]', '', 'g'), ''
        )::smallint                               as premises_entered,
        nullif(trim(forced_entry), '')            as forced_entry,
        nullif(
            regexp_replace(criminal_activity_type1, '[\s\n]', '', 'g'), ''
        )                                         as criminal_activity_type1,
        nullif(
            regexp_replace(criminal_activity_type2, '[\s\n]', '', 'g'), ''
        )                                         as criminal_activity_type2,
        nullif(
            regexp_replace(criminal_activity_type3, '[\s\n]', '', 'g'), ''
        )                                         as criminal_activity_type3,
        nullif(
            regexp_replace(weapon_or_force_type1, '[\s\n]', '', 'g'), ''
        )                                         as weapon_or_force_type1,
        automatic_ind1 = 'A' as automatic_ind1,
        nullif(
            regexp_replace(weapon_or_force_type2, '[\s\n]', '', 'g'), ''
        )                                         as weapon_or_force_type2,
        automatic_ind2 = 'A' as automatic_ind2,
        nullif(
            regexp_replace(weapon_or_force_type3, '[\s\n]', '', 'g'), ''
        )                                         as weapon_or_force_type3,
        automatic_ind3 = 'A'                      as automatic_ind3,
        nullif(trim(bias_motivation), '')         as bias_motivation
    from {{ source('nibrs_raw', 'offense') }}
)

select *
from offense_seg
