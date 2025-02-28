{{ config(materialized='table', schema='nibrs_std') }}

with admin_seg as (
    select
        nibrs_year::smallint                                 as nibrs_year,
        trim(segment)                                        as segment,
        trim(state_code)                                     as state_code,
        trim(ori)                                            as ori,
        trim(incident_no)                                    as incident_no,
        strptime(trim(incident_date), '%Y%m%d')              as incident_date,
        trim(report_date_ind)                                as report_date_ind,
        nullif(
            regexp_replace(trim(incident_hour), '[\\s\n-]', '', 'g'), ''
        )::smallint                                          as incident_hour,
        trim(total_offenses)::smallint                       as total_offenses,
        trim(total_victims)::smallint                        as total_victims,
        trim(total_offenders)::smallint                      as total_offenders,
        trim(total_arrestees)::smallint                      as total_arrestees,
        trim(city_submission)                                as city_submission,
        trim(ex_cleared)::enum('A', 'B', 'C', 'D', 'E', 'N') as ex_cleared,
        case
            when
                regexp_matches(
                    trim(ex_cleared_date),
                    '^((199[1-9])|(20[0-3][0-9]))((0[1-9])|(1[0-2]))(([0-2][0-9])|(3[01]))$'
                )
            then try_cast(strptime(trim(ex_cleared_date), '%Y%m%d') as date)
            else NULL
        end                                                  as ex_cleared_date
    from {{ source('nibrs_raw', 'administrative') }}
)

select *
from admin_seg
