{{ config(materialized='view', schema='nibrs_std') }}

with deduped_bh_part1 as (

    select distinct on (ori, nibrs_year)
        nibrs_year::integer                        as nibrs_year,
        'BH'                                       as segment,
        trim(state_code)                           as state_code,
        trim(ori)                                  as ori,
        nullif(trim(incident_no), '')              as incident_no,
        case
            when trim(ori_added) = '' then NULL
            else strptime(trim(ori_added), '%Y%m%d')
        end                                        as ori_added,
        case
            when trim(ori_nibrs) = '' then NULL
            else strptime(trim(ori_nibrs), '%Y%m%d')
        end                                        as ori_started_nibrs,
        trim(city)                                 as city,
        trim(state_abbr)                           as state_abbr,
        trim(pop_group)                            as pop_group,
        trim(division)                             as division,
        trim(region)                               as region,
        trim(agency_ind)                           as agency_ind,
        trim(core_city)::boolean                   as core_city,
        nullif(trim(covered_by_ori), '')           as covered_by_ori,
        trim(fbi_field_office)                     as fbi_field_office,
        trim(judicial_district)                    as judicial_district,
        nullif(nullif(trim(nibrs_flag), ''), '\n') as nibrs_flag,
        -- case
        --     when trim(inactive_date) in ('', '\n', ' ') then NULL
        --     else strptime(trim(inactive_date), '%Y%m%d')
        -- end as inactive_date
    from {{ source('nibrs_raw', 'batch_header_p1') }}
    order by nibrs_year, ori

),
deduped_bh_part2 as (
    select distinct on (ori, nibrs_year)
        nibrs_year::integer                      as nibrs_year,
        trim(ori)                                as ori,
        nullif(trim(city1_curr_pop)::integer, 0) as county1_curr_pop,
        nullif(trim(city1_code_ucr), '')         as county1_code_ucr,
        nullif(trim(city1_msa_code), '')         as county1_msa_code,
        nullif(trim(city1_last_pop)::integer, 0) as county1_last_pop,
        nullif(trim(city2_curr_pop)::integer, 0) as county2_curr_pop,
        nullif(trim(city2_code_ucr), '')         as county2_code_ucr,
        nullif(trim(city2_msa_code), '')         as county2_msa_code,
        nullif(trim(city2_last_pop)::integer, 0) as county2_last_pop,
        nullif(trim(city3_curr_pop)::integer, 0) as county3_curr_pop,
        nullif(trim(city3_code_ucr), '')         as county3_code_ucr,
        nullif(trim(city3_msa_code), '')         as county3_msa_code,
        nullif(trim(city3_last_pop)::integer, 0) as county3_last_pop,
        nullif(trim(city4_curr_pop)::integer, 0) as county4_curr_pop,
        nullif(trim(city4_code_ucr), '')         as county4_code_ucr,
        nullif(trim(city4_msa_code), '')         as county4_msa_code,
        nullif(trim(city4_last_pop)::integer, 0) as county4_last_pop
    from {{ source('nibrs_raw', 'batch_header_p2') }}
    order by nibrs_year, ori
),
deduped_bh_part3 as (
    select distinct on (ori, nibrs_year)
        nibrs_year::integer                                         as nibrs_year,
        trim(ori)                                                   as ori,
        nullif(trim(city5_curr_pop)::integer, 0)                    as county5_curr_pop,
        nullif(trim(city5_code_ucr), '')                            as county5_code_ucr,
        nullif(trim(city5_msa_code), '')                            as county5_msa_code,
        nullif(trim(city5_last_pop)::integer, 0)                    as county5_last_pop,
        trim(ind_1_6_12)::enum('', '01', '06', '12')                as ind_1_6_12,
        trim(months_reported)::smallint                             as months_reported,
        trim(master_year)::integer                                  as master_year,
        regexp_replace(trim(jan_zero_rpt), '(^$|\n)', 'N')::boolean as jan_zero_rpt,
        regexp_replace(trim(jan_a_or_b), '(^$|\n)', 'N')::boolean   as jan_a_or_b,
        regexp_replace(trim(jan_window), '(^$|\n)', 'N')::boolean   as jan_window,
        regexp_replace(trim(feb_zero_rpt), '(^$|\n)', 'N')::boolean as feb_zero_rpt,
        regexp_replace(trim(feb_a_or_b), '(^$|\n)', 'N')::boolean   as feb_a_or_b,
        regexp_replace(trim(feb_window), '(^$|\n)', 'N')::boolean   as feb_window,
        regexp_replace(trim(mar_zero_rpt), '(^$|\n)', 'N')::boolean as mar_zero_rpt,
        regexp_replace(trim(mar_a_or_b), '(^$|\n)', 'N')::boolean   as mar_a_or_b,
        regexp_replace(trim(mar_window), '(^$|\n)', 'N')::boolean   as mar_window,
        regexp_replace(trim(apr_zero_rpt), '(^$|\n)', 'N')::boolean as apr_zero_rpt,
        regexp_replace(trim(apr_a_or_b), '(^$|\n)', 'N')::boolean   as apr_a_or_b,
        regexp_replace(trim(apr_window), '(^$|\n)', 'N')::boolean   as apr_window,
        regexp_replace(trim(may_zero_rpt), '(^$|\n)', 'N')::boolean as may_zero_rpt,
        regexp_replace(trim(may_a_or_b), '(^$|\n)', 'N')::boolean   as may_a_or_b,
        regexp_replace(trim(may_window), '(^$|\n)', 'N')::boolean   as may_window,
        regexp_replace(trim(jun_zero_rpt), '(^$|\n)', 'N')::boolean as jun_zero_rpt,
        regexp_replace(trim(jun_a_or_b), '(^$|\n)', 'N')::boolean   as jun_a_or_b,
        regexp_replace(trim(jun_window), '(^$|\n)', 'N')::boolean   as jun_window,
        regexp_replace(trim(jul_zero_rpt), '(^$|\n)', 'N')::boolean as jul_zero_rpt,
        regexp_replace(trim(jul_a_or_b), '(^$|\n)', 'N')::boolean   as jul_a_or_b,
        regexp_replace(trim(jul_window), '(^$|\n)', 'N')::boolean   as jul_window,
        regexp_replace(trim(aug_zero_rpt), '(^$|\n)', 'N')::boolean as aug_zero_rpt,
        regexp_replace(trim(aug_a_or_b), '(^$|\n)', 'N')::boolean   as aug_a_or_b,
        regexp_replace(trim(aug_window), '(^$|\n)', 'N')::boolean   as aug_window,
        regexp_replace(trim(sep_zero_rpt), '(^$|\n)', 'N')::boolean as sep_zero_rpt,
        regexp_replace(trim(sep_a_or_b), '(^$|\n)', 'N')::boolean   as sep_a_or_b,
        regexp_replace(trim(sep_window), '(^$|\n)', 'N')::boolean   as sep_window,
        regexp_replace(trim(oct_zero_rpt), '(^$|\n)', 'N')::boolean as oct_zero_rpt,
        regexp_replace(trim(oct_a_or_b), '(^$|\n)', 'N')::boolean   as oct_a_or_b,
        regexp_replace(trim(oct_window), '(^$|\n)', 'N')::boolean   as oct_window,
        regexp_replace(trim(nov_zero_rpt), '(^$|\n)', 'N')::boolean as nov_zero_rpt,
        regexp_replace(trim(nov_a_or_b), '(^$|\n)', 'N')::boolean   as nov_a_or_b,
        regexp_replace(trim(nov_window), '(^$|\n)', 'N')::boolean   as nov_window,
        regexp_replace(trim(dec_zero_rpt), '(^$|\n)', 'N')::boolean as dec_zero_rpt,
        regexp_replace(trim(dec_a_or_b), '(^$|\n)', 'N')::boolean   as dec_a_or_b,
        regexp_replace(trim(dec_window), '(^$|\n)', 'N')::boolean   as dec_window,
        trim(cnty1_fips)                                            as county1_fips,
        trim(cnty2_fips)                                            as county2_fips,
        trim(cnty3_fips)                                            as county3_fips,
        trim(cnty4_fips)                                            as county4_fips,
        trim(cnty5_fips)                                            as county5_fips
    from {{ source('nibrs_raw', 'batch_header_p3') }}
    order by nibrs_year, ori
),
batch_header_from_parts as (
    select *
    from deduped_bh_part1 as b1
    full outer join deduped_bh_part2 as b2
    on b1.nibrs_year = b2.nibrs_year and b1.ori = b2.ori
    full outer join deduped_bh_part3 as b3
    on b1.nibrs_year = b3.nibrs_year and b1.ori = b3.ori
)

select *
from deduped_bh_part1
