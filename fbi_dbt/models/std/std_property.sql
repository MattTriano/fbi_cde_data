{{ config(materialized='view', schema='nibrs_std') }}

with property_seg as (
    select
        nibrs_year::smallint                                               as nibrs_year,
        trim(segment)                                                      as segment,
        trim(state_code)                                                   as state_code,
        trim(ori)                                                          as ori,
        trim(incident_no)                                                  as incident_no,
        {{ parse_nibrs_date('incident_date') }}                            as incident_date,
        {{ trim_and_stdize_nulls('property_loss_type') }}                  as property_loss_type,
        {{ trim_and_stdize_nulls('property_descr') }}                      as property_descr,
        {{ trim_and_stdize_nulls('property_descr') }}                      as property_descr,
        {{ trim_and_stdize_nulls('property_value', '[\s\n#]') }}::integer  as property_value,
        {{ parse_nibrs_date('date_recovered') }}                           as date_recovered,
        {{ trim_and_stdize_nulls('motor_vehicles_stolen') }}::smallint     as motor_vehicles_stolen,
        {{ trim_and_stdize_nulls('motor_vehicles_recovered') }}::smallint  as motor_vehicles_recovered,
        {{ trim_and_stdize_nulls('suspected_drug1') }}                     as suspected_drug1,
        {{ trim_and_stdize_nulls('suspected_drug1_qty') }}::integer        as suspected_drug1_qty,
        {{ trim_and_stdize_nulls('suspected_drug1_qty_thous') }}::smallint as suspected_drug1_qty_thous,
        {{ trim_and_stdize_nulls('suspected_drug1_qty_units') }}           as suspected_drug1_qty_units,
        {{ trim_and_stdize_nulls('suspected_drug2') }}                     as suspected_drug2,
        {{ trim_and_stdize_nulls('suspected_drug2_qty') }}::integer        as suspected_drug2_qty,
        {{ trim_and_stdize_nulls('suspected_drug2_qty_thous') }}::smallint as suspected_drug2_qty_thous,
        {{ trim_and_stdize_nulls('suspected_drug2_qty_units') }}           as suspected_drug2_qty_units,
        {{ trim_and_stdize_nulls('suspected_drug3') }}                     as suspected_drug3,
        {{ trim_and_stdize_nulls('suspected_drug3_qty') }}::integer        as suspected_drug3_qty,
        {{ trim_and_stdize_nulls('suspected_drug3_qty_thous') }}::smallint as suspected_drug3_qty_thous,
        {{ trim_and_stdize_nulls('suspected_drug3_qty_units') }}           as suspected_drug3_qty_units
    from {{ source('nibrs_raw', 'property') }}
)

select *
from property_seg
