-- ============================================================================
-- Banking Fraud Detection Pipeline - Snowflake Stored Procedures
-- ============================================================================
-- Purpose:
--   Loads banking datasets into raw Snowflake tables, curates valid records,
--   stores rejected/invalid records with severity labels, maintains customer
--   SCD Type 2 history, and detects fraud patterns from transaction and login data.
--
-- Main schemas used:
--   banking_data.data_raw      : Raw files loaded from external stages
--   banking_data.curated_data  : Cleaned business-ready tables
--   banking_data.fraud_tb      : Fraud and data quality flags
--   banking_data.sp            : Stored procedures
-- ============================================================================

-- ============================================================================
-- 00. Load raw files from Snowflake external stages
-- ============================================================================
create or replace procedure banking_data.sp.coping()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
BEGIN
copy into banking_data.data_raw.transactions_raw
           from ( select $1, $2, $3, $4, $5, 
                $6, $7, $8, $9, $10, 
                CURRENT_TIMESTAMP(), METADATA$FILENAME AS batch_id,
                from
                @banking_data.external_stage.transactions);

copy into banking_data.data_raw.logins_raw
from( select $1,$2,$3,$4,$5,$6,$7, current_timestamp(), METADATA$FILENAME as batch_id
    from @banking_data.external_stage.login_events) ;

copy into banking_data.data_raw.customers_raw
from @banking_data.external_stage.customers;


copy into banking_data.data_raw.coordinates_raw
from @banking_data.external_stage.city_coordinates;
    
RETURN 'Load completed';
END;
$$;
 

-- ============================================================================
-- 02. Curate valid transaction records
-- ============================================================================

create or replace  procedure banking_data.sp.transactions_01()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
BEGIN

insert into banking_data.curated_data.transactions (txn_id,customer_id,txn_time,txn_type,amount,merchant_name,city,country,device_id,channel,load_time,batch_id)
(
with latest_batch AS (
        SELECT batch_id
        FROM banking_data.data_raw.transactions_raw
        QUALIFY ROW_NUMBER() OVER (ORDER BY load_time DESC) = 1
    ), 
    dupps as (
select *, row_number() over (partition by txn_id order by txn_id) as dups
from banking_data.data_raw.transactions_raw
where batch_id = (select batch_id from latest_batch)
)
select txn_id,customer_id,try_to_timestamp(txn_time,'MM/DD/YYYY HH24:MI'),txn_type,amount,merchant_name,city,country,device_id,channel,load_time,batch_id
from dupps
where customer_id is not null
and try_to_timestamp(txn_time,'MM/DD/YYYY HH24:MI') is not null
and txn_type in ('ATM','POS','ONLINE','TRANSFER')
and try_cast(amount as int) is not null 
and try_cast(amount as int) >0
and try_cast(amount as int) < 50000
and merchant_name is not null
and city is not null
and country in (select country_name from banking_data.data_raw.countires)
and device_id is not null
and channel in ('ATM','CARD','ONLINE','MOBILE')
and dups = 1
);

RETURN 'Load completed';
END;
$$;



-- ============================================================================
-- 03. Flag invalid or suspicious transaction records
-- ============================================================================

create or replace  procedure banking_data.sp.transactions_02()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
BEGIN

merge into banking_data.fraud_tb.transaction_flags f1
using(
with latest_batch AS (
        SELECT batch_id
        FROM banking_data.data_raw.transactions_raw
        QUALIFY ROW_NUMBER() OVER (ORDER BY load_time DESC) = 1
    ),dupps as (
select *, row_number() over (partition by txn_id order by txn_id) as dups
from banking_data.data_raw.transactions_raw
WHERE batch_id = (SELECT batch_id FROM latest_batch)
)
select txn_id,customer_id,txn_time,txn_type,amount,merchant_name,city,country,device_id,channel,load_time,
case
WHEN customer_id is null then 'INVALID_CUSTOMER'
when try_to_timestamp(txn_time,'MM/DD/YYYY HH24:MI') is  null then 'INVALID_TXN_TIME'
WHEN txn_type not in ('ATM','POS','ONLINE','TRANSFER') then 'INVALID_TXN_TYPE'
WHEN try_cast(amount as int) is  null then 'INVALID_AMOUNT'
when try_cast(amount as int) <0 then 'INVALID_AMOUNT'
when try_cast(amount as int) >= 50000 then 'UNUSUAL_AMOUNT'
when merchant_name is null then 'INVALID_MERCHANT'
when city is null then 'INVALID_CITY'
when country not in (select country_name from banking_data.data_raw.countires) then 'INVALID_COUNTRY'
when device_id is null then 'INVALID_DEVICE'
when channel not in ('ATM','CARD','ONLINE','MOBILE') then 'INVALID_CHANNEL'
WHEN dups>1 then 'DUPLICATE_RECORD'
END as Flag,

case
when Flag = 'INVALID_CUSTOMER' then 'LOW'
when Flag='INVALID_TXN_TIME' then 'MEDIUM'
when Flag = 'INVALID_TXN_TYPE' then 'LOW'
when flag ='INVALID_AMOUNT' then 'LOW'
when Flag = 'UNUSUAL_AMOUNT' then 'HIGH'
when Flag = 'INVALID_MERCHANT' then 'LOW'
when Flag = 'INVALID_CITY' then 'MEDIUM'
when Flag = 'INVALID_COUNTRY' then 'HIGH'
when Flag = 'INVALID_CHANNEL' then 'LOW'
when Flag = 'DUPLICATE_RECORD' then 'LOW'
END as Severity,
batch_id

from dupps
where customer_id is null
or try_to_timestamp(txn_time,'MM/DD/YYYY HH24:MI') is  null
or txn_type not in ('ATM','POS','ONLINE','TRANSFER')
or try_cast(amount as int) is  null 
or try_cast(amount as int) <0
or try_cast(amount as int) >= 50000
or merchant_name is  null
or city is  null
or country not in (select country_name from banking_data.data_raw.countires)
or device_id is null
or channel not in ('ATM','CARD','ONLINE','MOBILE')
or dups>1
) as f2
on f1.txn_id = f2.txn_id and f1.flag = f2.flag

when not matched then
insert (txn_id,customer_id,txn_time,txn_type,amount,merchant_name,city,country,device_id,channel,load_time,Flag,Severity,batch_id)
values (f2.txn_id,f2.customer_id,f2.txn_time,f2.txn_type,f2.amount,f2.merchant_name,f2.city,f2.country,f2.device_id,f2.channel,f2.load_time,f2.Flag,Severity,f2.batch_id);

RETURN 'Load completed';
END;
$$;

-- ============================================================================
-- 01. Maintain customer history using SCD Type 2 logic
-- ============================================================================
create or replace  procedure banking_data.sp.transactions_00()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
BEGIN

update banking_data.curated_data.customers_scd cs
set cs.is_active = 'No'
from  banking_data.public.scd_stream ss
where cs.customer_id = ss.customer_id and cs.is_active ='Yes'
and (
cs.customer_name <> ss.customer_name or
cs.home_country <> ss.home_country or
cs.home_city <> ss.home_city or
cs.Account_type <> ss.account_type 
);

insert into banking_data.curated_data.customers_scd 
(customer_id,customer_name,home_country,home_city,account_type,account_open_date,is_active) 
select
    ss.customer_id,
    ss.customer_name,
    ss.home_country,
    ss.home_city,
    ss.account_type,
    ss.account_open_date,
    'Yes' is_active
from banking_data.public.scd_stream ss
left join banking_data.curated_data.customers_scd cs
    on ss.customer_id = cs.customer_id
   and cs.is_active = 'Yes'
where cs.customer_id is null
   or cs.customer_name <> ss.customer_name
   or cs.home_country <> ss.home_country
   or cs.home_city <> ss.home_city
   or cs.account_type <> ss.account_type;


RETURN 'Load completed';
END;
$$;

-- ============================================================================
-- Login processing procedures
-- ============================================================================

-- ============================================================================
-- 05. Curate valid login records
-- ============================================================================
select * from banking_data.data_raw.logins_raw

create or replace  procedure banking_data.sp.logins_01()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
BEGIN
merge into banking_data.curated_data.logins_curated c1
using(
with latest_batch AS (
        SELECT batch_id 
        FROM banking_data.data_raw.logins_raw
        QUALIFY ROW_NUMBER() OVER (ORDER BY load_time DESC) = 1
    ),logins_curated as (
select event_id, customer_id,login_time,ip_address,device_id,country,load_time,
row_number() over (partition by event_id order by event_id) as count_dups,
case 
    when login_status='OK' then 'SUCCESS'
    when login_status = 'SUCCESS' then 'SUCCESS'
    when login_status = 'FAILED' then 'FAILED'
    END as login_status,
    batch_id
from banking_data.data_raw.logins_raw 
where customer_id is not null
and try_to_timestamp(login_time,'MM/DD/YYYY HH24:MI') is not null
and regexp_like(ip_address,'^[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}$')
and ip_address is not null
and device_id is not null
and country  in (select country_name from banking_data.data_raw.countires)
and country is not null
and login_status in ('SUCCESS','FAILED')
and batch_id= (select batch_id from latest_batch)
)
select event_id, customer_id,login_time,login_status,ip_address,device_id,country,load_time,batch_id
from logins_curated
where count_dups <2 
) c2
on c1.event_id = c2.event_id
when not matched
then insert (event_id, customer_id,login_time,login_status,ip_address,device_id,country,load_time,batch_id)
    values(c2.event_id, cast(c2.customer_id as INT),to_timestamp(c2.login_time,'MM/DD/YYYY HH24:MI'),c2.login_status,c2.ip_address,c2.device_id,c2.country,c2.load_time,c2.batch_id);

RETURN 'Load completed';
END;
$$;

-- ============================================================================
-- 06. Flag duplicate login events
-- ============================================================================

create or replace  procedure banking_data.sp.logins_02()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
BEGIN
merge into banking_data.fraud_tb.flagged fl
using (
with latest_batch AS (
        SELECT batch_id 
        FROM banking_data.data_raw.logins_raw
        QUALIFY ROW_NUMBER() OVER (ORDER BY load_time DESC) = 1
    ), logins_curated as (
select event_id, customer_id,login_time,ip_address,device_id,country,load_time,
row_number() over (partition by event_id order by event_id) as count_dups,
case 
    when login_status='OK' then 'SUCCESS'
    when login_status = 'SUCCESS' then 'SUCCESS'
    when login_status = 'FAILED' then 'FAILED'
    END as login_status,
    batch_id
from banking_data.data_raw.logins_raw 
where customer_id is not null
and try_to_timestamp(login_time,'MM/DD/YYYY HH24:MI') is not null
and regexp_like(ip_address,'^[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}$')
and ip_address is not null
and device_id is not null
and country  in (select country_name from banking_data.data_raw.countires)
and country is not null
and login_status in ('SUCCESS','FAILED')
and batch_id = (select batch_id from latest_batch)
)
select event_id, customer_id,login_time,login_status,ip_address,device_id,country,load_time,
CASE 
    when count_dups>1 then 'Duplicate_Entry'
END as flag,
CASE
    when flag = 'Duplicate_Entry' then 'Low'
END Severity,
batch_id
from logins_curated
where count_dups >1
) du
on fl.event_id = du.event_id and fl.Device_Flag = du.flag
when not matched then
insert (event_id, customer_id,login_time,login_status,ip_address,device_id,country,load_time,Device_Flag,Severity, batch_id)
values (du.event_id, du.customer_id,cast(du.login_time as varchar),du.login_status,du.ip_address,du.device_id,du.country,du.load_time,du.flag,du.Severity,du.batch_id);

RETURN 'Load completed';
END;
$$;


-- ============================================================================
-- 07. Flag invalid login records
-- ============================================================================
create or replace  procedure banking_data.sp.logins_03()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
BEGIN
merge into banking_data.fraud_tb.flagged rd
using ( with latest_batch AS (
        SELECT batch_id 
        FROM banking_data.data_raw.logins_raw
        QUALIFY ROW_NUMBER() OVER (ORDER BY load_time DESC) = 1
    ), fr_cte as (select event_id, customer_id,login_time,login_status,ip_address,device_id,country,load_time,
case 
    when customer_id is null then 'Invalid_customer'
    when try_to_timestamp(login_time,'MM/DD/YYYY HH24:MI') is null then 'Invalid_date'
    when not regexp_like(ip_address,'^[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}$') then 'Invalid_IP'
    when ip_address is null then 'Invalid_IP'
    when device_id is null then 'Invalid_deviceID'
    when country not in (select country_name from banking_data.data_raw.countires) then 'Invalid_country'
    when country is null then 'Invalid_country'
    when login_status not in ('SUCCESS','FAILED') then 'Invalid_login'
    when login_status is null then 'Invalid_login'
END as Flag,
case 
    when Flag = 'Invalid_customer' then 'Low'
    when Flag='Invalid_date' then 'Low' 
    when Flag='Invalid_IP' then 'Low'
    when Flag = 'Invalid_deviceID' then 'Low'
    when Flag = 'Invalid_login' then 'Low'
    when Flag = 'Invalid_country' then 'High'
END as Severity,
batch_id
from banking_data.data_raw.logins_raw
where batch_id = (select batch_id from latest_batch) and (customer_id is null
or try_to_timestamp(login_time,'MM/DD/YYYY HH24:MI') is null
or regexp_like(ip_address,'^[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}$')= false
or ip_address is null
or device_id is null
or country not in (select country_name from banking_data.data_raw.countires)
or country is null
or login_status not in ('SUCCESS','FAILED')
or login_status is null)) select * from fr_cte ) nl

on rd.event_id = nl.event_id and rd.Device_Flag = nl.Flag
when not matched then 
insert (event_id, customer_id,login_time,login_status,ip_address,device_id,country,load_time,Device_Flag,Severity,batch_id)
values(nl.event_id, nl.customer_id,cast(nl.login_time as varchar),nl.login_status,nl.ip_address,nl.device_id,nl.country,nl.load_time,nl.Flag,nl.Severity,nl.batch_id);

RETURN 'Load completed';
END;
$$;


-- ============================================================================
-- 08. Flag multiple failed login attempts within a short time window
-- ============================================================================


create or replace  procedure banking_data.sp.logins_04()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
BEGIN
merge into banking_data.fraud_tb.flagged fl
using ( with latest_batch AS (
        SELECT batch_id 
        FROM banking_data.data_raw.logins_raw
        QUALIFY ROW_NUMBER() OVER (ORDER BY load_time DESC) = 1
    ),five_mins as (
select *, 
lag(login_time,4) over (partition by customer_id order by login_time) as five_days_before,
datediff(min,lag(login_time ,4) over (partition by customer_id order by login_time ),login_time) as mins_diff
from banking_data.curated_data.logins_curated lc
where lc.login_status = 'FAILED' and lc.batch_id = (select batch_id from latest_batch)
)
select event_id,customer_id,login_time,login_status,ip_address,device_id,country,load_time,
'Flagged for consecutive failed logins' as Flag,
case
when Flag = 'Flagged for consecutive failed logins' then 'High'
END as Severity, batch_id
from five_mins 
where mins_diff <=10
) as days
on fl.event_id = days.event_id and fl.device_flag =days.Flag
when not matched then
insert (event_id, customer_id,login_time,login_status,ip_address,device_id,country,load_time,Device_flag,Severity,batch_id)
values (days.event_id, days.customer_id,cast(days.login_time as varchar),days.login_status,days.ip_address,days.device_id,days.country,days.load_time,days.Flag,days.Severity,days.batch_id);

RETURN 'Load completed';
END;
$$;



-- ============================================================================
-- 04. Store first known device for each customer
-- ============================================================================
create or replace  procedure banking_data.sp.logins_00()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
BEGIN
insert into banking_data.curated_data.customer_known_devices (customer_id,device_id)
with latest_batch AS (
        SELECT batch_id 
        FROM banking_data.data_raw.logins_raw
        QUALIFY ROW_NUMBER() OVER (ORDER BY load_time DESC) = 1
    ), new_device as (
select customer_id,device_id, min(try_to_timestamp(login_time,'MM/DD/YYYY HH24:MI')) first_login_time,batch_id
from banking_data.data_raw.logins_raw
where device_id is not null and customer_id is not null and try_to_timestamp(login_time,'MM/DD/YYYY HH24:MI') is not null
and batch_id = (select batch_id from latest_batch)
group by customer_id, device_id, batch_id
order by customer_id
),
 device_rank as (
select *, row_number() over (partition by customer_id order by first_login_time,device_id) ranks 
from new_device
)
select customer_id,device_id from device_rank 
where ranks =1 and
customer_id not in (select customer_id from banking_data.curated_data.customer_known_devices);
RETURN 'Load completed';
END;
$$;

-- ============================================================================
-- 09. Flag successful login from unknown device
-- ============================================================================
create or replace procedure banking_data.sp.logins_05()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
BEGIN
merge into banking_data.Fraud_TB.flagged as df
using ( 
select cu.*,
CASE
    when cd.device_id is null then 'Device Unknown'
    else 'Device Known'
    END as device_flag,
CASE
    when device_flag = 'Device Unknown'
    then 'Medium'
    END as Severity
from (
  WITH latest_batch AS (
            SELECT batch_id 
            FROM banking_data.data_raw.logins_raw
            QUALIFY ROW_NUMBER() OVER (ORDER BY load_time DESC) = 1
        ),logins_curated as (
select event_id, customer_id,login_time,login_status,ip_address,device_id,country,load_time,
row_number() over (partition by event_id order by event_id) as count_dups, batch_id
from banking_data.data_raw.logins_raw 
WHERE batch_id = (SELECT batch_id FROM latest_batch)
)
select event_id, customer_id,login_time,login_status,ip_address,device_id,country,load_time,batch_id
from logins_curated
where count_dups =1 and login_status = 'SUCCESS'
)  cu left join banking_data.curated_data.customer_known_devices cd
on cu.customer_id = cd.customer_id and cu.device_id = cd.device_id
where device_flag ='Device Unknown' and cu.customer_id is not null
) as cf

on df.EVENT_ID = cf.EVENT_ID and df.device_flag = cf.device_flag
when not matched
then 
insert (event_id, customer_id,login_time,login_status,ip_address,device_id,country,load_time,Device_flag,Severity, batch_id)
values (cf.event_id, cf.customer_id,cast(cf.login_time as varchar),cf.login_status,cf.ip_address,cf.device_id,cf.country,cf.load_time,cf.Device_flag, cf.Severity, cf.batch_id);

RETURN 'Load completed';
END;
$$;


-- ============================================================================
-- 10. Flag impossible travel based on login country distance and time difference
-- ============================================================================



create or replace procedure banking_data.sp.logins_06()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
BEGIN
merge into banking_data.fraud_tb.flagged fl
using(
with latest_batch AS (
        SELECT batch_id 
        FROM banking_data.data_raw.logins_raw
        QUALIFY ROW_NUMBER() OVER (ORDER BY load_time DESC) = 1
    ), dis as (
select l.*, c.latitude as lat2,c.longitude as long2, lag(c.latitude,1) over (partition by l.customer_id order by l.login_time) as latitude_2,
lag(c.longitude,1) over (partition by l.customer_id order by l.login_time) as longitude_2,
try_to_timestamp(l.login_time,'MM/DD/YYYY HH24:MI') as current_login,lag(try_to_timestamp(l.login_time,'MM/DD/YYYY HH24:MI'),1) over (partition by l.customer_id order by l.load_time) as first_login
from banking_data.data_raw.logins_raw l 
left join
banking_data.data_raw.coordinates_raw c
on l.COUNTRY = c.COUNTRY
where l.login_status='SUCCESS' and l.batch_id = (select batch_id from latest_batch)
),
flag as (
select *,
    ST_DISTANCE(
        ST_MAKEPOINT(longitude_2, latitude_2), 
        ST_MAKEPOINT(long2, lat2)
    )/1000 AS distance_kms,
    datediff('min',first_login,current_login) as mins_login, 
    'IMPOSSIBLE_TRAVEL' as Flag
from dis
where mins_login <=30
and distance_kms >100
)
select event_id,customer_id,login_time,login_status,ip_address,device_id,country,load_time,Flag,
case
when Flag = 'IMPOSSIBLE_TRAVEL'
then 'High' end Severity,
batch_id
from flag
)it 
on fl.event_id = it.event_id and fl.Device_flag = it.flag
when not matched then
insert (event_id, customer_id,login_time,login_status,ip_address,device_id,country,load_time,Device_flag,Severity,batch_id)
values (it.event_id, it.customer_id,it.login_time,it.login_status,it.ip_address,it.device_id,it.country,it.load_time,it.Flag,it.Severity,it.batch_id);

RETURN 'Load completed';
END;
$$;