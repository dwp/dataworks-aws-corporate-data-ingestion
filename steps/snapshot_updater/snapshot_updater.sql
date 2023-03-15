--CREATE EXTERNAL TABLE OVER DATA
drop table if exists dwx_audit_transition.calc_parts_snapshot;
create external table dwx_audit_transition.calc_parts_snapshot (val string) stored as textfile
location 's3://#{hivevar:s3_published_bucket}/analytical-dataset/archive/11_2022_backup/calculationParts/';

--CREATE BASE TABLE WITH ALL DATA FOR DAY INCLUDING DUPES
drop table if exists dwx_audit_transition.calc_parts_snapshot_enriched;
create table dwx_audit_transition.calc_parts_snapshot_enriched (id_key string, dbType STRING, json STRING);
insert into dwx_audit_transition.calc_parts_snapshot_enriched
select
concat(get_json_object(val, '$._id.id'),"_",get_json_object(val, '$._id.type')) as id_key
,case when get_json_object(val, '$._removedDateTime') is null then "INSERT" else "DELETE" end as dbType
,val as json
from dwx_audit_transition.calc_parts_snapshot;
