-- This is a placeholder and will be replaced by snapshot_updater code when it is ready
CREATE TABLE IF NOT EXISTS uc_dw_auditlog.foobar (
 id int,
 name string,
 age int,
 gender string )
 COMMENT 'Test table'
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ',';
