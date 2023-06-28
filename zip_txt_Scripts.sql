--112000011



insert into ABCR_CONTROL.JOB_ORCHESTRATION_MASTER 
Values (112,112000011,NULL,'ZIP_TEST',NULL,'Y',NULL,SYSTEM_USER,CURRENT_TIMESTAMP,SYSTEM_USER,CURRENT_TIMESTAMP,NULL,NULL)

EXEC ABCR_CONTROL.USP_INSERT_JOB_ORCHESTRATION_MASTER
@TENANT_ID =112,
@Jobid =112000011,    
@Seq_ID =NULL,
@Job_Name_Text ='ZIP_TEST',
@Job_Description_Text =NULL,
@IS_Active_Flag ='Y',
@JOB_EXECUTION_TIME =NULL,
@Spark_Cluster_Size_Text =NULL,
@Tool_ID_Text =NULL


insert into ABCR_CONTROL.job_configuration_values 
values (112000011,'DataBricks_Notebook_ChildPath','/olympus-data-engineering-project/Development/Code/child_idh_framework_run_ABCR','NULL','Y','','','1900-01-01 00:00:00.000','1900-01-01 00:00:00.000'),
(112000011,'SRC_SYSTEM','FILE','NULL','Y','','','1900-01-01 00:00:00.000','1900-01-01 00:00:00.000'),
(112000011,'SOURCE_FILE_CONNECTION','SOURCE_SPARK_READ_OPTION1','NULL','Y','','','1900-01-01 00:00:00.000','1900-01-01 00:00:00.000')

EXEC ABCR_CONTROL.USP_INSERT_JOB_CONFIGURATION_VALUES
@JobId =112000011,
@Config_Key_Text ='DataBricks_Notebook_ChildPath',
@Config_Value_Text ='/olympus-data-engineering-project/Development/Code/child_idh_framework_run_ABCR',
@Description_Text =NULL,
@IS_Active_Flag ='Y'

EXEC ABCR_CONTROL.USP_INSERT_JOB_CONFIGURATION_VALUES
@JobId =112000011,
@Config_Key_Text ='SRC_SYSTEM',
@Config_Value_Text ='FILE',
@Description_Text =NULL,
@IS_Active_Flag ='Y'

EXEC ABCR_CONTROL.USP_INSERT_JOB_CONFIGURATION_VALUES
@JobId =112000011,
@Config_Key_Text ='SOURCE_FILE_CONNECTION',
@Config_Value_Text ='SOURCE_SPARK_READ_OPTION1',
@Description_Text =NULL,
@IS_Active_Flag ='Y'



EXEC ABCR_CONTROL.USP_INSERT_UOW_CONTROL 
@TENANT_ID=112,
@BOW_ID=112001,
@SBOW_ID=11200101,
@UOW_NAME= 'zip_txt',
@IS_ACTIVE_FLAG='Y',
@UOW_Description= 'zip_txt',
@retry_on_error_flag= NULL,
@severity_number= NULL,
@raise_ticket_flag= NULL,
@impact_number= NULL,
@Is_Reprocess_Flag= NULL,
@Update_Maintenance_System_Domain_Account_Name= NULL,
@Update_GMT_Timestamp= NULL,
@UOW_CODE= NULL

UOW == ?

--- GET UOW_ID----

EXEC ABCR_CONTROL.USP_INSERT_JOB_ORCHESTRATION_CONTROL
@JOB_ID =112000011,
@TENANT_ID =112,
@BOW_ID =112001,
@SBOW_ID=11200101,
@UOW_ID =,
@Seq_ID =1,
@IS_Active_Flag ='Y',
@Update_Maintenance_System_Domain_Account_Name =NULL,
@Update_GMT_Timestamp =NULL,
@Wait_Time_In_Min =NULL,
@Job_Params='{"layers":"PRLAN,LZ"}'

--Add UOW_ID

EXEC ABCR_AUDIT.USP_INSERT_BOW_CDC_AUX_CONTROL_R01
@TENANT_ID =112,
@BOW_ID =112001,
@SBOW_ID=11200101,
@UOW_ID =,
@CHANGE_DATA_CAPTURE_START_TIMESTAMP ='2021-01-01 00:00:00.0000000',
@CHANGE_DATA_CAPTURE_END_TIMESTAMP ='2022-10-19 15:48:02.0000000',
@CHANGE_DATA_CAPTURE_START_SEQUENCE_NUMBER =NULL,    
@CHANGE_DATA_CAPTURE_END_SEQUENCE_NUMBER=NULL,
@CDC_TYPE_DESCRIPTION ='Timestamp',
@UPDATE_MAINTENANCE_SYSTEM_DOMAIN_ACCOUNT_NAME =NULL,
@UPDATE_GMT_TIMESTAMP =NULL,
@BATCHID=NULL,
@MICROBATCHID=NULL


--Add UOW_ID

EXEC ABCR_CONTROL.USP_INSERT_ENTITY_MAPPING_CONTROL
@TENANT_ID =112,
@BOW_ID =112001,
@SBOW_ID=11200101,
@UOW_ID =,
@Parent_UOW_ID =0,
@IS_ACTIVE ='Y',
@Update_Maintenance_System_Domain_Account_Name =NULL,
@Update_GMT_Timestamp =NULL


--Add UOW_ID

EXEC ABCR_CONTROL.USP_INSERT_PROCESS_CONTROL
@Tenant_ID =112,  
@BOW_ID =112001,  
@SBOW_ID =11200101,
@UOW_ID =,  
@Stage_ID = 1,  
@Task_ID = 1,  
@Batch_ID = 1,  
@Query_Type =' ',  
@Query_Text =' ',  
@Schema_ID =,  
@SPARK_SETTINGS_TEXT =NULL,  
@IS_ACTIVE_FLAG = 'Y',  
@Source_Table_Name = ' ',  
@Target_Table_Name =' ',  
@PARAMS_JSON_TEXT ='{"Service":"PrelandingLoadService"}',  
@Actions_Text ='UNCOMPRESS',  
@Update_Maintenance_System_Domain_Account_Name =NULL,  
@Update_GMT_Timestamp = NULL


--replace 1120010100005 with  new UOW_ID 

Insert into ABCR_CONTROL.UOW_Configuration_Values 
values (112001,11200101,1120010100005,'IS_COMPRESSED','Y','Y',CURRENT_TIMESTAMP,NULL,SYSTEM_USER,NULL),
(112001,11200101,1120010100005,'COMPRESSION_FORMAT','tar.gz','Y',CURRENT_TIMESTAMP,NULL,SYSTEM_USER,NULL),
(112001,11200101,1120010100005,'SOURCE_FILE_PATH','/it/odw/global_abcr_pilot/dataingestion/input/gcw_test/AMS_Region_EDL_POS_20230530190939.txt.zip','Y',CURRENT_TIMESTAMP,NULL,SYSTEM_USER,NULL),
(112001,11200101,1120010100005,'DATA_FILE_PATH','/it/odw/global_abcr_pilot/dataingestion/output/gcw_test/','Y',CURRENT_TIMESTAMP,NULL,SYSTEM_USER,NULL),
(112001,11200101,1120010100005,'FILE_PATTERN','file_pat AMS_Region_EDL_POS_','Y',CURRENT_TIMESTAMP,NULL,SYSTEM_USER,NULL)

EXEC ABCR_CONTROL.USP_INSERT_UOW_CONFIGURATION_VALUES
@BOW_ID =112001,
@SBOW_ID =11200101,
@UOW_ID =1120010100005,
@Config_Key_Text ='IS_COMPRESSED',
@Config_Value_Text ='Y',
@IS_Active_Flag ='Y'


EXEC ABCR_CONTROL.USP_INSERT_UOW_CONFIGURATION_VALUES
@BOW_ID =112001,
@SBOW_ID =11200101,
@UOW_ID =1120010100005,
@Config_Key_Text ='COMPRESSION_FORMAT',
@Config_Value_Text ='tar.gz',
@IS_Active_Flag ='Y'

EXEC ABCR_CONTROL.USP_INSERT_UOW_CONFIGURATION_VALUES
@BOW_ID =112001,
@SBOW_ID =11200101,
@UOW_ID =1120010100005,
@Config_Key_Text ='SOURCE_FILE_PATH',
@Config_Value_Text ='/it/odw/global_abcr_pilot/dataingestion/input/gcw_test/AMS_Region_EDL_POS_20230530190939.txt.zip',
@IS_Active_Flag ='Y'


EXEC ABCR_CONTROL.USP_INSERT_UOW_CONFIGURATION_VALUES
@BOW_ID =112001,
@SBOW_ID =11200101,
@UOW_ID =1120010100005,
@Config_Key_Text ='DATA_FILE_PATH',
@Config_Value_Text ='/it/odw/global_abcr_pilot/dataingestion/output/gcw_test/',
@IS_Active_Flag ='Y'


EXEC ABCR_CONTROL.USP_INSERT_UOW_CONFIGURATION_VALUES
@BOW_ID =112001,
@SBOW_ID =11200101,
@UOW_ID =1120010100005,
@Config_Key_Text ='FILE_PATTERN',
@Config_Value_Text ='file_pat AMS_Region_EDL_POS_',
@IS_Active_Flag ='Y'



================================


--get UOW_ID

EXEC ABCR_CONTROL.USP_INSERT_UOW_CONTROL 
@TENANT_ID=112,
@BOW_ID=112001,
@SBOW_ID=11200102,
@UOW_NAME= 'zip_txt',
@IS_ACTIVE_FLAG='Y',
@UOW_Description= 'zip_txt',
@retry_on_error_flag= NULL,
@severity_number= NULL,
@raise_ticket_flag= NULL,
@impact_number= NULL,
@Is_Reprocess_Flag= NULL,
@Update_Maintenance_System_Domain_Account_Name= NULL,
@Update_GMT_Timestamp= NULL,
@UOW_CODE= NULL

--Add UOW_ID

EXEC ABCR_AUDIT.USP_INSERT_BOW_CDC_AUX_CONTROL_R01
@TENANT_ID =112,
@BOW_ID =112001,
@SBOW_ID=11200102,
@UOW_ID =,
@CHANGE_DATA_CAPTURE_START_TIMESTAMP ='2021-01-01 00:00:00.0000000',
@CHANGE_DATA_CAPTURE_END_TIMESTAMP ='2022-10-19 15:48:02.0000000',
@CHANGE_DATA_CAPTURE_START_SEQUENCE_NUMBER =NULL,    
@CHANGE_DATA_CAPTURE_END_SEQUENCE_NUMBER=NULL,
@CDC_TYPE_DESCRIPTION ='Timestamp',
@UPDATE_MAINTENANCE_SYSTEM_DOMAIN_ACCOUNT_NAME =NULL,
@UPDATE_GMT_TIMESTAMP =NULL,
@BATCHID=NULL,
@MICROBATCHID=NULL


--Add Child and Parent UOW_ID

EXEC ABCR_CONTROL.USP_INSERT_ENTITY_MAPPING_CONTROL
@Tenant_ID =112,
@BOW_ID =112001,
@SBOW_ID=11200102,
@UOW_ID =,
@Parent_UOW_ID =,
@IS_ACTIVE ='Y',
@Update_Maintenance_System_Domain_Account_Name =NULL,
@Update_GMT_Timestamp =NULL


--replace 1120010200008 with  new UOW_ID 

Insert into ABCR_CONTROL.UOW_Configuration_Values 
values (112001,11200102,1120010200008,'EXTENSION','txt','Y',CURRENT_TIMESTAMP,NULL,SYSTEM_USER,NULL),
(112001,11200102,1120010200008,'FILE_PATTERN','file_pat AMS_Region_EDL_POS_$','Y',CURRENT_TIMESTAMP,NULL,SYSTEM_USER,NULL),
(112001,11200102,1120010200008,'DATA_FILE_PATH','/it/odw/global_abcr_pilot/dataingestion/output/gcw_test/','Y',CURRENT_TIMESTAMP,NULL,SYSTEM_USER,NULL),
(112001,11200102,1120010200008,'DELIMITER','29','Y',CURRENT_TIMESTAMP,NULL,SYSTEM_USER,NULL),
(112001,11200102,1120010200008,'IS_HEADER_INCLUDED','Y','Y',CURRENT_TIMESTAMP,NULL,SYSTEM_USER,NULL),
(112001,11200102,1120010200008,'IS_PART_FILE','N','Y',CURRENT_TIMESTAMP,NULL,SYSTEM_USER,NULL),
(112001,11200102,1120010200008,'AUDIT_CODE','latin-1','Y',CURRENT_TIMESTAMP,NULL,SYSTEM_USER,NULL),
(112001,11200102,1120010200008,'IS_FILE_VALIDATION','N','Y',CURRENT_TIMESTAMP,NULL,SYSTEM_USER,NULL),
(112001,11200102,1120010200008,'IS_REV_ID','N','Y',CURRENT_TIMESTAMP,NULL,SYSTEM_USER,NULL),
(112001,11200102,1120010200008,'IS_SRC_SYS_CD_SEQ','N','Y',CURRENT_TIMESTAMP,NULL,SYSTEM_USER,NULL),
(112001,11200102,1120010200008,'IS_AUDIT_FILE','N','Y',CURRENT_TIMESTAMP,NULL,SYSTEM_USER,NULL),
(112001,11200102,1120010200008,'LZ_TABLE_NAME','lz_ams_region_edl_pos','Y',CURRENT_TIMESTAMP,NULL,SYSTEM_USER,NULL),
(112001,11200102,1120010200008,'ARCHIVE_FILE_PATH','/it/odw/global_abcr_pilot/dataingestion/output/archive/gcw_test/','Y',CURRENT_TIMESTAMP,NULL,SYSTEM_USER,NULL),
(112001,11200102,1120010200008,'IS_COMPRESSED','N','Y',CURRENT_TIMESTAMP,NULL,SYSTEM_USER,NULL),
(112001,11200102,1120010200008,'SCHEMA_NAME','global_abcr_pilot','Y',CURRENT_TIMESTAMP,NULL,SYSTEM_USER,NULL),
(112001,11200102,1120010200008,'FILE_HEADER_RECORD','{"index":0,"schema": ["designator","subject","seq_num","date_format","ts_format"]}','Y',CURRENT_TIMESTAMP,NULL,SYSTEM_USER,NULL),
(112001,11200102,1120010200008,'FILE_FOOTER_RECORD','{"index":-1,"schema":footer_schema = ["designator","row_count","total_units","net_sales"]}','Y',CURRENT_TIMESTAMP,NULL,SYSTEM_USER,NULL),
(112001,11200102,1120010200008,'BUSINESS_VALIDATION_RULES','{"source": {"rule": {"Sales_Qty":"sum","NDP_USD_EXT":"sum"},"type": "data"},"target": {               "rule":{"total_units": "value","net_sales": "value"},"type": "footer"},"threshold": "0.1","col_mappings":{"Sales_Qty":"total_units","NDP_USD_EXT":"net_sales"}}','Y',CURRENT_TIMESTAMP,NULL,SYSTEM_USER,NULL)

EXEC ABCR_CONTROL.USP_INESRT_UOW_Configuration_Values 
@BOW_ID=112001,
@SBOW_ID=11200102,
@UOW_ID=1120010200008,
@Config_Key_Text='EXTENSION',
@Config_Value_Text='TXT',
@IS_Active_Flag='Y'

EXEC ABCR_CONTROL.USP_INSERT_UOW_Configuration_Values
@BOW_ID=112001,
@SBOW_ID=11200102,
@UOW_ID=1120010200008,
@Config_Key_Text='FILE_PATTERN',
@Config_Value_Text='file_pat AMS_Region_EDL_POS_$',
@IS_Active_Flag='Y'


EXEC ABCR_CONTROL.USP_INSERT_UOW_Configuration_Values
@BOW_ID=112001,
@SBOW_ID=11200102,
@UOW_ID=1120010200008,
@Config_Key_Text='DATA_FILE_PATH',
@Config_Value_Text='/it/odw/global_abcr_pilot/dataingestion/output/gcw_test/',
@IS_Active_Flag='Y'


EXEC ABCR_CONTROL.USP_INSERT_PROCESS_CONTROL
@Tenant_ID =112,  
@BOW_ID =112001,  
@SBOW_ID =11200102,
@UOW_ID =, 
@Stage_ID = 1,  
@Task_ID = 1,  
@Batch_ID =1,  
@Query_Type ='SPARK_SQL',  
@Query_Text ='insert into global_abcr_pilot.lz_ams_region_edl_pos select *, cast(concat(substring(string(current_timestamp),1,4),substring(string(current_timestamp),6,2), substring(string(current_timestamp),9,2), substring(string(current_timestamp),12,2), substring(string(current_timestamp),15,2), substring(string(current_timestamp),18,2)) as BigInt) as Partition_Load_Job_Number from @lz_view',  
@Schema_ID =,  
@SPARK_SETTINGS_TEXT =NULL,  
@IS_ACTIVE_FLAG = 'Y',  
@Source_Table_Name = 'FROM File',  
@Target_Table_Name ='global_abcr_pilot.lz_ams_region_edl_pos',  
@PARAMS_JSON_TEXT ='{"Service":"LandingLoadService"}',  
@Actions_Text ='INSERT_DATA_INTO_LZ',  
@Update_Maintenance_System_Domain_Account_Name =NULL,  
@Update_GMT_Timestamp = NULL









