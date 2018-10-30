/****
****Script Name   : Incr_NO_Budget_Bkp.sql
****Description   : Incremental data load for NO_Budget_Bkp
****/

SET SESSION AUTOCOMMIT TO ON;

/* Inserting values into Audit table  */
INSERT /*DIRECT*/ INTO swt_rpt_stg.FAST_LD_AUDT
 (
       SUBJECT_AREA
      ,TBL_NM
      ,LD_DT
      ,START_DT_TIME
      ,END_DT_TIME
      ,SRC_REC_CNT
      ,TGT_REC_CNT
      ,COMPLTN_STAT
      )
  select 'NetSUITE_OPENAIR','NO_Budget_Bkp',now()::date,now(),null,(select count(*) from "swt_rpt_stg"."NO_Budget_Bkp") ,null,'N';
  
 
 /* Inserting Stage table data into Historical Table */
  
insert /*DIRECT*/ into swt_rpt_stg.NO_Budget_Hist
(
id
,date
,name
,projectid
,total
,budgetcategory_id
,created
,currency
,customerid
,updated
,categoryid
,LD_DT
)
 select 
 id
,date
,name
,projectid
,total
,budgetcategory_id
,created
,currency
,customerid
,updated
,categoryid
,SYSDATE AS LD_DT
FROM "swt_rpt_base".NO_Budget_Bkp WHERE EXISTS
(SELECT 1 FROM "swt_rpt_stg".NO_Budget_Bkp STG 
WHERE STG.id = "swt_rpt_base".NO_Budget_Bkp.id AND STG.updated >= "swt_rpt_base".NO_Budget_Bkp.updated);



 /* Deleting before seven days data from current date in the Historical Table */  

delete /*DIRECT*/ from "air"."swt_rpt_stg"."NO_Budget_HIST"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,now())::date;

 
 


 /* Incremental VSQL script for loading data from Stage to Base */  

DELETE /*DIRECT*/ FROM "swt_rpt_base".NO_Budget_Bkp WHERE EXISTS
(SELECT 1 FROM "swt_rpt_stg".NO_Budget_Bkp STG 
WHERE STG.id = "swt_rpt_base".NO_Budget_Bkp.id AND STG.updated >= "swt_rpt_base".NO_Budget_Bkp.updated);

INSERT /*DIRECT*/ INTO "swt_rpt_base".NO_Budget_Bkp
(
	 id
	,date
	,name
	,projectid 
	,total
	,budgetcategory_id
	,created 
	,currency
	,customerid
	,updated
	,categoryid
	,SWT_INS_DT
)
SELECT
	 id
	,date
	,name
	,projectid 
	,total
	,budgetcategory_id
	,created 
	,currency
	,customerid
	,updated
	,categoryid
	,SWT_INS_DT
FROM
(
SELECT 
	 id
	,date
	,name
	,projectid 
	,total
	,budgetcategory_id
	,created 
	,currency
	,customerid
	,updated
	,categoryid
	,SYSDATE AS SWT_INS_DT
	,ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated DESC)RN
	FROM "swt_rpt_stg".NO_Budget_Bkp
)STG
	WHERE NOT EXISTS
	(SELECT 1 FROM "swt_rpt_base".NO_Budget_Bkp BASE
		WHERE STG.id = BASE.id)
		AND STG.RN = 1;	
		
		
/* Updating Audit status Flag and target table Counts */	
	
    update swt_rpt_stg.FAST_LD_AUDT set COMPLTN_STAT='Y',END_DT_TIME = now(),TGT_REC_CNT = (select count(*) from swt_rpt_base.NO_Budget_Bkp where SWT_INS_DT::date = now()::date)
    where SUBJECT_AREA = 'NetSUITE_OPENAIR' and
    TBL_NM = 'NO_Budget_Bkp' and
    COMPLTN_STAT = 'N' and
    SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'NetSUITE_OPENAIR' and  TBL_NM = 'NO_Budget_Bkp' and  COMPLTN_STAT = 'N');	
    
    
    /* This PURGE command is used for to delete delete vectors and Permanently removes deleted data from physical storage so that the disk space can be reused */ 
    SELECT PURGE_TABLE ( 'swt_rpt_stg.NO_Budget_HIST' );
    SELECT PURGE_TABLE ( 'swt_rpt_base.NO_Budget_Bkp' );
   
    
    /* Updating Statistics on the HIST and Base Tables*/
    select ANALYZE_STATISTICS('swt_rpt_base.NO_Budget_Bkp');
    select ANALYZE_STATISTICS('swt_rpt_stg.NO_Budget_HIST');

    
    
    
    
    

    