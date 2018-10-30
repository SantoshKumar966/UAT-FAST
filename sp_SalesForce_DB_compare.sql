USE [myDB]
GO

/****** Object:  StoredProcedure [dbo].[sp_SalesForce_DB_compare]    Script Date: 5/26/2017 6:16:59 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE procedure [dbo].[sp_SalesForce_DB_compare] as
begin

DECLARE @SalesForce_table_name varchar(500)
DECLARE @SalesForce_column_name varchar(500)
DECLARE @SalesForce_data_type varchar(500)
DECLARE @DB_table_name varchar(500)
DECLARE @DB_column_name varchar(500)
DECLARE @DB_data_type varchar(500)
DECLARE @datatype_mismatch varchar(2000)
DECLARE @comments varchar(2000)
DECLARE @tupleCount int
DECLARE @column_count int
--------------------------------------------------------
DECLARE @MyCursor_v CURSOR
SET @MyCursor_v = CURSOR FAST_FORWARD
FOR
SELECT table_name,column_name,data_type from SalesForce_dataset

truncate table SalesForce_DB_compare_result;

OPEN @MyCursor_v
FETCH NEXT FROM @MyCursor_v
INTO @SalesForce_table_name,@SalesForce_column_name,@SalesForce_data_type
WHILE @@FETCH_STATUS = 0
BEGIN
--statements follows...
set @tupleCount= (select count(1) from AT_DB_dataset where table_name=@SalesForce_table_name)
if(@tupleCount>0)
begin
	set @column_count=(select count(1) from AT_DB_dataset where table_name=@SalesForce_table_name and column_name=@SalesForce_column_name)
	if(@column_count>0)
	begin
		insert into SalesForce_DB_compare_result(SalesForce_table_name,SalesForce_column_name,SalesForce_data_type,DB_table_name,DB_column_name,DB_data_type,datatype_mismatch,comments)
		select @SalesForce_table_name,@SalesForce_column_name,@SalesForce_data_type,table_name,column_name,data_type,null,'column exists in both'
			from AT_DB_dataset where table_name=@SalesForce_table_name and column_name=@SalesForce_column_name
	end
		else
		begin
			insert into SalesForce_DB_compare_result(SalesForce_table_name,SalesForce_column_name,SalesForce_data_type,DB_table_name,DB_column_name,DB_data_type,datatype_mismatch,comments)
			values(@SalesForce_table_name,@SalesForce_column_name,@SalesForce_data_type,@SalesForce_table_name,null,null,null,'Column not exist in DB')
		end
end
	else
		begin
			insert into SalesForce_DB_compare_result(SalesForce_table_name,SalesForce_column_name,SalesForce_data_type,DB_table_name,DB_column_name,DB_data_type,datatype_mismatch,comments)
			values(@SalesForce_table_name,@SalesForce_column_name,@SalesForce_data_type,null,null,null,null,'table does not exist in DB')
	end
FETCH NEXT FROM @MyCursor_v
INTO @SalesForce_table_name,@SalesForce_column_name,@SalesForce_data_type
END
CLOSE @MyCursor_v
DEALLOCATE @MyCursor_v

--------------------------------------------------------
--now checking reverse
DECLARE @MyCursor_m CURSOR
SET @MyCursor_m = CURSOR FAST_FORWARD
FOR
SELECT table_name,column_name,data_type from AT_DB_dataset
OPEN @MyCursor_m
FETCH NEXT FROM @MyCursor_m
INTO @DB_table_name,@DB_column_name,@DB_data_type
WHILE @@FETCH_STATUS = 0
BEGIN
--statements follows...
set @tupleCount= (select count(1) from SalesForce_dataset where table_name=@DB_table_name)
if(@tupleCount>0)
begin
	set @column_count=(select count(1) from SalesForce_dataset where table_name=@DB_table_name and column_name=@DB_column_name)
	if(@column_count=0)
		begin
			insert into SalesForce_DB_compare_result(SalesForce_table_name,SalesForce_column_name,SalesForce_data_type,DB_table_name,DB_column_name,DB_data_type,datatype_mismatch,comments)
			values(@DB_table_name,null,null,@DB_table_name,@DB_column_name,@DB_data_type,null,'Column not exist in SalesForce')
		end
end
	else
		begin
			insert into SalesForce_DB_compare_result(SalesForce_table_name,SalesForce_column_name,SalesForce_data_type,DB_table_name,DB_column_name,DB_data_type,datatype_mismatch,comments)
			values(null,null,null,@DB_table_name,@DB_column_name,@DB_data_type,null,'Table not exist in SalesForce')
	end
FETCH NEXT FROM @MyCursor_m
INTO @DB_table_name,@DB_column_name,@DB_data_type
END
CLOSE @MyCursor_m
DEALLOCATE @MyCursor_m

--------------------------------------------------------
--now checking data type mismatch

update SalesForce_DB_compare_result set datatype_mismatch='true' 
where case when SalesForce_data_type is null then DB_data_type else replace(ltrim(rtrim(replace(SalesForce_data_type,' ',''))),char(160),'') end <> case when DB_data_type is null then SalesForce_data_type else replace(ltrim(rtrim(replace(DB_data_type,' ',''))),char(160),'') end

--now update datatype_mismatch=NULL, for columns not present in vertica SIT
update [SalesForce_DB_compare_result] set datatype_mismatch=NULL
where DB_data_type is null and comments='Column not exist in DB'
and datatype_mismatch='true'--(26 row(s) affected)

--now update SalesForce_data_type_modified column to get a consolidated result

update [SalesForce_DB_compare_result] set datatype_mismatch=NULL
where SalesForce_data_type like 'string%'
and convert(int,substring(DB_data_type,CHARINDEX('(', DB_data_type,1)+1,CHARINDEX(')', DB_data_type,1)-CHARINDEX('(', DB_data_type,1)-1))>=
convert(int,substring(SalesForce_data_type,CHARINDEX('(', SalesForce_data_type,1)+1,CHARINDEX(')', SalesForce_data_type,1)-CHARINDEX('(', SalesForce_data_type,1)-1))

update [SalesForce_DB_compare_result] set datatype_mismatch=NULL
where SalesForce_data_type like 'picklist%' and DB_data_type like 'varchar%'
and convert(int,substring(DB_data_type,CHARINDEX('(', DB_data_type,1)+1,CHARINDEX(')', DB_data_type,1)-CHARINDEX('(', DB_data_type,1)-1))>=
convert(int,substring(SalesForce_data_type,CHARINDEX('(', SalesForce_data_type,1)+1,CHARINDEX(')', SalesForce_data_type,1)-CHARINDEX('(', SalesForce_data_type,1)-1))

update [SalesForce_DB_compare_result] set datatype_mismatch=NULL
where SalesForce_data_type like 'currency%' and DB_data_type like 'numeric%'
and
convert(int,substring(DB_data_type,CHARINDEX('(', DB_data_type,1)+1,CHARINDEX(',', DB_data_type,1)-CHARINDEX('(', DB_data_type,1)-1))>=
convert(int,substring(SalesForce_data_type,CHARINDEX('(', SalesForce_data_type,1)+1,CHARINDEX(',', SalesForce_data_type,1)-CHARINDEX('(', SalesForce_data_type,1)-1))
and
convert(int,substring(DB_data_type,CHARINDEX(',', DB_data_type,1)+1,CHARINDEX(')', DB_data_type,1)-CHARINDEX(',', DB_data_type,1)-1))>=
convert(int,substring(SalesForce_data_type,CHARINDEX(',', SalesForce_data_type,1)+1,CHARINDEX(')', SalesForce_data_type,1)-CHARINDEX(',', SalesForce_data_type,1)-1))


update [SalesForce_DB_compare_result] set datatype_mismatch=NULL
where SalesForce_data_type like 'textarea%' and DB_data_type like 'varchar%'
and convert(int,substring(DB_data_type,CHARINDEX('(', DB_data_type,1)+1,CHARINDEX(')', DB_data_type,1)-CHARINDEX('(', DB_data_type,1)-1))>=
convert(int,substring(SalesForce_data_type,CHARINDEX('(', SalesForce_data_type,1)+1,CHARINDEX(')', SalesForce_data_type,1)-CHARINDEX('(', SalesForce_data_type,1)-1))

update [SalesForce_DB_compare_result] set datatype_mismatch=NULL
where SalesForce_data_type like 'textarea%' and DB_data_type like 'long varchar%'
and convert(int,substring(DB_data_type,CHARINDEX('(', DB_data_type,1)+1,CHARINDEX(')', DB_data_type,1)-CHARINDEX('(', DB_data_type,1)-1))>=
convert(int,substring(SalesForce_data_type,CHARINDEX('(', SalesForce_data_type,1)+1,CHARINDEX(')', SalesForce_data_type,1)-CHARINDEX('(', SalesForce_data_type,1)-1))

update [SalesForce_DB_compare_result] set datatype_mismatch=NULL
where SalesForce_data_type like 'percent%' and DB_data_type like 'numeric%'
and
convert(int,substring(DB_data_type,CHARINDEX('(', DB_data_type,1)+1,CHARINDEX(',', DB_data_type,1)-CHARINDEX('(', DB_data_type,1)-1))>=
convert(int,substring(SalesForce_data_type,CHARINDEX('(', SalesForce_data_type,1)+1,CHARINDEX(',', SalesForce_data_type,1)-CHARINDEX('(', SalesForce_data_type,1)-1))
and
convert(int,substring(DB_data_type,CHARINDEX(',', DB_data_type,1)+1,CHARINDEX(')', DB_data_type,1)-CHARINDEX(',', DB_data_type,1)-1))>=
convert(int,substring(SalesForce_data_type,CHARINDEX(',', SalesForce_data_type,1)+1,CHARINDEX(')', SalesForce_data_type,1)-CHARINDEX(',', SalesForce_data_type,1)-1))

update [SalesForce_DB_compare_result] set datatype_mismatch=NULL
where SalesForce_data_type like 'double%' and DB_data_type like 'numeric%'
and
convert(int,substring(DB_data_type,CHARINDEX('(', DB_data_type,1)+1,CHARINDEX(',', DB_data_type,1)-CHARINDEX('(', DB_data_type,1)-1))>=
convert(int,substring(SalesForce_data_type,CHARINDEX('(', SalesForce_data_type,1)+1,CHARINDEX(',', SalesForce_data_type,1)-CHARINDEX('(', SalesForce_data_type,1)-1))
and
convert(int,substring(DB_data_type,CHARINDEX(',', DB_data_type,1)+1,CHARINDEX(')', DB_data_type,1)-CHARINDEX(',', DB_data_type,1)-1))>=
convert(int,substring(SalesForce_data_type,CHARINDEX(',', SalesForce_data_type,1)+1,CHARINDEX(')', SalesForce_data_type,1)-CHARINDEX(',', SalesForce_data_type,1)-1))

update [SalesForce_DB_compare_result] set datatype_mismatch=NULL
where SalesForce_data_type like 'datetime%' and DB_data_type like 'timestamp%'

update [SalesForce_DB_compare_result] set datatype_mismatch=NULL
where SalesForce_data_type like 'date%' and DB_data_type like 'timestamp%'

update [SalesForce_DB_compare_result] set datatype_mismatch=NULL
where SalesForce_data_type like 'date%' and DB_data_type like 'date%'

update [SalesForce_DB_compare_result] set datatype_mismatch=NULL
where SalesForce_data_type like 'int%' and DB_data_type like 'int%'

update [SalesForce_DB_compare_result] set datatype_mismatch=NULL
where SalesForce_data_type like 'int%' and DB_data_type like 'numeric(18,2)%'

update [SalesForce_DB_compare_result] set datatype_mismatch=NULL
where SalesForce_data_type like 'multipicklist%' and DB_data_type like 'varchar%'
and convert(int,substring(DB_data_type,CHARINDEX('(', DB_data_type,1)+1,CHARINDEX(')', DB_data_type,1)-CHARINDEX('(', DB_data_type,1)-1))>=
convert(int,substring(SalesForce_data_type,CHARINDEX('(', SalesForce_data_type,1)+1,CHARINDEX(')', SalesForce_data_type,1)-CHARINDEX('(', SalesForce_data_type,1)-1))

update [SalesForce_DB_compare_result] set datatype_mismatch=NULL
where SalesForce_data_type like 'phone%' and DB_data_type like 'varchar%'
and convert(int,substring(DB_data_type,CHARINDEX('(', DB_data_type,1)+1,CHARINDEX(')', DB_data_type,1)-CHARINDEX('(', DB_data_type,1)-1))>=
convert(int,substring(SalesForce_data_type,CHARINDEX('(', SalesForce_data_type,1)+1,CHARINDEX(')', SalesForce_data_type,1)-CHARINDEX('(', SalesForce_data_type,1)-1))

update [SalesForce_DB_compare_result] set datatype_mismatch=NULL
where SalesForce_data_type like 'id%' and DB_data_type like 'varchar%'
and convert(int,substring(DB_data_type,CHARINDEX('(', DB_data_type,1)+1,CHARINDEX(')', DB_data_type,1)-CHARINDEX('(', DB_data_type,1)-1))>=
convert(int,substring(SalesForce_data_type,CHARINDEX('(', SalesForce_data_type,1)+1,CHARINDEX(')', SalesForce_data_type,1)-CHARINDEX('(', SalesForce_data_type,1)-1))

update [SalesForce_DB_compare_result] set datatype_mismatch=NULL
where SalesForce_data_type like 'url%' and DB_data_type like 'varchar%'
and convert(int,substring(DB_data_type,CHARINDEX('(', DB_data_type,1)+1,CHARINDEX(')', DB_data_type,1)-CHARINDEX('(', DB_data_type,1)-1))>=
convert(int,substring(SalesForce_data_type,CHARINDEX('(', SalesForce_data_type,1)+1,CHARINDEX(')', SalesForce_data_type,1)-CHARINDEX('(', SalesForce_data_type,1)-1))

update [SalesForce_DB_compare_result] set datatype_mismatch=NULL
where SalesForce_data_type like 'email%' and DB_data_type like 'varchar%'
and convert(int,substring(DB_data_type,CHARINDEX('(', DB_data_type,1)+1,CHARINDEX(')', DB_data_type,1)-CHARINDEX('(', DB_data_type,1)-1))>=
convert(int,substring(SalesForce_data_type,CHARINDEX('(', SalesForce_data_type,1)+1,CHARINDEX(')', SalesForce_data_type,1)-CHARINDEX('(', SalesForce_data_type,1)-1))

update [SalesForce_DB_compare_result] set datatype_mismatch=NULL
where SalesForce_data_type like 'anyType%' and DB_data_type like 'varchar%'
and convert(int,substring(DB_data_type,CHARINDEX('(', DB_data_type,1)+1,CHARINDEX(')', DB_data_type,1)-CHARINDEX('(', DB_data_type,1)-1))>=
convert(int,substring(SalesForce_data_type,CHARINDEX('(', SalesForce_data_type,1)+1,CHARINDEX(')', SalesForce_data_type,1)-CHARINDEX('(', SalesForce_data_type,1)-1))

update [SalesForce_DB_compare_result] set datatype_mismatch=NULL
where SalesForce_data_type like 'encryptedstring%' and DB_data_type like 'varchar%'
and convert(int,substring(DB_data_type,CHARINDEX('(', DB_data_type,1)+1,CHARINDEX(')', DB_data_type,1)-CHARINDEX('(', DB_data_type,1)-1))>=
convert(int,substring(SalesForce_data_type,CHARINDEX('(', SalesForce_data_type,1)+1,CHARINDEX(')', SalesForce_data_type,1)-CHARINDEX('(', SalesForce_data_type,1)-1))

update SalesForce_DB_compare_result  set datatype_mismatch=NULL
where comments='column exists in both' and datatype_mismatch is not NULL
and charindex('__',SalesForce_data_type,1)>0 and DB_data_type in ('varchar(18)','varchar(500)')

update SalesForce_DB_compare_result  set datatype_mismatch=NULL
where SalesForce_data_type in ('Profile','UserRole','Group User','User','Product2','Asset','Account','address','Contact','Opportunity','RecordType',
'DandBCompany','BusinessHours','Campaign','Lead','Contract','Contact','Quote','PricebookEntry','CallCenter','Pricebook2','ExternalDataSource')
and DB_data_type in ('varchar(18)','varchar(19)','varchar(50)','varchar(500)')

--now update datatype_mismatch=NULL, for correct mapped columns

update [SalesForce_DB_compare_result] set datatype_mismatch=NULL
where SalesForce_data_type like 'boolean%' and DB_data_type in ('char(10)','varchar(10)','varchar(18)','varchar(20)','varchar(500)')
and datatype_mismatch='true'

end;



GO


