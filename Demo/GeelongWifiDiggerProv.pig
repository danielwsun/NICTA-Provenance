-------------------------------------------------------------------------------------------------------------------
-- Use this script to dig more infomation in data 'WifiStatusTotal.csv'.
-- Data downloaded from 'data.gov.au'.
--
-- Date:	2016/2/13
-- Author:	Trams Wang
-- Version:	2.0
-------------------------------------------------------------------------------------------------------------------

/*
 * Data stores in remote DS(Data Server) in Provenance system;
 * Thus we need to use loader/storer provided by Provenance system.
 * And we still need 'ProvInterStore' to store the intermediate result.
 */
REGISTER Provenance.jar;

/*
 * Provides some UDFs used in this test.
 */
REGISTER DemoUDF.jar;

/*
 * Log information will be processed in PS(Pipeline Server) in Provenance;
 * Use this function to store intermediate result in Provenance.
 */
DEFINE ProvInterStore com.nicta.provenance.pigudf.ProvInterStore('http', 'localhost', '8888');

/*
 * Define two kinds of data cleaning methods.
 */
DEFINE CleanByDel test.CleanByDel('19');
DEFINE CleanByRep test.CleanByRep('19');

-------------------------------------------------------------------------------------------------------------------
-- Sort Wifi locations according to the total accesses.
-- Using deleting strategy for cleaning.
-------------------------------------------------------------------------------------------------------------------
/* We may not assign the schema for now, because there will be some data that could not be
 * correctly parsed.*/
raw = LOAD 'localhost/8888' USING com.nicta.provenance.pigudf.ProvLoader('Lines_3K.csv', 'raw');

delcln = FILTER (FOREACH raw GENERATE FLATTEN(CleanByDel(*))) BY NOT ($0 MATCHES '');
delcln = FOREACH delcln GENERATE FLATTEN(ProvInterStore('raw', 'CleanByDelete', 'delcln', *));

/* The Commented columns are just for illustration that those data are not used in the
 * following process.
 * We are going to use these data to learn total accesses of each location.*/
delcln_loc_named = FOREACH delcln GENERATE --$0 AS VisitorID:chararray,
		   	   	      	    (chararray)$1 AS LocationID:chararray,
					    --$2 AS TrackSequence:int,
					    (chararray)$3 AS FirstAccess:chararray,
					    (chararray)$4 AS LastAccess:chararray,
					    (int)$5 AS AccessCount:int
					    --$6 AS vID:chararray,
					    --$7 AS BrowserAgent:chararray,
					    --$8 AS TargetURL:chararray,
					    --$9 AS Name:chararray,
					    --$10 AS Address:chararray,
					    --$11 AS SrcIPAddr:chararray,
					    --$12 AS LocationName:chararray,
					    --$13 AS City:chararray,
					    --$14 AS State:chararray,
					    --$15 AS Postcode:int,
					    --$16 AS PublicIPAddr:chararray,
					    --$17 AS Longitude:double,
					    --$18 AS Latitude:double
					    ;
delcln_loc_named = FOREACH delcln_loc_named GENERATE FLATTEN(ProvInterStore('delcln', 'DelLocFilter', 'delcln_loc_named', *)) AS (LocationID:chararray, FirstAccess:chararray, LastAccess:chararray, AccessCount:int);

/* Group data up according to LocationID.*/
delcln_loc_grouped = GROUP delcln_loc_named BY LocationID;
delcln_loc_grouped = FOREACH delcln_loc_grouped GENERATE FLATTEN(ProvInterStore('delcln_loc_named', 'DelLocGrouper', 'delcln_loc_grouped', *)) AS (group:chararray, delcln_loc_named:{});

/* Count amount of each group.*/
delcln_loc_counted = FOREACH delcln_loc_grouped GENERATE group AS LocationID:chararray, COUNT(delcln_loc_named) AS TotalAccess:long;
delcln_loc_counted = FOREACH delcln_loc_counted GENERATE FLATTEN(ProvInterStore('delcln_loc_grouped', 'DelLocCounter', 'delcln_loc_counted', *)) AS (LocationID:chararray, TotalAccess:long);

/* Sort result.*/
delcln_loc_ordered = ORDER delcln_loc_counted BY TotalAccess DESC;

/* Store result*/
STORE delcln_loc_ordered INTO 'localhost/8888' USING com.nicta.provenance.pigudf.ProvStorer('delcln_loc_counted', 'DelLocOrdering', 'delcln_loc_ordered');

-------------------------------------------------------------------------------------------------------------------
-- Sort URLs according to total access.
-- Using deleting strategy for cleaning.
-------------------------------------------------------------------------------------------------------------------
/* Filter useful columns.*/
delcln_url_named = FOREACH delcln GENERATE (chararray)$8 AS TargetURL:chararray,
		   	   	  	   (long)$5 AS AccessCount:long;

delcln_url_grouped = GROUP delcln_url_named BY TargetURL;

delcln_url_summed = FOREACH delcln_url_grouped GENERATE group AS TargetURL:chararray, SUM(delcln_url_named.AccessCount) AS TotalAccess:long;

delcln_url_ordered = ORDER delcln_url_summed BY TotalAccess DESC;

--STORE delcln_url_ordered INTO 'localhost/8888' USING com.nicta.provenance.pigudf.ProvStorer('delcln_url_ordered', 'delcln_url_ordered_idx');

-------------------------------------------------------------------------------------------------------------------
-- Sort Wifi locations according to the total accesses.
-- Using repairing strategy for cleaning.
-------------------------------------------------------------------------------------------------------------------
repcln = FILTER (FOREACH raw GENERATE flatten(CleanByRep(*))) BY NOT ($0 MATCHES '');

repcln_loc_named = FOREACH repcln GENERATE (chararray)$1 AS LocationID:chararray,
		   	   	      	   (chararray)$3 AS FirstAccess:chararray,
					   (chararray)$4 AS LastAccess:chararray,
					   (int)$5 AS accessCount:int;

repcln_loc_grouped = GROUP repcln_loc_named BY LocationID;

repcln_loc_counted = FOREACH repcln_loc_grouped GENERATE group AS LocationID:chararray, COUNT(repcln_loc_named) AS TotalAccess:long;

repcln_loc_ordered = ORDER repcln_loc_counted BY TotalAccess DESC;

--STORE repcln_loc_ordered INTO 'localhost/8888' USING com.nicta.provenance.pigudf.ProvStorer('repcln_loc_ordered', 'repcln_loc_ordered_idx');

-------------------------------------------------------------------------------------------------------------------
-- Sort URLs according to total access.
-- Using repairing strategy for cleaning.
-------------------------------------------------------------------------------------------------------------------
repcln_url_named = FOREACH repcln GENERATE (chararray)$8 AS TargetURL:chararray,
		      	      		   (long)$5 AS AccessCount:long;

repcln_url_grouped = GROUP repcln_url_named BY TargetURL;

repcln_url_summed = FOREACH repcln_url_grouped GENERATE group AS TargetURL:chararray, SUM(repcln_url_named.AccessCount) AS TotalAccess:long;

repcln_url_ordered = ORDER repcln_url_summed BY TotalAccess DESC;

--STORE repcln_url_ordered INTO 'localhost/8888' USING com.nicta.provenance.pigudf.ProvStorer('repcln_url_ordered', 'repcln_url_ordered_idx');
