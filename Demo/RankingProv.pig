-------------------------------------------------------------------------------------------------------------------
-- Use this script to rank wifi hotspot temperature according to 'WifiStatus50K_11.csv'.
-- Data downloaded from 'data.gov.au'.
--
-- Date:	2016/3/1
-- Author:	Trams Wang
-- Version:	1.1
-------------------------------------------------------------------------------------------------------------------
REGISTER Provenance.jar;
REGISTER DemoUDF.jar;

DEFINE ProvInterStore 	com.nicta.provenance.pigudf.ProvInterStore('http', 'localhost', '8888');
DEFINE Clean 		test.CleanByRep('19');
DEFINE ConvertTime 	test.ConvertTime();
DEFINE CalDensity	test.CalculateDensity();

---------------------------------------------------------------------------------------------------
raw = LOAD 'localhost/8888' USING com.nicta.provenance.pigudf.ProvLoader('WifiStatus50K_11.csv', 'raw');

cleaned = FILTER (FOREACH raw GENERATE FLATTEN(Clean(*))) BY NOT ($0 MATCHES '');
cleaned = FOREACH cleaned GENERATE FLATTEN(ProvInterStore('raw', 'Cleanning', 'cleaned', *));

named = FOREACH cleaned GENERATE (chararray)$1 AS LocationID:chararray,
				 (chararray)$3 AS FirstAccess:chararray,
				 (chararray)$4 AS LastAccess:chararray,
				 (int)$5 AS AccessCount:int
				 ;
named = FOREACH named GENERATE FLATTEN(ProvInterStore('cleaned', 'Naming', 'named', *))
	AS (LocationID:chararray, FirstAccess:chararray, LastAccess:chararray, AccessCount:int);

timed = FOREACH named GENERATE LocationID, ConvertTime(*) AS Duration;
timed = FOREACH timed GENERATE FLATTEN(ProvInterStore('named', 'ConvertTime', 'timed', *))
	AS (LocationID:chararray, Duration:long);

timed_grouped = GROUP timed BY LocationID;
timed_grouped = FOREACH timed_grouped GENERATE FLATTEN(ProvInterStore('timed', 'TGrouping', 'timed_grouped', *))
	AS (group:chararray, timed:{(LocationID:chararray, Duration:long)});

timed_summed = FOREACH timed_grouped GENERATE group AS LocationID, SUM(timed.Duration) AS TotalDuration;
timed_summed = FOREACH timed_summed GENERATE FLATTEN(ProvInterStore('timed_grouped', 'TAccumulation', 'timed_summed', *))
	AS (LocationID:chararray, TotalDuration:long);

timed_ordered = ORDER timed_summed BY TotalDuration DESC;
STORE timed_ordered INTO 'localhost/8888' USING com.nicta.provenance.pigudf.ProvStorer('timed_summed', 'TimedRanking', 'timed_ordered');

---------------------------------------------------------------------------------------------------
access_grouped = GROUP named BY LocationID;
access_grouped = FOREACH access_grouped GENERATE FLATTEN(ProvInterStore('named', 'AGrouping', 'access_grouped', *))
	AS (group:chararray, named:{(LocationID:chararray, FirstAccess:chararray, LastAccess:chararray, AccessCount:int)});

access_summed = FOREACH access_grouped GENERATE group AS LocationID, SUM(named.AccessCount) AS TotalAccesses;
access_summed = FOREACH access_summed GENERATE FLATTEN(ProvInterStore('access_grouped', 'AAccummulation', 'access_summed', *))
	AS (LocationID:chararray, TotalAccesses:long);

access_ordered = ORDER access_summed BY TotalAccesses DESC;
STORE access_ordered INTO 'localhost/8888' USING com.nicta.provenance.pigudf.ProvStorer('access_summed', 'AccRanking', 'access_ordered');

---------------------------------------------------------------------------------------------------
density = JOIN timed_summed BY LocationID, access_summed BY LocationID;
density = FOREACH density GENERATE FLATTEN(ProvInterStore('timed_summed,access_summed', 'Combinning', 'density', *))
	AS (timed_summed::LocationID:chararray, timed_summed::TotalDuration:long,
	    access_summed::LocationID2:chararray, access_summed::TotalAccesses:long);

density_scored = FOREACH density GENERATE timed_summed::LocationID AS LocationID,
	CalDensity(access_summed::TotalAccesses, timed_summed::TotalDuration) AS Density:double;
density_scored = FOREACH density_scored GENERATE FLATTEN(ProvInterStore('density', 'DScoring', 'density_scored', *))
	AS (Location:chararray, Density:double);

density_ordered = ORDER density_scored BY Density DESC;
STORE density_ordered INTO 'localhost/8888' USING com.nicta.provenance.pigudf.ProvStorer('density_scored', 'DensityOrdering', 'density_ordered');
