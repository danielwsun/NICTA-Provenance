-------------------------------------------------------------------------------------------------------------------
-- Use this script to rank wifi hotspot temperature according to 'WifiStatus_2.csv'.
-- Data downloaded from 'data.gov.au'.
--
-- Date:	2016/3/1
-- Author:	Trams Wang
-- Version:	1.0
-------------------------------------------------------------------------------------------------------------------
REGISTER Provenance.jar;
REGISTER DemoUDF.jar;

DEFINE ProvInterStore 	com.nicta.provenance.pigudf.ProvInterStore('http', 'localhost', '8888');
DEFINE Clean 		test.CleanByRep('19');
DEFINE ConvertTime 	test.ConvertTime();
DEFINE CalRankScore 	test.CalculateRank();

---------------------------------------------------------------------------------------------------
raw = LOAD 'localhost/8888' USING com.nicta.provenance.pigudf.ProvLoader('WifiStatus_2.csv', 'raw');

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

timed_ranked = RANK timed_summed BY TotalDuration DESC;
timed_ranked = FOREACH timed_ranked GENERATE FLATTEN(ProvInterStore('timed_summed', 'TRanking', 'timed_ranked', *)) 
	AS (rank_timed_summed:long, LocationID:chararray, TotalDuration:long);

timed_result = FOREACH timed_ranked GENERATE LocationID, rank_timed_summed AS TimeRank;
STORE timed_result INTO 'localhost/8888' USING com.nicta.provenance.pigudf.ProvStorer('timed_ranked', 'TimedNaming', 'timed_result');

---------------------------------------------------------------------------------------------------
access_grouped = GROUP named BY LocationID;
access_grouped = FOREACH access_grouped GENERATE FLATTEN(ProvInterStore('named', 'AGrouping', 'access_grouped', *))
	AS (group:chararray, named:{(LocationID:chararray, FirstAccess:chararray, LastAccess:chararray, AccessCount:int)});

access_summed = FOREACH access_grouped GENERATE group AS LocationID, SUM(named.AccessCount) AS TotalAccesses;
access_summed = FOREACH access_summed GENERATE FLATTEN(ProvInterStore('access_grouped', 'AAccummulation', 'access_summed', *))
	AS (LocationID:chararray, TotalAccesses:long);

access_ranked = RANK access_summed BY TotalAccesses DESC;
access_ranked = FOREACH access_ranked GENERATE FLATTEN(ProvInterStore('access_summed', 'ARanking', 'access_ranked', *))
	AS (rank_access_summed:long, LocationID:chararray, TotalAccesses:long);

access_result = FOREACH access_ranked GENERATE LocationID, rank_access_summed AS AccessRank;
STORE access_result INTO 'localhost/8888' USING com.nicta.provenance.pigudf.ProvStorer('access_ranked', 'AccNaming', 'access_result');

---------------------------------------------------------------------------------------------------
combined = JOIN timed_ranked BY LocationID, access_ranked BY LocationID;
combined = FOREACH combined GENERATE FLATTEN(ProvInterStore('timed_ranked,access_ranked', 'Combinning', 'combined', *))
	AS (timed_ranked::rank_timed_summed:long, timed_ranked::LocationID:chararray, timed_ranked::TotalDuration:long,
	    access_ranked::rank_access_summed:long, access_ranked::LocationID:chararray, access_ranked::TotalAccess:long);

combined_named = FOREACH combined GENERATE timed_ranked::LocationID AS LocationID, timed_ranked::rank_timed_summed AS TimeRank, access_ranked::rank_access_summed AS AccessRank;
combined_named = FOREACH combined_named GENERATE FLATTEN(ProvInterStore('combined', 'CNaming', 'combined_named', *))
	AS (LocationID:chararray, TimeRank:long, AccessRank:long);

combined_scored = FOREACH combined_named GENERATE LocationID, CalRankScore(TimeRank, AccessRank) AS TotalScore:long;
combined_scored = FOREACH combined_scored GENERATE FLATTEN(ProvInterStore('combined_named', 'CScoring', 'combined_scored', *))
	AS (LocationID:chararray, TotalScore:long);

combined_ordered = ORDER combined_scored BY TotalScore ASC;
STORE combined_ordered INTO 'localhost/8888' USING com.nicta.provenance.pigudf.ProvStorer('combined_scored', 'CmbOrdering', 'combined_ordered');
