-------------------------------------------------------------------------------------------------------------------
-- Use this script to rank wifi hotspot temperature according to 'WifiStatusTotal.csv'.
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

raw = LOAD 'localhost/8888' USING com.nicta.provenance.pigudf.ProvLoader('WifiStatusTotal.csv', 'raw');

cleaned = FILTER (FOREACH raw GENERATE FLATTEN(Clean(*))) BY NOT ($0 MATCHES '');

named = FOREACH cleaned GENERATE (chararray)$1 AS LocationID:chararray,
				 (chararray)$3 AS FirstAccess:chararray,
				 (chararray)$4 AS LastAccess:chararray,
				 (int)$5 AS AccessCount:int
				 ;

timed = FOREACH named GENERATE LocationID, ConvertTime(*) AS Duration;

timed_grouped = GROUP timed BY LocationID;

timed_summed = FOREACH timed_grouped GENERATE group AS LocationID, SUM(timed.Duration) AS TotalDuration;

timed_ranked = RANK timed_summed BY TotalDuration DESC;

timed_result = FOREACH timed_ranked GENERATE LocationID, rank_timed_summed AS TimeRank;

STORE timed_result INTO 'localhost/8888' USING com.nicta.provenance.pigudf.ProvStorer('timed_ranked', 'TimedNaming', 'timed_result');

access_grouped = GROUP named BY LocationID;

access_summed = FOREACH access_grouped GENERATE group AS LocationID, SUM(named.AccessCount) AS TotalAccesses;

access_ranked = RANK access_summed BY TotalAccesses DESC;

access_result = FOREACH access_ranked GENERATE LocationID, rank_access_summed AS AccessRank;

STORE access_result INTO 'localhost/8888' USING com.nicta.provenance.pigudf.ProvStorer('access_ranked', 'AccNaming', 'access_result');

combined = JOIN timed_ranked BY LocationID, access_ranked BY LocationID;

combined_named = FOREACH combined GENERATE timed_ranked::LocationID AS LocationID, timed_ranked::rank_timed_summed AS TimeRank, access_ranked::rank_access_summed AS AccessRank;

combined_scored = FOREACH combined_named GENERATE LocationID, CalRankScore(TimeRank, AccessRank) AS TotalScore:long;

combined_ordered = ORDER combined_scored BY TotalScore ASC;

STORE combined_ordered INTO 'localhost/8888' USING com.nicta.provenance.pigudf.ProvStorer('combined_scored', 'CmbOrdering', 'combined_ordered');
