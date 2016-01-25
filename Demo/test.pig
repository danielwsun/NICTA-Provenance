REGISTER Provenance.jar
DEFINE InterStore com.nicta.provenance.pigudf.ProvInterStore('http', 'localhost', '8888');
raw = LOAD 'localhost/8888' USING com.nicta.provenance.pigudf.ProvLoader('money', 'raw') AS (item:chararray, amt:int, paid:boolean);

grp = GROUP raw BY paid;
grp_sto = GROUP grp ALL;
grp_idx = FOREACH grp_sto GENERATE (chararray)InterStore('raw', 'money', 'GROUPER', 'grp', grp_sto.grp) AS idx:chararray;
DUMP grp_idx;

STORE grp INTO 'localhost/8888' USING com.nicta.provenance.pigudf.ProvStorer('grp', 'makeupindex');