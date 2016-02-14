REGISTER Provenance.jar;
REGISTER DemoUDF.jar;
DEFINE ProvInterStore com.nicta.provenance.pigudf.ProvInterStore('http', 'localhost', '8888');
DEFINE Mark test.Marker();

A = LOAD 'localhost/8888' USING com.nicta.provenance.pigudf.ProvLoader('money', 'A');

B = FOREACH A GENERATE *, Mark('a');
B = FOREACH B GENERATE FLATTEN(ProvInterStore('A', 'M-A', 'B', *));

C = FOREACH B GENERATE *, Mark('b');
STORE C INTO 'localhost/8888' USING com.nicta.provenance.pigudf.ProvStorer('B', 'M-B', 'C');
