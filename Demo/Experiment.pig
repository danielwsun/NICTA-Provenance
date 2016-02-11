REGISTER Provenance.jar;
REGISTER DemoUDF.jar;
DEFINE ProvInterStore com.nicta.provenance.pigudf.ProvInterStore('http', 'localhost', '8888');
DEFINE Mark test.Marker();
DEFINE InterStore(srcvar, srcidx, processor, dstvar, dstrelation) RETURNS dstidx{
  sto = GROUP $dstrelation ALL;
  $dstidx = FOREACH sto GENERATE ProvInterStore('$srcvar', $srcidx.$0, '$processor', '$dstvar', sto.$dstrelation);
}
DEFINE InterStoreConst(srcvar, srcidx, processor, dstvar, dstrelation) RETURNS dstidx{
  sto = GROUP $dstrelation ALL;
  $dstidx = FOREACH sto GENERATE ProvInterStore('$srcvar', '$srcidx', '$processor', '$dstvar', sto.$dstrelation);
}

A = LOAD 'localhost/8888' USING com.nicta.provenance.pigudf.ProvLoader('money', 'A');

B = FOREACH A GENERATE *, Mark('a');
DUMP B;

C = FOREACH B GENERATE *, Mark('b');
DUMP C;

D = FOREACH C GENERATE *, Mark('c');
DUMP D;

STORE D INTO 'localhost/8888' USING com.nicta.provenance.pigudf.ProvStorer('D', 'iD');
