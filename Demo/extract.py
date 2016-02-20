f = open('PigTestTime.log', 'r');
res = open('PigTestTime.csv', 'w');
i=1;
for line in f:
    idx = line.find('(');
    if (-1 != idx):
        time = line[idx+1:-1].split(" ")[0];
        res.write("%s\n" % time);
        i += 1;
f.close();
res.close();

f = open('PigTestTimeProv.log', 'r');
res = open('PigTestTimeProv.csv', 'w');
i=1;
for line in f:
    idx = line.find('(');
    if (-1 != idx):
        time = line[idx+1:-1].split(" ")[0];
        res.write("%s\n" % time);
        i += 1;
f.close();
res.close();
