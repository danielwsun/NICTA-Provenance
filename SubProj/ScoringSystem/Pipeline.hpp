#ifndef PIPELINE_H
#define PIPELINE_H
#include <vector>
#include <string>

using namespace std;

class Data
{
public:
  Data();
  ~Data();
  virtual void Record(const char *filename) = 0;
  virtual void* Clone() = 0;
};

typedef Data* (*ProcFunc)(Data *data);

class Pipeline;

class PipeNode
{
  friend Pipeline;
public:
  PipeNode(const char* label, ProcFunc func);
  ~PipeNode();
  void AddSucc(PipeNode* node);
  double Score();
  void Score(double sc);
  PipeNode* pred;
  vector<PipeNode*> *succs;
  ProcFunc proc;
  string label;
  double score;
};

typedef void (*ScoreFunc)(PipeNode* node, Data *result, Data *ans);

class Pipeline
{
public:
  Pipeline();
  ~Pipeline();
  int AddNode(const char *label, ProcFunc func);
  void Transition(int src, int dst);
  void Execute(Data *data, const char *path);
  void Show();
  void RegisterScoreMethod(ScoreFunc funcu);
private:
  void Score(PipeNode* node, Data *result, Data *ans);
private:
  vector<PipeNode*> *nodes;
  ScoreFunc sf;
  FILE *log;
};

#endif
