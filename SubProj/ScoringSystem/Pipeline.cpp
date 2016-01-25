#include "Pipeline.hpp"
#include <stdio.h>
#include <time.h>
#include <string.h>
#include <stdlib.h>

Data *ans = NULL;

/*
 * Data
 */
Data::Data(){}

Data::~Data(){}
/*
 * PipeNode
 */
PipeNode::PipeNode(const char *lb, ProcFunc func) : proc(func), pred(NULL), score(0)
{
  succs = new vector<PipeNode*>();
  label = lb;
}

PipeNode::~PipeNode()
{
  delete succs;
}

void PipeNode::AddSucc(PipeNode *node)
{
  succs->push_back(node);
  node->pred = this;
}

double PipeNode::Score()
{
  return score;
}

void PipeNode::Score(double sc)
{
  score = sc;
}

/*
 * Pipeline
 */
Pipeline::Pipeline(): sf(NULL)
{
  nodes = new vector<PipeNode*>();
  log = fopen("Data/logs.txt", "w");
}

Pipeline::~Pipeline()
{
  for (vector<PipeNode*>::iterator it = nodes->begin(); it < nodes->end(); it++)
    {
      delete *it;
    }
  delete nodes;
  fclose(log);
}

int Pipeline::AddNode(const char *label, ProcFunc func)
{
  nodes->push_back(new PipeNode(label, func));
  return nodes->size() - 1;
}

void Pipeline::Transition(int src, int dst)
{
  (*nodes)[src]->AddSucc((*nodes)[dst]);
}

void Pipeline::Execute(Data *data, const char *path)
{
  static unsigned int id = 0;
  vector<PipeNode*> queue;
  vector<Data*> data_queue;
  vector<unsigned int> id_queue;
  
  queue.push_back((*nodes)[0]);
  data_queue.push_back(data);
  id_queue.push_back(++id);

  for (int i = 0; i < queue.size(); i++)
    {
      id++;
      if (NULL == queue[i]->proc)
	{
	  fprintf(stderr, "NULL proccessor occured!\n");
	  return;
	}
      else
	{
	  //fprintf(stdout, "Executing: %s...\n", queue[i]->label.c_str());
	  clock_t start = clock();
	  Data *temp_data = queue[i]->proc(data_queue[i]);
	  clock_t duration = clock() - start;
	  if (NULL == data)
	    {
	      fprintf(stderr, "Null Data found after: %s, abort\n", queue[i]->label.c_str());
	      return;
	    }
	  vector<PipeNode*> *temp_succs = queue[i]->succs;
	  char filename[32];
	  char strid[32];
	  strcpy(filename, path);
	  strcat(filename, "/");
	  sprintf(strid, "%x", id);
	  strcat(filename, strid);
	  temp_data->Record(filename);
	  //write to log
	  fprintf(log, "%x %s %x %ld\n", id_queue[i], queue[i]->label.c_str(), id, duration);  //srcid, proc, dstid, duration
	  
	  for (int j = 0; j < temp_succs->size(); j++)
	    {
	      queue.push_back((*temp_succs)[j]);
	      data_queue.push_back(temp_data);
	      id_queue.push_back(id);
	    }
	  if (0 == temp_succs->size()) //last node in a path
	    {
	      //fprintf(stdout, "Scoring after: %s...\n", queue[i]->label.c_str());
              Score(queue[i], temp_data, ans);
	      //printf("Scoring done\n");
	      //fflush(stdout);
	    }
	}
    }
}

void Pipeline::Show()
{
  fprintf(stdout, "===\n");
  for (int i = 0; i < nodes->size(); i++)
    {
      fprintf(stdout, "%s<%f>:", (*nodes)[i]->label.c_str(), (*nodes)[i]->score);
      /*for (int j = 0; j < (*nodes)[i]->succs->size(); j++)
	{
	  fprintf(stdout, "%s, ", (*(*nodes)[i]->succs)[j]->label.c_str());
	  }*/
      fprintf(stdout, "\n");
    }
  fprintf(stdout, "===\n");
}

void Pipeline::Score(PipeNode *node, Data *result, Data *ans)
{
  if (NULL == sf) return;
  sf(node, result, ans);
}

void Pipeline::RegisterScoreMethod(ScoreFunc func)
{
  sf = func;
}
