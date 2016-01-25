#include "ScoreMethods.hpp"
#include "Pipeline.hpp"
#include "Processors.hpp"
#include <stdlib.h>

void NaiveDistance(PipeNode *node, Data *result, Data *answer)
{
  Matrix *res = (Matrix*)result;
  Matrix *ans = (Matrix*)answer;
  double dist = abs((*res)[0][0] - (*ans)[0][0]);

  while(NULL != node)
    {
      node->score -= dist;
      node = node->pred;
    }
}
