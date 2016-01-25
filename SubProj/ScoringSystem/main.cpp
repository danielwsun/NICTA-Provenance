#include <stdio.h>
#include "Pipeline.hpp"
#include "Processors.hpp"
#include "ScoreMethods.hpp"
#include <time.h>
#include <stdlib.h>

#define TOTAL 10000
#define EXAMPLE_FILE "Data/Example.txt"

int main(int argc, char* argv[])
{
  if (2 != argc)
    {
      fprintf(stderr, "Usage: %s <Times>\n", argv[0]);
      return 0;
    }
  
  Pipeline pp;
  int gen = pp.AddNode("Pump", Generate);
  int sum_row = pp.AddNode("SumRow", SumByRow);
  int sum_col = pp.AddNode("SumCol", SumByCol);
  int transpose = pp.AddNode("Tran", Transpose);
  int find_min_max = pp.AddNode("FdMinMax", FindMinMax);
  int est_min_max = pp.AddNode("EstMinMax", EstimateByMinMax);
  int find_middle = pp.AddNode("FdMed", FindMiddle);
  int est_by_middle = pp.AddNode("EstMed", EstimateByMiddle);

  pp.Transition(gen, sum_row);
  pp.Transition(sum_row, sum_col);

  pp.Transition(gen, transpose);
  pp.Transition(transpose, find_min_max);
  pp.Transition(find_min_max, est_min_max);

  pp.Transition(transpose, find_middle);
  pp.Transition(find_middle, est_by_middle);

  pp.RegisterScoreMethod(NaiveDistance);

  //pp.Show();
  srand(time(NULL));
  int times = atoi(argv[1]);
  
  for (int i = 0; i < times; i++)
    {
      pp.Execute((Data*)NULL+1, "../data");
    }

  pp.Show();
  
  return 0;
}
