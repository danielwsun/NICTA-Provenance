#ifndef PROCESSORS_H
#define PROCESSORS_H
#include "Pipeline.hpp"
class Matrix : public Data
{
public:
  Matrix(int row_n, int col_n);
  ~Matrix();
  virtual void Record(const char *filename);
  virtual void* Clone();
  int* operator[](int row);
  int Rows();
  int Cols();
private:
  int rows, cols;
  int **matrix;
};


Data* Generate(Data *nothing);
Data* ReadExample(Data *filename);
Data* Transpose(Data *mat);
Data* SumByRow(Data *mat);
Data* SumByCol(Data *mat);
Data* FindMinMax(Data *mat);
Data* EstimateByMinMax(Data *mat);
Data* FindMiddle(Data *mat);
Data* EstimateByMiddle(Data *mat);

#endif
