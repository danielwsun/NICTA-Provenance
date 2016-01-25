#include "Processors.hpp"
#include <vector>
#include <algorithm>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define MAX_SIZE 7
#define MAX_VAL  100
/*
 * Some auxiliary paras
 */
extern Data *ans;
Matrix **examples = NULL;
Matrix **answers = NULL;
int example_idx = 0;

/*
 * Matrix
 */
Matrix::Matrix(int row_n, int col_n) : Data(), rows(row_n), cols(col_n)
{
  int *temp = (int*)calloc(row_n * col_n, sizeof(int));
  matrix = (int**)calloc(row_n, sizeof(int*));
  for (int i = 0; i < row_n; i++)
    {
      matrix[i] = temp + i * col_n;
    }
}

Matrix::~Matrix()
{
  free(matrix[0]);
  free(matrix);
}

void Matrix::Record(const char *filename)
{
  FILE *file = fopen(filename, "w");
  if (!file)
    {
      fprintf(stderr, "Record::Open file failed: %s\n", filename);
      return;
    }
  fprintf(file, "%d %d\n", rows, cols);
  for (int i = 0; i < rows; i++)
    {
      for (int j = 0; j < cols; j++)
	{
	  fprintf(file, "%5d", matrix[i][j]);
	}
      fprintf(file, "\n");
    }
  fclose(file);
}

void* Matrix::Clone()
{
  Matrix *mat = new Matrix(rows, cols);
  memcpy(mat->matrix[0], matrix[0], rows * cols * sizeof(int));
  return mat;
}

int* Matrix::operator[](int row)
{
  return matrix[row];
}

int Matrix::Rows(){return rows;}
int Matrix::Cols(){return cols;}

/*
 * Processors
 */
Data* Generate(Data* nothing)
{
  //long seed = time(NULL);
  //srand(seed);
  //printf("Seed: %ld\n", seed);
  int rows = rand() % MAX_SIZE + 1; //[1..MAX_SIZE]
  int cols = rand() % MAX_SIZE + 1; //[1..MAX_SIZE]
  Matrix *mat = new Matrix(rows, cols);
  int sum = 0;
  for (int i = 0; i < rows; i++)
    {
      for (int j = 0; j < cols; j++)
	{
	  (*mat)[i][j] = rand() % (2 * MAX_VAL - 1) - MAX_VAL + 1; //[-M..M]
	  sum += (*mat)[i][j];
	}
    }
  Matrix *temp_ans = new Matrix(1, 1);
  (*temp_ans)[0][0] = sum;
  ans = (Data*)temp_ans;
  
  return mat;
}

Data* ReadExample(Data *filename)
{
  if (NULL == examples)
    {
      FILE *file = fopen((const char*)filename, "r");
      if (NULL == file)
	{
	  fprintf(stderr, "Example file open failed: %s\n", (const char*)filename);
	  return NULL;
	}
      int total, rows, cols, sum;
      fscanf(file, "%d", &total);
      examples = (Matrix**)calloc(total, sizeof(Matrix*));
      answers = (Matrix**)calloc(total, sizeof(Matrix*));
      if (NULL == examples || NULL == answers)
	{
	  fprintf(stderr, "Example pool out of memory\n");
	  return NULL;
	}
      for (int k = 0; k < total; k++)
	{
	  fscanf(file, "%d %d", &rows, &cols);
	  Matrix *mat = new Matrix(rows, cols);
	  for (int i = 0; i < rows; i++)
	    {
	      for (int j = 0; j < cols; j++)
		{
		  fscanf(file, "%d", &((*mat)[i][j]));
		}
	    }
	  fscanf(file, "%d", &sum);
	  examples[k] = mat;
	  mat = new Matrix(1, 1);
	  (*mat)[0][0] = sum;
	  answers[k] = mat;
	}
      example_idx = 1;
      ans = answers[0];
      return examples[0];
    }
  ans = answers[example_idx];
  return examples[example_idx++];
}

Data* Transpose(Data *matrix)
{
  Matrix *m = (Matrix*)matrix;
  int rows = m->Rows();
  int cols = m->Cols();
  Matrix *mat = new Matrix(cols, rows);
  for (int i = 0; i < rows; i++)
    {
      for (int j = 0; j < cols; j++)
	{
	  (*mat)[j][i] = (*m)[i][j];
	}
    }
  return mat;
}

Data* SumByRow(Data *matrix)
{
  Matrix *m = (Matrix*)matrix;
  int rows = m->Rows();
  int cols = m->Cols();
  Matrix *mat = new Matrix(rows, 1);
  for (int i = 0; i < rows; i++)
    {
      int sum = 0;
      for (int j = 0; j < cols; j++)
	{
	  sum += (*m)[i][j];
	}
      (*mat)[i][0] = sum;
    }
  return mat;
}

Data* SumByCol(Data *matrix)
{
  Matrix *m = (Matrix*)matrix;
  int rows = m->Rows();
  int cols = m->Cols();
  Matrix *mat = new Matrix(1, cols);
  for (int j = 0; j < cols; j++)
    {
      int sum = 0;
      for (int i = 0; i < rows; i++)
	{
	  sum += (*m)[i][j];
	}
      (*mat)[0][j] = sum;
    }
  return mat;
}

Data* FindMinMax(Data *matrix)
{
  Matrix *m = (Matrix*)matrix;
  int rows = m->Rows();
  int cols = m->Cols();
  int min = MAX_VAL;
  int max = -MAX_VAL;
  for (int i = 0; i < rows; i++)
    {
      for (int j = 0; j < cols; j++)
	{
	  if (min > (*m)[i][j]) min = (*m)[i][j];
	  if (max < (*m)[i][j]) max = (*m)[i][j];
	}
    }
  Matrix *mat = new Matrix(1, 3);
  (*mat)[0][0] = min;
  (*mat)[0][1] = max;
  (*mat)[0][2] = rows * cols;
  return mat;
}

Data* EstimateByMinMax(Data *matrix)
{
  Matrix *m = (Matrix*)matrix;
  int sum = ((*m)[0][0] + (*m)[0][1]) * (*m)[0][2] / 2;
  Matrix *mat = new Matrix(1, 1);
  (*mat)[0][0] = sum;
  return mat;
}

Data* FindMiddle(Data *matrix)
{
  Matrix *m = (Matrix*)matrix;
  int rows = m->Rows();
  int cols = m->Cols();
  vector<int> v;
  for (int i = 0; i < rows; i++)
    {
      for (int j = 0; j < cols; j++)
	{
	  v.push_back((*m)[i][j]);
	}
    }
  sort(v.begin(), v.end());
  int size = rows * cols;
  Matrix *mat = new Matrix(1, 2);
  if (size % 2)
    {
      //odd
      (*mat)[0][0] = v[(size - 1) / 2];
      (*mat)[0][1] = size;
    }
  else
    {
      //even
      (*mat)[0][0] = (v[size / 2] + v[size / 2 - 1]) / 2;
      (*mat)[0][1] = size;
    }
  return mat;
}

Data* EstimateByMiddle(Data *matrix)
{
  Matrix *m = (Matrix*)matrix;
  Matrix *mat = new Matrix(1, 1);
  (*mat)[0][0] = (*m)[0][0] * (*m)[0][1];
  return mat;
}
