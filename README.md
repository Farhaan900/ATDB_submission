# Volcano-Processing-Prototype

RDBMS engine is equipped with versatile and flexible operations for processing data. A user given SQL statement is split into multiple granular operations. These operations then work together in producing output.

## Processing Model

Based on the processing engine the data are processed differently. In an abstract view, there are two different processing models

1.  Iterator-based
2. Compiled

In compiled execution, the SQL is parsed and compiled into a tightly wounded function and the data are executed. Though this method is highly efficient, it also requires complex re-engineering of operations to be adapted for execution. Hence, a more flexible method - volcano model is used.

## Volcano Model

Also known as iterator-based processing, the input is iteratively processed by the operator sequence mentioned in the SQL. This method was devised by Dr.Goetz Gräfe and is detailed in the paper

[Query evaluation techniques for large databases](https://web.stanford.edu/class/cs346/2014/graefe.pdf)

The operations in this technique, iteratively consume data from previous operation, process input data and pass it to the next operator. This provides the flexibility of processing any arbitrary functions without any rework.

### Driver
The flexibility of the process model is mainly due to the driver structure of the volcano operation. Any operation to be included in the model has to have implement the driver structure given below.

```C
open(){

  //All initialization instructions come here
}

next(){
  
  //All data processing instructions come here
  //This function must always forward result or EOL
}

close(){
  //Final bookkeeping and garbage collection instructions come here
}
```

## Prototype
This repository holds the bare-metal processing framework required for processing a given SQL statement. *processing/volcano.hpp* contains the skeletong for each of the operations that are used to evaluate a given SQL.

# Task

Your task is to implement the following

1. Implement missing operations in *volcano.hpp*
2. Implement the volcano based execution plan as well as compiled execution plan for the given queries

The results of both the models must evaluate to equal.

# Optional Tasks
3. Implement any two of the TPC-H queries ( [specification](http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.18.0.pdf) ). The input data can be generated and can be random integers
4. Try threading to have functional parallelism across the operations. 

# Details

1. The Query 3 
