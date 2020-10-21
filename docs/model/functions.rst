Functions
=========


Evaluation Functions
--------------------

Evaluation functions represent statements (expressions) that are evaluated over one or more columns in a data frame. The evaluation function will always receive the full data frame and it is expected to return a data series or an array with a length that matches the number of rows in the data frame. The result may contain one or more columns.

The evaluation function implements the eval(df: pd.DataFrame) -> EvalResult function, where eval result is list or pd.DataSeries.


Conventions: an evaluation function the expects a single column should have an argument `column` of type `ColumnRef`. A evaluation function that accepts one or more columns should have a parameter `columns: ColumnsRef`.


Stream Consumers and Processors
-------------------------------

Stream consumers and processors are the building blocks for data pipelines. In a pilepline data is streamed row by row to the individual consumers.

The processor is needed to distinguish between the definition of a pipeline and the instantiation. The definition of a pipleine is composed of processors. Before streaming, the pipeline is instantiated as a chain of consumers. Internal consumer (i.e., consumer that pass their processed results on to another consumer in the pipeline are called ProducingConsumers since they act both as consumer and as producer of rows in the data stream. Consumers that do not pass data on but collect or aggregate results are called Collectors.

Note that streams in openclean come from files or data frames. They are not endless strems that disappear (what is the term fir this?). Instead, one can stream the same data multiple times, i.e., by reading the same file or iterating over the rows in a data frame.

Producers have the open method that gives them the schema. They return a consumer. ProducingConsumer have a schema associated with them (cploumns propery) that represents the schema of the rows that they produce. Collectors do not have to have a columns property.

The consumer has two methods. The consume method is called for every data row in the stream. The close method should return any collected or aggregated result (if any). For producing consumers the result returned at close is the result returned by the downstream consumer.

Prepared functions, e.g, Max cannot be part of a stream. 
