=========================
Data Streams in openclean
-------------------------

Stream consumers and processors are the building blocks for data pipelines. In a piepline data is streamed row by row to the individual consumers.

The processor is needed to distinguish between the definition of a pipeline and the instantiation. The definition of a pipeline is composed of processors. Before streaming, the pipeline is instantiated as a chain of consumers. Internal consumer (i.e., consumer that pass their processed results on to another consumer in the pipeline are called **producing consumers** since they act both as consumer and as producer of rows in the data stream. Consumers that do not pass data on but collect or aggregate results are called **collectors**.

Note that streams in openclean come from files or data frames. They are not endless streams that disappear (what is the term fir this?). Instead, one can stream the same data multiple times, i.e., by reading the same file or iterating over the rows in a data frame.

Producers have the open method that gives them the schema. They return a consumer. ProducingConsumer have a schema associated with them (columns property) that represents the schema of the rows that they produce. Collectors do not have to have a columns property.

The consumer has two methods. The consume method is called for every data row in the stream. The close method should return any collected or aggregated result (if any). For producing consumers the result returned at close is the result returned by the downstream consumer.

Prepared functions, e.g, Max cannot be part of a stream.
