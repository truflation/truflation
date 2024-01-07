# TFI-Data Pipeline

This repository is the home for `tfi-data`, a robust and versatile Python data pipeline that can be used to effectively and efficiently manage the flow of data processing, including ingestion, transformation, and export of data. This pipeline promotes loose coupling and enhances the maintainability and scalability of your code.

## Structure

The modularized ingestor is structured into two components --  `Pipeline` and `PipelineDetails` -- to separate the high-level execution of tasks in the pipeline from the specifics of those tasks. The `Pipeline` class is initialized with an instance of `PipeLineDetails`, which provides the details for a specific pipeline configuration. More about these components can be found in the [overview](overview.md) document.

## Usage

You can use this module by either cloning the repository or downloading the Docker image (link to the image will be provided in the future).

1. Clone this repository or download the Docker image.
2. Create a Python script or use the provided `pipeline_connector.py` and pass in the location of your `PipelineDetails`. If you prefer, you can import the `pipeline_details.py` module and create your own implementation.
3. Install with `pip install .`
4. Run the pipline
   1. Using Docker:
        ```docker-compose -f docker-compose-example-csv.yml up -- build```
        Note: An example implementation of the pipeline can be found [here](https://github.com/truflationdev/truflation-data/tree/main/examples/csv_example).
   2. Without Docker:
       `src/truflation/data/pipeline_coupler.py path_to_my_pipline_details.py`

## Components

* **Pipeline.py**: Manages the data pipeline and coordinates the listed components.
* **GeneralLoader**: Reads and parses data from specified sources and stores it in a cache.
* **Validator**: Validates the ingested data based on certain rules or conditions.
* **Transformer**: Transforms the loaded data according to specified rules or functions.
* **Exporter**: Exports a dataframe into a database according to the export details parameter.
* **PipelineDetails**: Contains details for a specific pipeline configuration.
* **pipeline_coupler.py**: Combines `Pipeline.py` and `PipelineDetails` and schedules the running of the pipeline through APScheduler.
* **multi_pipeline_coupler.py**: A pipeline coupler that runs multiple pipelines asynchronously. Pipelines are already meant to be asynchronous to other pipelines. In this script, the pipeline itself becomes asynchronous and thus could have speed advantages even with singular pipelines. 
* **pipeline_run_direct.py**: Combines `Pipeline.py` and `PipelineDetails` and immediately runs pipeline without scheduling.
More details on these components are in the [overview](overview.md).

## Deployment

This pipeline is designed to be deployed as a Docker image for consistent deployment across different environments. More about deployment can be found in the [overview](overview.md) document.

## Repository Structure

This repository (`tfi-data`) contains the `Pipeline` class and its associated components necessary for data ingestion, transformation, and export. It does not include the module that calls and runs the `Pipeline` class or the details that get fed to the class. However, there are examples in the repo of implementing and calling PipelineDetails.

The details specific to the data processing, including `PipelineDetails`, are stored in a separate private repository named `dataloaders`.




# Pipeline Class

The `Pipeline` class orchestrates the ingestion, transformation, and exporting of data. This class inherits from the `Task` base class.

## Attributes

- `name`: The unique identifier of the pipeline.
- `pre_ingestion_function`: A callable function executed before the ingestion process starts.
- `post_ingestion_function`: A callable function that runs after the completion of the ingestion process.
- `sources`: A dictionary of data sources where the pipeline should ingest data from.
- `loader`: An instance of `GeneralLoader` to handle the ingestion and parsing of data.
- `transformer`: An instance of `Transformer` to handle the transformation of the ingested data.
- `exports`: A list of exports where the pipeline should send the data.
- `exporter`: An instance of `Exporter` to handle the exporting of data.

## Methods

- `ingest()`: Executes the entire pipeline process, which includes data ingestion, transformation, and exporting.
- `header(s: str)`: Prints a header for a section of the pipeline process.

The `ingest()` function is the core function, orchestrating the process from start to finish. It begins by executing the `pre_ingestion_function`, then it reads, parses, and validates data from all sources. After that, it performs the necessary transformations and stores the result in a cache. Finally, it exports the transformed data to the specified destinations and runs the `post_ingestion_function`.


# PipeLineDetails Class

This class provides a detailed configuration for a data pipeline.

## Components

- `name`: The unique identifier for the pipeline.
- `sources`: List of `SourceDetails` objects, each outlining the setup for a specific data source.
- `exports`: List of `ExportDetails` objects, each describing how to handle data exports for the pipeline.
- `cron_schedule`: A dictionary defining the pipeline's schedule based on the cron format.
- `pre_ingestion_function`: This optional callable function runs before the data ingestion process begins.
- `post_ingestion_function`: Another optional callable function that is executed after the ingestion process.
- `transformer`: A function that manipulates the ingested data as needed before it is exported.

# SourceDetails Class

This class encapsulates the details of a data source that is part of the pipeline.

## Components

- `name`: Unique identifier for the data source.
- `source_type`: Specifies the type of the data source, such as 'csv', 'rest+http', 'sqlite', etc.
- `source`: Specifies the particular source of data. This could be a URL, a file path, etc. depending on `source_type`.
- `connector`: An instance of `Connector` class used to establish a connection with the data source.
- `parser`: A function to process the data from the source and transform it into a pandas DataFrame.

# ExportDetails Class

This class outlines the details for exporting data processed by the pipeline.

## Components

- `name`: Unique identifier for the export task.
- `connector`: Specifies the `Connector` instance used for data operations.
- `key`: Used for reading and writing data.

## Methods

- `read()`: Reads data using the assigned key.
- `write(data)`: Writes the provided pandas DataFrame to the assigned key.




## Contributing

Please read our [Contributing Guide](CONTRIBUTING.md) before submitting a Pull Request to the project.

## Support

If you're having any problem, please [raise an issue](https://github.com/truflationdev/truflation-data/issues/new) on GitHub.
