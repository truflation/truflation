# TFI-Data Pipeline

This repository is the home for `tfi-data`, a robust and versatile Python data pipeline that can be used to effectively and efficiently manage the flow of data processing, including ingestion, transformation, and export of data. This pipeline promotes loose coupling and enhances the maintainability and scalability of your code.

## Structure

The code is structured into a `Pipeline` and `PipelineDetails` to separate the high-level execution of tasks in the pipeline from the specifics of those tasks. The `Pipeline` class is initialized with an instance of `PipeLineDetails`, which provides the details for a specific pipeline configuration. More about these components can be found in the [overview](overview.md) document.

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
* **PipelineCoupler.py**: Combines `Pipeline.py` and `PipelineDetails` and schedules the running of the pipeline through APScheduler.
More details on these components are in the [overview](overview.md).

## Deployment

This pipeline is designed to be deployed as a Docker image for consistent deployment across different environments. More about deployment can be found in the [overview](overview.md) document.

## Repository Structure

This repository (`tfi-data`) contains the `Pipeline` class and its associated components necessary for data ingestion, transformation, and export. It does not include the module that calls and runs the `Pipeline` class or the details that get fed to the class. 

The details specific to the data processing, including `PipelineDetails`, are stored in a separate private repository named `dataloaders`.

## Contributing

Please read our [Contributing Guide](CONTRIBUTING.md) before submitting a Pull Request to the project.

## Support

If you're having any problem, please [raise an issue](https://github.com/truflationdev/truflation-data/issues/new) on GitHub.
