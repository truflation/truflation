from typing import Callable, Union, Dict
import pandas
from truflation.data.connector import Connector
import json


class SourceDetails:
    """
    SourceDetails is a class that encapsulates the details for a data source.
    These details include the name of the source, the type of the source, the
    specific source, the connector used to connect to the source, and a parser
    function to process the data from the source.

    This class serves as a configuration object, providing all necessary details
    required to read from a particular data source.

    Parameters
    ----------
    name: str
        A string that represents the name of the data source.

    source_type: str
        A string that indicates the type of the data source. The following are valid:
            cache
            csv
            gsheet
            json
            playwright+http
            rest+http
            http
            sqlite
            postgresql
            mysql
            mariadb
            oracle
            mssql
            sqlalchemy
            gsheets
            pybigquery


    source: str
        A string that specifies the particular source of data. The interpretation
        of this string depends on the source_type. For a CSV file, for example,
        this would be the file path.

    connector: Connector (default = None)
        An instance of a Connector class or its subclass used to establish a
        connection with the data source. This is optional and if not provided,
        a default connector may be used.

    parser: Callable[[Union[pandas.DataFrame, Dict, json]], pandas.DataFrame] (default = lambda x: x)
        A function that takes a pandas DataFrame as input and returns a
        pandas DataFrame as output. This is used to perform any necessary
        transformations or preprocessing on the data read from the source.
        By default, this is an identity function that returns the input as is.

    Attributes
    ----------
    name: str
        Name of the data source.

    source_type: str
        Type of the data source.

    source: str
        Specific source of data.

    connector: Connector | None
        Connector instance used to connect to the data source.

    parser: Callable[[Union[pandas.DataFrame, Dict, json]] | None
        Parser function used to process the data from the source.

    transformer: Callable[pandas.DataFrame] | None
        Process dataframe after parsing

    transformer_kwargs: Dict | None
        Used to pass in kwargs to transformer

    kwargs: Dict
        key-ward arguments
    """

    def __init__(self, name: str, source_type: str, source: str, *args, connector: Connector = None,
                 parser: Callable[[Union[pandas.DataFrame, Dict]], pandas.DataFrame] = None,
                 transformer=None, transformer_kwargs=None, **kwargs):
        self.name = name
        # options: override, csv,
        self.source_type = source_type
        self.source = source
        self.connector = connector  # instance of overriden class
        self.parser = parser  # parser is run on the dataframe that is returned
        self.args = args
        self.transformer = transformer
        self.transformer_kwargs = transformer_kwargs
        self.kwargs = kwargs
