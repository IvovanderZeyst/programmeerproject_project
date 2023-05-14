"""


"""
import pandas as pd
from typing import Dict


def __strip(column_data: 'pd.Series', charset: str) -> 'pd.Series':
    """Strip leading and trailing characters from a pandas Series.

    Args:
        column_data (pd.Series): The input Series to be stripped.
        charset (str): The characters to be stripped from the Series.

    Returns:
        pd.Series: The stripped Series.
    """
    column_data = __to_string(column_data)
    return column_data.apply(str.strip, args=(charset,))


def __regex(column_data: 'pd.Series', regex: str) -> 'pd.Series':
    column_data = __to_string(column_data)
    return column_data.str.extract(regex)


def __to_string(column_data: 'pd.Series') -> 'pd.Series':
    """Convert a pandas Series to a string data type.

    Args:
        column_data (pd.Series): The input Series to be converted.

    Returns:
        pd.Series: The converted Series as strings.
    """
    return column_data.astype(str)


def __to_float(column_data: 'pd.Series') -> 'pd.Series':
    """Convert a pandas Series to a float data type.

    Args:
        column_data (pd.Series): The input Series to be converted.

    Returns:
        pd.Series: The converted Series as floats.
    """
    return pd.to_numeric(column_data, errors='coerce').astype(float)


def __to_integer(column_data: 'pd.Series') -> 'pd.Series':
    """Convert a pandas Series to an integer data type.

    Args:
        column_data (pd.Series): The input Series to be converted.

    Returns:
        pd.Series: The converted Series as integers.
    """
    return column_data.astype('Int64')


def __to_datetime(column_data: 'pd.Series',
                  datetime_format: str = '%Y-%m-%d %H:%M:%S') -> 'pd.Series':
    """Convert a pandas Series to a datetime data type.

    Args:
        column_data (pd.Series): The input Series to be converted.
        datetime_format (str, optional): The format of the datetime data.
        Defaults to '%Y-%m-%d %H:%M:%S'.

    Returns:
        pd.Series: The converted Series as datetime.
    """
    return __to_string(pd.to_datetime(column_data, format=datetime_format))


def __to_bool(column_data: 'pd.Series', true_value: str = 'true',
              false_value: str = 'false') -> 'pd.Series':
    """
       Convert a pandas Series of strings to boolean values.

       Args:
           column_data (pd.Series): The Series containing strings to be
               converted to boolean values.
           true_value (str, optional): The string value that should be
               considered as True. Defaults to 'true'.
           false_value (str, optional): The string value that should be
               considered as False. Defaults to 'false'.

       Returns:
           pd.Series: The Series with boolean values.

       Example:
           column_data = pd.Series(['true', 'false', 'true', 'false'])
           bool_series = __to_bool(column_data, true_value='true',
               false_value='false')
       """
    return column_data.map({true_value: True, false_value: False})


def _apply_cleaning(column_data: 'pd.Series', clean_map: Dict[str, str]):
    """Apply cleaning operations on a pandas Series based on a clean_map.

    Args:
        column_data (pd.Series): The input Series to be cleaned.
        clean_map (Dict[str, str]): A dictionary containing the cleaning
            operations to be applied.

    Returns:
        pd.Series: The cleaned Series.
    """
    if 'strip' in clean_map:
        column_data = __strip(column_data, clean_map.get('strip'))
    if 'regex' in clean_map:
        column_data = __regex(column_data, clean_map.get('regex'))

    return column_data


def _apply_mapping(column_data: 'pd.Series', clean_map: Dict[str, str]):
    """Apply data type mapping operations on a pandas Series based on a
        clean_map.

    Args:
        column_data (pd.Series): The input Series to be mapped.
        clean_map (Dict[str, str]): A dictionary containing the data type
            mapping operations to be applied.

    Returns:
        pd.Series: The mapped Series.
    """
    data_type = clean_map.get('data_type', None)
    if not data_type:
        raise ValueError(f"'date_type' attribute not set")
    if data_type == 'string':
        return __to_string(column_data)
    if data_type == 'datetime':
        datetime_format = clean_map.get('datetime_format', None)
        if not datetime_format:
            raise ValueError(f"'datetime_format' attribute not set")
        return __to_datetime(column_data, datetime_format)
    if data_type == 'integer':
        return __to_integer(column_data)
    if data_type == 'float':
        return __to_float(column_data)
    if data_type == 'bool':
        true_value = clean_map.get('true_value', None)
        false_value = clean_map.get('false_value', None)
        return __to_bool(column_data, true_value, false_value)


def apply_clean_mapping(dataframe: 'pd.DataFrame',
                        clean_mapping: Dict[str, Dict[str, str]],
                        rename_columns: bool = False) -> 'pd.DataFrame':
    """
    Apply data cleaning and mapping to a pandas DataFrame.

    Args:
        dataframe (pd.DataFrame): The DataFrame to be cleaned.
        clean_mapping (Dict[str, Dict[str, str]]): A dictionary containing
            column names as keys and mapping rules as values. Each mapping
            rule is also a dictionary containing the following keys:
            - 'strip' (optional): A string of characters to be stripped from t
                he column data.
            - 'data_type' (required): The data type to be converted to.
                Can be one of 'string', 'datetime', 'integer',
              or 'float'.
            - 'datetime_format' (optional): The format of the datetime data,
                required only if 'data_type' is 'datetime'.
            - 'true_value' (optional): The string value that should be
                considered as True, required only if 'data_type' is 'bool'.
            - 'false_value' (optional): The string value that should be
                considered as False, required only if 'data_type' is 'bool'.

        rename_columns (bool, optional): If True, the cleaned DataFrame will
            have column names updated according to the clean_map.
            Defaults to False.

    Returns:
        pd.DataFrame: The cleaned DataFrame.

    Example:
        import pandas as pd
        clean_mapping = {'column1': {'strip': ' ', 'data_type': 'integer'},
                         'column2': {'data_type': 'float'},
                         'column3': {'data_type': 'bool', 'true_value': 'yes',
                         'false_value': 'no'}}
        dataframe = pd.DataFrame({'column1': [' 123 ', ' 456 '],
                                  'column2': [' 12.34 ', ' 56.78 '],
                                  'column3': ['yes', 'no']})

        cleaned_df = apply_clean_mapping(dataframe, clean_mapping,
            rename_columns=True)
        print(cleaned_df)

    Note:
        - The 'strip' attribute in clean_mapping is optional. If provided,
            it will strip the specified characters from
          the column data.
        - The 'data_type' attribute in clean_mapping is required and must be
            one of 'string', 'datetime', 'integer',
            or 'float'.
        - The 'datetime_format' attribute in clean_mapping is required only
            if 'data_type' is 'datetime', and it should
          be a valid datetime format.
        - The 'true_value' and 'false_value' attributes in clean_mapping are
            required only if 'data_type' is 'bool', and
          they specify the string values that should be considered as
            True and False, respectively.
        - If 'rename_columns' is set to True, the cleaned DataFrame will
            have column names updated according to the clean_map.
    """
    for column_name, clean_map in clean_mapping.items():
        if column_name == 'csv_read_options':
            continue
        dataframe[column_name] = _apply_cleaning(dataframe[column_name],
                                                 clean_map)
        dataframe[column_name] = _apply_mapping(dataframe[column_name],
                                                clean_map)

    if rename_columns:
        rename_mapping = {k: clean_mapping[k].get('rename_col_to', k)
                for k, v in clean_mapping.items() if k != 'csv_read_options'}
        dataframe = dataframe.rename(columns=rename_mapping)

    return dataframe
