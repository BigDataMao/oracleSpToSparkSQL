# coding=utf-8
""" Create pyspark scheme structure from given hive table

"""
import subprocess
import argparse


def get_show_createtable_output(table_name):
    """ Runs cmd command
    >>> hive -e "show create table secret.phones;"

    Arguments:
        table_name (string): Table name in hive

    """
    return subprocess.check_output(['hive', '-e', '"show create table {};"'.format(table_name)]).decode('utf-8')


def get_pyspark_dtype(dtype):
    """ Returns pyspark datatype cohered with hive datatypes

    Pyspark datatypes overview:
        BinaryType – Binary data.
        BooleanType – Boolean values.
        ByteType – A byte value.
        DateType – A datetime value.
        DoubleType – A floating-point double value.
        IntegerType – An integer value.
        LongType – A long integer value.
        NullType – A null value.
        ShortType – A short integer value.
        StringType – A text string.
        TimestampType – A timestamp value (typically in seconds from 1/1/1970).
        UnknownType – A value of unidentified type.

    Arguments:
        dtype (string): Hive datatype

    """
    data_types = {
        'string': 'StringType()',
        'boolean': 'BooleanType()',
        'date': 'DateType()',
        'datetime': 'StringType()',  # coz Time type works terrible with different tzones
        'double': 'DoubleType()',
        'float': 'FloatType()',
        'byte': 'ByteType()',
        'int': 'IntegerType()',
        'bigint': 'LongType()',
        'smallint': 'ShortType()',
    }
    return data_types.get(dtype)


def format_show_createtable_output(out):
    """ Formats "show create table xxx.yyy" into List
    And enriches existing hive datatype with equal pyspark datatype

    Arguments:
        out (string): Result of "show create table ..." command looks like:

        >>> CREATE TABLE `secret.phones`(
        >>>     `phone` bigint comment "LOL",
        >>>     `phone_hash` string
        >>> )

    """
    out = out[out.find('(') + 1:out.find(')')]
    out = out.replace('`', '').replace('\n', '')

    data = []
    for column in out.split(','):
        column = column.strip()
        # append data type
        column_name, column_type = column.split(' ')[:2]

        data.append([
            column_name,
            column_type,
            get_pyspark_dtype(column_type)
        ])

    return data


def get_struct_field(column):
    """ Each hive column format formatted to pyspark StructField(column_name, column_data_type)

    Arguments:
        column (List): Enriched hive column parameters like:
        [column_name, hive_column_datatype, pyspark_column_datatype]

    """
    column = {'col_name': column[0], 'col_type': column[2]}
    struct_text_start = '\tStructField("{col_name}", {col_type}),\n'.format(**column)

    return struct_text_start


def generate_schema(data):
    """ Compiles pyspark StructType

    StructType([
        StructField("city", StringType()),
        StructField("country", StringType()),
        StructField("population", IntegerType())
    ])

    Arguments:
        data (List): List of Lists of columns in hive table like:
        [[column_name, hive_column_type],[column_name, hive_column_type], ...]

    """
    schema_text_headed = 'StructType([\n'
    schema_text_footer = '])'
    schema_text_body = ''

    for column in data:
        schema_text_body += get_struct_field(column)

    return schema_text_headed + schema_text_body + schema_text_footer


def main():
    """ Parses arguments and invoke all functions
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-t',
        action='store',
        type=str,
        help='Table name in format: "schema_name.table_name"',
        required=True
    )
    args = parser.parse_args()

    table_name = args.t
    out = get_show_createtable_output(table_name)
    formatted_data = format_show_createtable_output(out)
    generated_pyspark_dataframe_schema = generate_schema(formatted_data)

    print(generated_pyspark_dataframe_schema)


main()
