import csv
import json
import avro
from avro.datafile import DataFileWriter
from avro.io import DatumWriter


def guess_content_type(content: any) -> type[any]:
    """from csv file we can extract only numbers (integers and floats) and text
    gonna be used for avro schema file generation and converting any type of csv"""
    try:
        var_type = type(int(content))
    except ValueError:
        try:
            var_type = type(float(content))
        except ValueError:
            var_type = type(content)
    return var_type


def decide_common_type(stored: type[any], current: type[any]) -> type[any]:
    """to decide type of a column iteratively comparing already decided type and current type
    there is following consideration - integer is always lower type, then float, then string"""
    if stored == current:
        return stored
    elif stored == int:
        # stored and current different but stored has a lower type so taking current
        return current
    elif stored == float and current != int:
        # stored and current different, current is string in this case
        return current
    else:
        # stored is string here
        return stored


def parse_csv(csv_path: str) -> tuple[list[str], list[list[any]], dict[str, type[any]]]:
    """spliting csv into necessary parts - columns, values, and types

    Returns tuple[list[str], list[list[any]], dict[str, type[any]]] where are:=
    list[str] - column names of csv
    list[list[any]] - collection of rows which is collection of values itself
    dict[str, type[any]] - mapping of column name to its type
    """
    with open(csv_path, 'r') as csv_content:
        csv_reader = csv.DictReader(csv_content)
        csv_contents = []
        type_guesses = {}
        # avro cannot contain spaces in fields so we have to replace this symbol
        columns = [column.replace(' ', '_') for column in csv_reader.fieldnames]
        for csv_content in csv_reader:
            csv_contents.append([])
            for column in csv_reader.fieldnames:
                content = csv_content[column]
                csv_contents[-1].append(content)

                content_type = guess_content_type(content)
                type_guesses[column.replace(' ', '_')] = decide_common_type(type_guesses.get(column.replace(' ', '_'), int), content_type)

        return [columns, csv_contents, type_guesses]
    

def python_type_to_avsc_type(py_type: type[any]) -> str:
    """convert python type to relative avro type for scheme generating"""
    if (py_type == int):
        return "long"
    elif (py_type == float):
        return "double"
    else:
        return "string"


def form_avro_schema(types: dict[str, type[any]], avro_name: str) -> str:
    """generating json of avsc from mapping of column name to its type from csv"""
    schema = {}
    schema['namespace'] = f'{avro_name}.avro'
    schema['type'] = 'record'
    schema['name'] = avro_name
    fields = []
    for column in types.keys():
        fields.append({'name': column, 'type': python_type_to_avsc_type(types[column])})

    schema['fields'] = fields
    return json.dumps(schema)


def convert_to_type(field: any, type: type[any]) -> any:
    """convert the value to proper type for serialization"""
    if type == int:
        return int(field)
    elif type == float:
        return float(field)
    else:
        return field


def store_contents_as_avro(columns: list[str], contents: list[list[any]], types: dict[str, type[any]], avro_name: str):
    """serialize parsed csv into avro file with given name"""
    schema = avro.schema.parse(form_avro_schema(types, avro_name))
    with open(f'{avro_name}.avro', 'w') as out:
        writer = DataFileWriter(out, DatumWriter(), schema)
        for content in contents:
            row = {}
            for index, field in enumerate(content):
                row[columns[index]] = convert_to_type(field, types[columns[index]])

            writer.append(row)

        writer.close()


def csv_to_avro(csv_path: str, avro_name: str):
    """function to convert given csv file path to avro with given name"""
    (columns, contents, types) = parse_csv(csv_path)
    store_contents_as_avro(columns, contents, types, avro_name)

if __name__ == '__main__':
    CSV_FILE_PATH = 'bitcoin_price_Training - Training.csv'
    AVRO_NAME = 'bitcoin_price'
    
    csv_to_avro(CSV_FILE_PATH, AVRO_NAME)