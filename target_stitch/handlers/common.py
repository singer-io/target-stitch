from  decimal import Decimal
from datetime import datetime
import dateutil
import pytz

def ensure_multipleof_is_decimal(schema):
    '''Ensure multipleOf (if exists) points to a Decimal.

        Recursively walks the given schema (which must be dict), converting
        every instance of the multipleOf keyword to a Decimal.

        This modifies the input schema and also returns it.

        '''
    if 'multipleOf' in schema:
        schema['multipleOf'] = Decimal(str(schema['multipleOf']))

    if 'properties' in schema:
        for k, v in schema['properties'].items():
            ensure_multipleof_is_decimal(v)

    if 'items' in schema:
        ensure_multipleof_is_decimal(schema['items'])

    return schema

def marshall_data(schema, data, schema_predicate, data_marshaller):
    if schema is None:
        return data

    if "properties" in schema and isinstance(data, dict):
        for key, subschema in schema["properties"].items():
            if key in data:
                data[key] = marshall_data(subschema, data[key], schema_predicate, data_marshaller)
        return data

    if "items" in schema and isinstance(data, list):
        subschema = schema["items"]
        for i in range(len(data)):
            data[i] = marshall_data(subschema, data[i], schema_predicate, data_marshaller)
        return data

    if schema_predicate(schema, data):
        return data_marshaller(data)

    return data

def marshall_date_times(schema, data):
    return marshall_data(
        schema, data,
        lambda s, d: "format" in s and s["format"] == 'date-time' and isinstance(d, str),
        lambda d: strptime(d))

def marshall_decimals(schema, data):
    return marshall_data(
        schema, data,
        lambda s, d: "multipleOf" in s and isinstance(d, (float, int)),
        lambda d: Decimal(str(d)))


def strptime_format1(d):
    return datetime.strptime(d, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.UTC)

def strptime_format2(d):
    return datetime.strptime(d, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.UTC)

def strptime_format_fallback(d):
    return dateutil.parser.parse(d)

def strptime(d):
    """
    The target should only encounter date-times consisting of the formats below.
    They are compatible with singer-python's strftime function.
    """
    for fn in [strptime_format1, strptime_format2, strptime_format_fallback]:
        try:
            return fn(d)
        except:
            pass
    raise Exception("Got an unparsable datetime: {}".format(d))
