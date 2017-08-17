import unittest
from decimal import Decimal
import target_stitch
from collections import namedtuple
from jsonschema import ValidationError, Draft4Validator, validators, FormatChecker
import dateutil
import copy

# A test case, with an input schema and value and an expected result
Case = namedtuple('Case', ['schema', 'value', 'expected'])

# The result when validation succeeds
Result = namedtuple('Result', ['value'])

# The result when validation fails
Error = namedtuple('Error', [])


cases = [

    # When type is integer the input must be an integer and will
    # always come out an integer.
    Case({"type": "integer"},  1,   Result(1)),
    Case({"type": "integer"},  1.0, Error()),
    Case({"type": "integer"},  'a', Error()),
    Case({"type": "integer"}, True, Error()),

    # When type is number and there is no multipleOf the input can be
    # int or float and the output will be float.
    Case({"type": "number"},  1,   Result(1.0)),
    Case({"type": "number"},  1.0, Result(1.0)),
    Case({"type": "number"},  'a', Error()),
    Case({"type": "number"}, True, Error()),
    
    # When type is number and multipleOf is present the input can be
    # int or float and the output will be Decimal.
    Case({'type': 'number', 'multipleOf': 0.1},  1,   Result(Decimal("1"))),
    Case({'type': 'number', 'multipleOf': 0.1},  1.0, Result(Decimal("1.0"))),
    Case({"type": "number", 'multipleOf': 0.1},  'a', Error()),
    Case({"type": "number", 'multipleOf': 0.1}, True, Error()),

    # When type is string and format is date-time the value must be a
    # string and it must be a recognizable datetime string.
    Case({'type': 'string', 'format': 'date-time'}, 1, Error()),
    Case({'type': 'string', 'format': 'date-time'}, 1.0, Error()),
    Case({'type': 'string', 'format': 'date-time'}, 'a', Error()),    
    Case({'type': 'string', 'format': 'date-time'}, '2017-08-16T20:00:00.000000Z', Result(dateutil.parser.parse('2017-08-16T20:00:00.000000Z'))),
    Case({'type': 'string', 'format': 'date-time'}, True, Error()),    
]


def run_case(case):
    '''Given a Case, call parse_record on it and return the result as either a
    Result or an Error.'''
    
    stream = 'stream'
    record = {'v': case.value}
    full_schema = target_stitch.ensure_multipleof_is_decimal(
        {'type': 'object',
         'properties': {'v': copy.deepcopy(case.schema)}})
    validator = Draft4Validator(full_schema, format_checker=FormatChecker())        
    try:
        return Result(target_stitch.parse_record('stream', record, {'stream': full_schema}, {'stream': validator})['v'])
    except:
        return Error()

def format_value_and_type(value):
    if value is None:
        return ''
    return str(type(value).__name__ + '(' + str(value) + ')')

class TestTypeConversion(unittest.TestCase):

    def test(self):
        for case in cases:
            result = run_case(case)
            self.assertEqual(case.expected, result, str(case))
            if isinstance(case.expected, Result):
                self.assertEqual(type(case.expected.value), type(result.value), str(case))


if __name__ == '__main__':
    print('        {:>41} {:>30} {:>30} {:>30}'.format(
        'schema', 'input', 'got', 'expected'))
    for case in cases:
        if isinstance(case.expected, Result):
            expected_result = case.expected.value
        else:
            expected_result = None

        got = run_case(case)
        if isinstance(got, Result):
            got_result = got.value
        else:
            got_result = None

        if expected_result == got_result and type(expected_result) == type(got_result):
            print('MATCH    {:41} {:>30} {:>30} {:>30}'.format(
                str(case.schema),
                format_value_and_type(case.value),
                format_value_and_type(got_result),
                format_value_and_type(expected_result)))
        else:
            print('MISMATCH {:41} {:>30} {:>30} {:>30}'.format(
                str(case.schema),
                format_value_and_type(case.value),
                format_value_and_type(got_result),
                format_value_and_type(expected_result)))


