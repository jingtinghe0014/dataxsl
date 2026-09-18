"""Explicit, fail-fast conversion of mapped target fields."""
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
import math

import pandas as pd

TYPES = {'string', 'int', 'integer', 'float', 'double', 'decimal', 'boolean',
         'date', 'datetime', 'timestamp', 'time'}


COLUMN_TYPES_SCHEMA = {
    'type': 'object',
    'additionalProperties': {'anyOf': [
        {'type': 'string', 'enum': sorted(TYPES)},
        {'type': 'object', 'required': ['type'], 'additionalProperties': False,
         'properties': {'type': {'type': 'string', 'enum': sorted(TYPES)},
                        'format': {'type': 'string'}}},
    ]},
}


def convert_value(value, definition):
    if value is None or pd.isna(value):
        return None
    if definition is None:
        return value
    spec = {'type': definition} if isinstance(definition, str) else definition
    kind = spec['type']
    if kind == 'string':
        return str(value)
    if kind in {'int', 'integer', 'float', 'double', 'decimal'}:
        if isinstance(value, bool):
            raise ValueError('Boolean is not a numeric value')
        try:
            number = Decimal(str(value).strip())
        except InvalidOperation:
            raise ValueError('Invalid numeric value') from None
        if not number.is_finite():
            raise ValueError('Nonfinite numeric value')
        if kind in {'int', 'integer'}:
            if number != number.to_integral_value():
                raise ValueError('Fractional integer value')
            return int(number)
        if kind == 'decimal':
            return number
        result = float(number)
        if not math.isfinite(result):
            raise ValueError('Float overflow')
        return result
    if kind == 'boolean':
        normalized = str(value).strip().lower()
        if normalized in {'true', '1', 'yes', 'y', 't'}:
            return True
        if normalized in {'false', '0', 'no', 'n', 'f'}:
            return False
        raise ValueError('Invalid boolean value')
    if kind == 'date' and isinstance(value, date):
        return value.date() if isinstance(value, datetime) else value
    if kind in {'datetime', 'timestamp'} and isinstance(value, datetime):
        return value
    if kind == 'time' and isinstance(value, time):
        return value
    default = '%Y-%m-%d' if kind == 'date' else '%H:%M:%S' if kind == 'time' else '%Y-%m-%d %H:%M:%S'
    fmt = spec.get('format', default)
    for old, new in [('yyyy', '%Y'), ('MM', '%m'), ('dd', '%d'), ('HH', '%H'), ('mm', '%M'), ('ss', '%S')]:
        fmt = fmt.replace(old, new)
    parsed = datetime.strptime(str(value), fmt)
    return parsed.date() if kind == 'date' else parsed.time() if kind == 'time' else parsed
