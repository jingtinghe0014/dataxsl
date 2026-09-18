"""Side-effect-free job loading and validation."""
import copy
import json
import os
import math
from pathlib import Path
from string import Template

from jsonschema import Draft7Validator, validators


# Preserve the executor's Python integer contract (1.0 and True are not indexes).
# Nonfinite floats are not valid JSON numbers, even when Python's parser accepts them.
ConfigValidator = validators.extend(Draft7Validator, type_checker=Draft7Validator.TYPE_CHECKER.redefine_many({
    'integer': lambda checker, value: type(value) is int,
    'number': lambda checker, value: type(value) is int or (
        type(value) is float and math.isfinite(value)),
}))


def validate_schema(config, schema, location):
    """Report only the rejected field path and rule, never configuration values."""
    error = next(ConfigValidator(schema).iter_errors(config), None)
    if error is not None:
        suffix = '.'.join(map(str, error.absolute_path))
        path = f'{location}.{suffix}' if suffix else location
        raise ValueError(f'Invalid configuration at {path} ({error.validator})')


PLUGIN_SCHEMA = {
    'type': 'object', 'required': ['name', 'parameter'],
    'additionalProperties': False,
    'properties': {'name': {'type': 'string', 'minLength': 1},
                   'parameter': {'type': 'object'}},
}
READER_SCHEMA = copy.deepcopy(PLUGIN_SCHEMA)
# Legacy jobs declare read-only metadata outside the Reader constructor parameters.
READER_SCHEMA['properties']['mode'] = {'type': 'string', 'const': 'readOnly'}

JOB_SCHEMA = {
    'type': 'object', 'required': ['job'], 'additionalProperties': False,
    'properties': {'job': {
        'type': 'object', 'required': ['content'], 'additionalProperties': False,
        'properties': {
            'setting': {'type': 'object', 'additionalProperties': False,
                        'properties': {
                            'channel': {'type': 'integer', 'const': 1},
                            'parallel': {'type': 'integer', 'minimum': 1},
                            'queue_size': {'type': 'integer', 'minimum': 1}}},
            'content': {'type': 'array', 'minItems': 1, 'maxItems': 1,
                        'items': {'type': 'object', 'required': ['reader', 'writer'],
                                  'additionalProperties': False,
                                  'properties': {'reader': READER_SCHEMA,
                                                 'writer': PLUGIN_SCHEMA}}},
        },
    }},
}


def positive_integer(name, value):
    if type(value) is not int or value < 1:
        raise ValueError(f'{name} must be a positive integer')
    return value


def validate_job(config):
    # Do not include rejected values: they may contain credentials.
    error = next(iter(Draft7Validator(JOB_SCHEMA).iter_errors(config)), None)
    if error is not None:
        path = '.'.join(map(str, error.absolute_path)) or 'job'
        raise ValueError(f'Invalid job configuration at {path} ({error.validator})')


def normalize_job(config):
    config = copy.deepcopy(config)
    if not isinstance(config, dict) or not isinstance(config.get('job'), dict):
        validate_job(config)
    job = config['job']
    setting = job.setdefault('setting', {})
    if not isinstance(setting, dict):
        validate_job(config)
    legacy = setting.pop('speed', {})
    if not isinstance(legacy, dict):
        raise ValueError('job.setting.speed must be an object')
    for name, value in legacy.items():
        if name in setting and setting[name] != value:
            raise ValueError(f'Conflicting job.setting.{name} and speed.{name}')
        setting.setdefault(name, value)
    for name, default in {'channel': 1, 'parallel': 1, 'queue_size': 10}.items():
        if setting.get(name) is None:
            setting[name] = default
    if setting['channel'] != 1 or type(setting['channel']) is not int:
        raise ValueError('main.py supports channel=1 only; use the async model for partitioning')
    validate_job(config)
    return config


def load_job(path, parameters=None):
    substitutions = dict(os.environ)
    for parameter in parameters or []:
        if not isinstance(parameter, str) or '=' not in parameter:
            raise ValueError('Each parameter must be key=value')
        name, value = parameter.split('=', 1)
        if not name:
            raise ValueError('Parameter name must not be empty')
        substitutions[name] = value
    try:
        config = json.loads(Path(path).read_text(encoding='utf-8'))
    except json.JSONDecodeError as exc:
        raise ValueError(f'Invalid job JSON at line {exc.lineno}, column {exc.colno}') from None

    def render(value):
        if isinstance(value, str):
            try:
                return Template(value).substitute(substitutions)
            except KeyError as exc:
                raise ValueError(f'Missing template parameter: {exc.args[0]}') from None
            except ValueError:
                raise ValueError('Invalid template syntax; escape literal dollars as $$') from None
        if isinstance(value, list):
            return [render(item) for item in value]
        if isinstance(value, dict):
            return {name: render(item) for name, item in value.items()}
        return value

    return normalize_job(render(config))
