# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from collections import Mapping

from mo_dots import Data, startswith_field, listwrap, concat_field, unwraplist, Null
from mo_logs import Log

REQUIRED = 'required'
OPTIONAL = 'optional'
REPEATED = 'repeated'


def rows_to_columns(data, all_leaves):
    """
    CONVERT ARRAY OF JSON OBJECTS INTO SET OF COLUMNS, EACH A MULTIDIMENSIONAL ARRAY
    :param data: The array of objects
    :param all_leaves: list of all leaf columns
    :return:
    """

    # organize schema along property paths
    new_schema = Data()
    for full_name in all_leaves:
        new_schema[full_name] = {}
    output = {n: [] for n in all_leaves}

    names = {}

    def _pre_calculate_the_names(schema, path):
        names[path] = [n for n in all_leaves if startswith_field(n, path)]
        for name, sub_schema in schema.items():
            new_path = concat_field(path, name)
            _pre_calculate_the_names(sub_schema, new_path)
    _pre_calculate_the_names(new_schema, '.')

    def _rows_to_columns(value, schema, path, counters, destination):
        if isinstance(value, list):
           for i, new_value in enumerate(value):
                new_counters = counters+(i,)
                if isinstance(new_value, list):
                    # multi-dimensional
                    new_destination = {k: [] for k in names[path]}
                    _rows_to_columns(new_value, schema, path, new_counters, new_destination)
                    for k, v in new_destination.items():
                        destination[k].append(v)
                else:
                    _rows_to_columns(new_value, schema, path, counters, destination)
        elif value == None:
            if schema:
                for name, sub_schema in schema.items():
                    _rows_to_columns(value, sub_schema, concat_field(path, name), counters, destination)
            else:
                destination[path].append(None)
        elif schema:
            for name, sub_schema in schema.items():
                new_path = concat_field(path, name)
                new_value = value.get(name)
                new_counters = counters+(0,)
                new_destination = {k: [] for k in names[new_path]}
                _rows_to_columns(new_value, sub_schema, new_path, new_counters, new_destination)
                for k, v in new_destination.items():
                    destination[k].append(v)
        else:
            destination[path].append(value)

    _rows_to_columns(data, new_schema, '.', tuple(), output)
    return output


def value_to_rep(data, all_leaves):
    """
    REPIPITION LEVELS DO NOT REQUIRE MORE THAN A LIST OF COLUMNS TO FILL
    :param data: array of objects
    :param all_leaves: Names of all the leaf columns
    :return: values and the repetition levels
    """

    # organize schema along property paths
    schema = Data()
    for full_name in all_leaves:
        schema[full_name] = {}

    values = {full_name: [] for full_name in all_leaves}
    rep_levels = {full_name: [] for full_name in all_leaves}

    def _none_to_rep(schema, path, rep_level):
        if schema:
            for name, sub_schema in schema.items():
                new_path = concat_field(path, name)
                _none_to_rep(sub_schema, new_path, rep_level)
        else:
            values[path].append(None)
            rep_levels[path].append(rep_level)

    def _value_to_rep(value, schema, path, counters):
        if isinstance(value, list):
            for k, new_value in enumerate(value):
                new_counters = counters + (k,)
                _value_to_rep(new_value, schema, path, new_counters)
        elif isinstance(value, Mapping):
            for name, sub_schema in schema.items():
                new_path = concat_field(path, name)
                new_value = value.get(name, None)
                _value_to_rep(new_value, sub_schema, new_path, counters)
        elif value is None:
            _none_to_rep(schema, path, get_rep_level(counters))
        else:
            values[path].append(value)
            rep_levels[path].append(get_rep_level(counters))

    _value_to_rep(data, schema, '.', tuple())
    return values, rep_levels


def value_to_def(data, all_leaves, nature):
    """
    DEFINITION LEVELS ENCODE NULLS, WHICH REQUIRES KNOWING THE
    REQUIRED, OPTIONAL, REPEATED NATURE OF EACH COLUMN

    :param data:
    :param all_leaves:
    :param nature: Map each column name to one of REQUIRED, OPTIONAL, REPEATED
    :return:
    """

    # organize schema along property paths
    new_schema = Data()
    for full_name in all_leaves:
        new_schema[full_name] = {}
    def_levels = {full_name: [] for full_name in all_leaves}

    def _none_to_def(schema, path, counters):
        if schema:
            for name, sub_schema in schema.items():
                new_path = concat_field(path, name)
                _none_to_def(sub_schema, new_path, counters)
        else:
            def_levels[path].append(len(counters)-1)

    def _value_to_def(value, schema, path, counters):
        if isinstance(value, list):
            if nature[path] is not REPEATED:
                Log.error("variable {{name}} can not be an array", name=path)
            for k, new_value in enumerate(value):
                new_counters = counters + (k,)
                _value_to_def(new_value, schema, path, new_counters)
        elif isinstance(value, Mapping):
            for name, sub_schema in schema.items():
                new_path = concat_field(path, name)
                new_value = value.get(name, None)
                new_counters = counters
                if nature[new_path] is OPTIONAL and new_value != None:
                    new_counters = counters + (0,)
                _value_to_def(new_value, sub_schema, new_path, new_counters)
        elif value == None:
            if nature[path] is REQUIRED:
                Log.error("requred variable {{name}} can not be missing", name=path)
            _none_to_def(schema, path, counters)
        else:
            def_levels[path].append(len(counters)-1)

    _value_to_def(data, new_schema, '.', tuple())
    return def_levels


def get_rep_level(counters):
    rep_level = -1
    for rep_level, c in reversed(list(enumerate(counters))):
        if c > 0:
            break
    return rep_level


def columns_to_rows(data, schema):
    pass

