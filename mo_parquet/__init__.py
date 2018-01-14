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

def rows_to_columns(data, schema):
    # organize schema along property paths
    new_schema = Data()
    output = {}
    values = {}
    rep_levels = {}
    all_names = set()
    leaves = schema.leaves('.')
    for col in leaves:
        full_name = col.names['.']
        new_schema[full_name] = {}
        output[full_name] = []
        values[full_name] = []
        rep_levels[full_name] = []
        all_names.add(full_name)

    names = {}
    def _pre_calculate_the_names(new_schema, path):
        for name, sub_schema in new_schema.items():
            new_path = concat_field(path, name)
            names[new_path] = [n for n in all_names if startswith_field(n, new_path)]
            if sub_schema:
                _pre_calculate_the_names(sub_schema, new_path)
    _pre_calculate_the_names(new_schema, '.')

    def _rows_to_columns(data, schema, path, counters, destination):
        for i, row in enumerate(data):
            new_counter = counters+(i,)
            if isinstance(row, list):
                # multidimensional
                new_destination = {k: [] for k in names[path]}
                _rows_to_columns(row, schema, path, new_counter, new_destination)
                for k, v in new_destination.items():
                    destination[k].append(v)
                continue

            for name, sub_schema in schema.items():
                new_path = concat_field(path, name)
                value = listwrap(row.get(name))
                if not sub_schema:
                    rep_level = -1
                    for rep_level, c in reversed(list(enumerate(new_counter))):
                        if c > 0:
                            break
                    rep_levels[new_path].append(rep_level)
                    values[new_path].append(value)
                    destination[new_path].append(value)
                else:
                    new_destination = {k: [] for k in names[new_path]}
                    _rows_to_columns(value, sub_schema, new_path, new_counter, new_destination)
                    for k, v in new_destination.items():
                        destination[k].append(v)

    _rows_to_columns(data, new_schema, '.', tuple(), output)
    return output


def value_to_rep(data, schema):
    # organize schema along property paths
    new_schema = Data()
    values = {}
    rep_levels = {}
    leaves = schema.leaves('.')
    for col in leaves:
        full_name = col.names['.']
        new_schema[full_name] = {}
        values[full_name] = []
        rep_levels[full_name] = []

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

    _value_to_rep(data, new_schema, '.', tuple())
    return values, rep_levels


def value_to_def(data, schema, restrictions):
    # organize schema along property paths
    new_schema = Data()
    def_levels = {}
    leaves = schema.leaves('.')
    for col in leaves:
        full_name = col.names['.']
        new_schema[full_name] = {}
        def_levels[full_name] = []

    def _none_to_def(schema, path, counters):
        if schema:
            for name, sub_schema in schema.items():
                new_path = concat_field(path, name)
                _none_to_def(sub_schema, new_path, counters)
        else:
            def_levels[path].append(len(counters)-1)

    def _value_to_def(value, schema, path, counters):
        if isinstance(value, list):
            if restrictions[path] is not REPEATED:
                Log.error("variable {{name}} can not be an array", name=path)
            for k, new_value in enumerate(value):
                new_counters = counters + (k,)
                _value_to_def(new_value, schema, path, new_counters)
        elif isinstance(value, Mapping):
            for name, sub_schema in schema.items():
                new_path = concat_field(path, name)
                new_value = value.get(name, None)
                new_counters = counters
                if restrictions[new_path] is OPTIONAL and new_value != None:
                    new_counters = counters + (0,)
                _value_to_def(new_value, sub_schema, new_path, new_counters)
        elif value == None:
            if restrictions[path] is REQUIRED:
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

