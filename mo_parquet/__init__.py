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


def rows_to_val_rep(data, schema, is_required):
    # organize schema along property paths
    new_schema = Data()
    values = {}
    rep_levels = {}
    def_levels = {}
    leaves = schema.leaves('.')
    for col in leaves:
        full_name = col.names['.']
        new_schema[full_name] = {}
        values[full_name] = []
        rep_levels[full_name] = []
        def_levels[full_name] = []

    def _none_to_ref_and_def(schema, path, rep_level, def_level):
        if schema:
            for name, sub_schema in schema.items():
                new_path = concat_field(path, name)
                _none_to_ref_and_def(sub_schema, new_path, rep_level, def_level)
        else:
            values[path].append(None)
            rep_levels[path].append(rep_level)
            def_levels[path].append(def_level)


    def _value_to_ref_and_def(value, schema, path, counters):
        if isinstance(value, list):
            if path in is_required:
                Log.error("requred variable {{name}} can not be an array", name=path)
            for k, new_value in enumerate(value):
                new_counters = counters + (k,)
                _value_to_ref_and_def(new_value, schema, path, new_counters)
        elif isinstance(value, Mapping):
            for name, sub_schema in schema.items():
                new_path = concat_field(path, name)
                new_value = value.get(name, None)

                if new_path not in is_required:
                    new_counters = counters + (0,)  # OPTIONAL
                else:
                    new_counters = counters

                _value_to_ref_and_def(new_value, sub_schema, new_path, new_counters)
        elif value == None:
            if path in is_required:
                Log.error("requred variable {{name}} can not be missing", name=path)
            _none_to_ref_and_def(schema, path, get_rep_level(counters), len(counters)-1)
        else:
            # if path not in is_required:
            #     new_counters = counters + (0,)  # OPTIONAL
            # else:
            #     new_counters = counters

            new_counters = counters
            rep_level = get_rep_level(new_counters)
            values[path].append(value)
            rep_levels[path].append(rep_level)
            def_levels[path].append(len(counters)-1)

    _value_to_ref_and_def(data, new_schema, '.', tuple())
    return values, rep_levels, def_levels

def get_rep_level(counters):
    rep_level = -1
    for rep_level, c in reversed(list(enumerate(counters))):
        if c > 0:
            break
    return rep_level


def columns_to_rows(data, schema):
    pass

