# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from jx_base.expressions import NULL
from jx_python.meta import get_schema_from_list
from mo_dots import Null
from mo_testing.fuzzytestcase import FuzzyTestCase

from mo_parquet import rows_to_columns, REQUIRED, OPTIONAL, REPEATED


class TestColumns(FuzzyTestCase):
    def test_dremel_array(self):
        expected = {
            "DocId": [[10], [20]],
            "Name.Url": [
                [["http://A"], ["http://B"], Null],
                [["http://C"]]
            ],
            "Links.Forward": [
                [[20, 40, 60]],
                [[80]]
            ],
            "Links.Backward": [
                [[]],
                [[10, 30]]
            ],
            "Name.Language.Code": [
                [[["en-us"], ["en"]], [], [["en-gb"]]],
                [[[]]]
            ],
            "Name.Language.Country": [
                [[["us"], []], [], [["gb"]]],
                [[[]]]
            ]
        }

        schema = get_schema_from_list("dummy", DREMEL_DATA)
        all_names = [c.names['.'] for c in schema.leaves('.')]
        columns = rows_to_columns(DREMEL_DATA, all_names)

        self.assertEqual(columns, expected)

    def test_dremel_rep_values(self):
        expected_values = {
            "DocId": [10, 20],
            "Name.Url": ["http://A", "http://B", NULL, "http://C"],
            "Links.Forward": [20, 40, 60, 80],
            "Links.Backward": [NULL, 10, 30],
            "Name.Language.Code": ["en-us", "en", NULL, "en-gb", NULL],
            "Name.Language.Country": ["us", NULL, NULL, "gb", NULL]
        }
        expected_reps = {
            "DocId": [0, 0],
            "Name.Url": [0, 1, 1, 0],
            "Links.Forward": [0, 1, 1, 0],
            "Links.Backward": [0, 0, 1],
            "Name.Language.Code": [0, 2, 1, 1, 0],
            "Name.Language.Country": [0, 2, 1, 1, 0]
        }
        schema = get_schema_from_list("dummy", DREMEL_DATA)
        all_names = [c.names['.'] for c in schema.leaves('.')]
        table = rows_to_columns(DREMEL_DATA, all_names)
        self.assertEqual(table.values, expected_values)
        self.assertEqual(table.rep_levels, expected_reps)

        expected_defs = {
            "DocId": [0, 0],
            "Name.Url": [2, 2, 1, 2],
            "Links.Forward": [2, 2, 2, 2],
            "Links.Backward": [1, 2, 2],
            "Name.Language.Code": [2, 2, 1, 2, 1],
            "Name.Language.Country": [3, 2, 1, 3, 1]
        }
        restrictions = {
            ".": REPEATED,  # EXPECTING A LIST
            "DocId": REQUIRED,
            "Name": REPEATED,
            "Name.Url": OPTIONAL,
            "Links": OPTIONAL,
            "Links.Forward": REPEATED,
            "Links.Backward": REPEATED,
            "Name.Language": REPEATED,
            "Name.Language.Code": REQUIRED,
            "Name.Language.Country": OPTIONAL
        }

        schema = get_schema_from_list("dummy", DREMEL_DATA)
        all_names = [c.names['.'] for c in schema.leaves('.')]

        schema = Schema()
        table = rows_to_columns(DREMEL_DATA, all_names, restrictions)
        self.assertEqual(table.def_levels, expected_defs)

    def test_null_repeated(self):

        data = [
            {"v": None},  # Since v is REPEATED, WE MUST ASSUME IT IS []
            {"v": []},
            {"v": [None]},
            {"v": [None, None]}
        ]

        expected_values = {"v": [NULL, NULL, NULL, NULL, NULL]}
        expected_reps = {"v": [0, 0, 0, 0, 1]}
        expected_defs = {"v": [0, 0, 1, 1, 1]}

        schema = get_schema_from_list("dummy", data)
        all_names = [c.names['.'] for c in schema.leaves('.')]
        values, reps = rows_to_columns(data, all_names)
        self.assertEqual(values, expected_values)
        self.assertEqual(reps, expected_reps)

        nature = {".": REPEATED, "v": REPEATED}
        defs = rows_to_columns(data, all_names, nature)
        self.assertEqual(defs, expected_defs)

    def test_null_optional(self):

        good_data = [
            {"v": None},
            {"v": "legit value"}
        ]
        bad_data = [
            {"v": []},
            {"v": [None]},
            {"v": [None, None]}
        ]

        expected_values = {"v": [NULL, "legit value"]}
        expected_reps = {"v": [0, 0]}
        expected_defs = {"v": [0, 1]}

        schema = get_schema_from_list("dummy", good_data)
        all_names = [c.names['.'] for c in schema.leaves('.')]
        values, reps = rows_to_columns(good_data, all_names)
        self.assertEqual(values, expected_values)
        self.assertEqual(reps, expected_reps)

        nature = {".": REPEATED, "v": OPTIONAL}
        defs = rows_to_columns(good_data, all_names, nature)
        self.assertEqual(defs, expected_defs)

        for b in bad_data:
            self.assertRaises(Exception, rows_to_columns, [b], all_names, nature)

    def test_null_required(self):

        good_data = [
            {"v": "legit value"}
        ]
        bad_data = [
            {"v": None},
            {"v": []},
            {"v": [None]},
            {"v": [None, None]}
        ]

        expected_values = {"v": ["legit value"]}
        expected_reps = {"v": [0]}
        expected_defs = {"v": [0]}

        schema = get_schema_from_list("dummy", good_data)
        all_names = [c.names['.'] for c in schema.leaves('.')]
        values, reps = rows_to_columns(good_data, all_names)
        self.assertEqual(values, expected_values)
        self.assertEqual(reps, expected_reps)

        nature = {".": REPEATED, "v": REQUIRED}
        defs = rows_to_columns(good_data, all_names, nature)
        self.assertEqual(defs, expected_defs)

        for b in bad_data:
            self.assertRaises(Exception, rows_to_columns, [b], all_names, nature)

    def test_nulls_to_array(self):
        data = [
            {"v": None},
            {"v": "legit value"},
            {"v": []},
            {"v": [None]},
            {"v": [None, None]}
        ]

        # ASSUMES ALL PROPERTIES ARE REPEATED, THEREFORE
        #     None == []
        #     value == [value]
        #     list == list
        expected = {"v": [
            [NULL],
            ["legit value"],
            [],
            [NULL],
            [NULL, NULL]
        ]}

        schema = get_schema_from_list("dummy", data)
        all_names = [c.names['.'] for c in schema.leaves('.')]
        columns = rows_to_columns(data, all_names)

        self.assertEqual(columns, expected)

    def test_classic_nested(self):
        data = [
            {"a": "value0"},
            {"a": "value1", "b": [{"c": -1, "d": 0}]},
            {"a": "value2", "b": [{"c": 1, "d": 2}, {"c": 3, "d": 4}]},
            {"a": "value3", "b": [{"c": 5, "d": 6}, {"c": 7}, {"e": [{"g": 1}, {"g": 2}]}, {"c": 9, "d": 10}]}
        ]

        expected_values = {
            "a": ["value0", "value1", "value2", "value3"],
            "b.c": [NULL, -1, 1, 3, 5, 7, NULL, 9],
            "b.d": [NULL, 0, 2, 4, 6, NULL, NULL, 10],
            "b.e.g": [NULL, NULL, NULL, NULL, NULL, NULL, 1, 2, NULL]
        }

        expected_reps = {
            "a": [0, 0, 0, 0],
            "b.c": [0, 0, 0, 1, 0, 1, 1, 1],
            "b.d": [0, 0, 0, 1, 0, 1, 1, 1],
            "b.e.g": [0, 0, 0, 1, 0, 1, 1, 2, 1]
        }

        expected_defs = {
            "a": [0, 0, 0, 0],
            "b.c": [0, 2, 2, 2, 2, 2, 1, 2],
            "b.d": [0, 2, 2, 2, 2, 1, 1, 2],
            "b.e.g": [0, 1, 1, 1, 1, 1, 2, 2, 1]
        }

        schema = get_schema_from_list("dummy", data)
        all_names = [c.names['.'] for c in schema.leaves('.')]
        values, reps = rows_to_columns(data, all_names)
        self.assertEqual(values, expected_values)
        self.assertEqual(reps, expected_reps)

        nature = {
            ".": REPEATED,
            "a": REQUIRED,
            "b": REPEATED,
            "b.c": OPTIONAL,
            "b.d": OPTIONAL,
            "b.e": REPEATED,
            "b.e.g": REQUIRED
        }
        defs = rows_to_columns(data, all_names, nature)
        self.assertEqual(defs, expected_defs)

    def test_optional_required_repeated(self):
        data = [
            {},
            {"a": {"b": {"c": 1}}},
            {"a": {"b": {"c": 2, "d": [3]}}},
            {"a": {"b": {"c": 4, "d": [5, 6]}}}
        ]

        expected_values = {
            "a.b.c": [NULL, 1, 2, 4],
            "a.b.d": [NULL, NULL, 3, 5, 6]
        }

        expected_reps = {
            "a.b.c": [0, 0, 0, 0],
            "a.b.d": [0, 0, 0, 0, 1]
        }

        expected_defs = {
            "a.b.c": [0, 1, 1, 1],
            "a.b.d": [0, 1, 2, 2, 2]
        }

        schema = get_schema_from_list("dummy", data)
        all_names = [c.names['.'] for c in schema.leaves('.')]
        values, reps = rows_to_columns(data, all_names)
        self.assertEqual(values, expected_values)
        self.assertEqual(reps, expected_reps)

        nature = {
            ".": REPEATED,
            "a": OPTIONAL,
            "a.b": REQUIRED,
            "a.b.c": REQUIRED,
            "a.b.d": REPEATED
        }

        defs = rows_to_columns(data, all_names, nature)
        self.assertEqual(defs, expected_defs)


DREMEL_DATA = [
    {
        "DocId": 10,
        "Links": {
            "Forward": [20, 40, 60]
        },
        "Name": [
            {
                "Language": [
                    {
                        "Code": "en-us",
                        "Country": "us"
                    },
                    {
                        "Code": "en"
                    }
                ],
                "Url": "http://A",
            },
            {
                "Url": "http://B"
            },
            {
                "Language": [
                    {
                        "Code": "en-gb",
                        "Country": "gb"
                    }
                ]
            }
        ]
    },
    {
        "DocId": 20,
        "Links": {
            "Backward": [10, 30],
            "Forward": [80]
        },
        "Name": [
            {"Url": "http://C"}
        ]
    }
]
