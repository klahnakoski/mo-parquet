# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from mo_future import text_type
from mo_logs import Log
from mo_parquet import rows_to_columns, SchemaTree
from mo_parquet.schema import REQUIRED, REPEATED, OPTIONAL
from mo_testing.fuzzytestcase import FuzzyTestCase


class TestColumns(FuzzyTestCase):

    @classmethod
    def setUpClass(cls):
        Log.start()

    def test_dremel_rep_values(self):
        expected_values = {
            "DocId": [10, 20],
            "Name.Url": [b"http://A", b"http://B", b"http://C"],
            "Links.Forward": [20, 40, 60, 80],
            "Links.Backward": [10, 30],
            "Name.Language.Code": [b"en-us", b"en", b"en-gb"],
            "Name.Language.Country": [b"us", b"gb"]
        }
        expected_reps = {
            "DocId": [0, 0],
            "Name.Url": [0, 1, 1, 0],
            "Links.Forward": [0, 1, 1, 0],
            "Links.Backward": [0, 0, 1],
            "Name.Language.Code": [0, 2, 1, 1, 0],
            "Name.Language.Country": [0, 2, 1, 1, 0]
        }
        expected_defs = {
            "DocId": [0, 0],
            "Name.Url": [2, 2, 1, 2],
            "Links.Forward": [2, 2, 2, 2],
            "Links.Backward": [1, 2, 2],
            "Name.Language.Code": [2, 2, 1, 2, 1],
            "Name.Language.Country": [3, 2, 1, 3, 1]
        }

        schema = SchemaTree(locked=True)
        schema.add("DocId", REQUIRED, int)
        schema.add("Name", REPEATED, object)
        schema.add("Name.Url", OPTIONAL, text_type)
        schema.add("Links", OPTIONAL, object)
        schema.add("Links.Forward", REPEATED, int)
        schema.add("Links.Backward", REPEATED, int)
        schema.add("Name.Language", REPEATED, object)
        schema.add("Name.Language.Code", REQUIRED, text_type)
        schema.add("Name.Language.Country", OPTIONAL, text_type)

        table = rows_to_columns(DREMEL_DATA, schema=schema)
        self.assertEqual(table.values, expected_values)
        self.assertEqual(table.reps, expected_reps)
        self.assertEqual(table.defs, expected_defs)

    def test_null_repeated(self):
        data = [
            {"v": None},  # Since v is REPEATED, WE MUST ASSUME IT IS []
            {"v": []},
            {"v": [None]},
            {"v": [None, None]}
        ]

        expected_values = {"v": []}
        expected_reps = {"v": [0, 0, 0, 0, 1]}
        expected_defs = {"v": [0, 0, 1, 1, 1]}

        schema = SchemaTree(locked=True)
        schema.add("v", (REPEATED, OPTIONAL), object)

        table = rows_to_columns(data, schema)
        self.assertEqual(table.values, expected_values)
        self.assertEqual(table.reps, expected_reps)
        self.assertEqual(table.defs, expected_defs)

        nature = {".": REPEATED, "v": REPEATED}

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

        expected_values = {"v": [b"legit value"]}
        expected_reps = {"v": [0, 0]}
        expected_defs = {"v": [0, 1]}

        schema = SchemaTree(locked=True)
        schema.add("v", OPTIONAL, text_type)

        table = rows_to_columns(good_data, schema)
        self.assertEqual(table.values, expected_values)
        self.assertEqual(table.reps, expected_reps)
        self.assertEqual(table.defs, expected_defs)

        for b in bad_data:
            self.assertRaises(Exception, rows_to_columns, [b], schema)

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

        expected_values = {"v": [b"legit value"]}
        expected_reps = {"v": [0]}
        expected_defs = {"v": [0]}

        schema = SchemaTree(locked=True)
        schema.add("v", REQUIRED, text_type)
        table = rows_to_columns(good_data, schema)
        self.assertEqual(table.values, expected_values)
        self.assertEqual(table.reps, expected_reps)
        self.assertEqual(table.defs, expected_defs)

        for b in bad_data:
            self.assertRaises(Exception, rows_to_columns, [b], schema)

    def test_classic_nested(self):
        data = [
            {"a": "value0"},
            {"a": "value1", "b": [{"c": -1, "d": 0}]},
            {"a": "value2", "b": [{"c": 1, "d": 2}, {"c": 3, "d": 4}]},
            {"a": "value3", "b": [{"c": 5, "d": 6}, {"c": 7}, {"e": [{"g": 1}, {"g": 2}]}, {"c": 9, "d": 10}]}
        ]

        expected_values = {
            "a": [b"value0", b"value1", b"value2", b"value3"],
            "b.c": [-1, 1, 3, 5, 7, 9],
            "b.d": [0, 2, 4, 6, 10],
            "b.e.g": [1, 2]
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

        schema = SchemaTree(locked=True)
        schema.add("a", REQUIRED, text_type)
        schema.add("b", REPEATED, object)
        schema.add("b.c", OPTIONAL, int)
        schema.add("b.d", OPTIONAL, int)
        schema.add("b.e", REPEATED, object)
        schema.add("b.e.g", REQUIRED, int)

        table = rows_to_columns(data, schema)
        self.assertEqual(table.values, expected_values)
        self.assertEqual(table.reps, expected_reps)
        self.assertEqual(table.defs, expected_defs)

    def test_optional_required_repeated(self):
        data = [
            {},
            {"a": {"b": {"c": 1}}},
            {"a": {"b": {"c": 2, "d": [3]}}},
            {"a": {"b": {"c": 4, "d": [5, 6]}}}
        ]

        expected_values = {
            "a.b.c": [1, 2, 4],
            "a.b.d": [3, 5, 6]
        }

        expected_reps = {
            "a.b.c": [0, 0, 0, 0],
            "a.b.d": [0, 0, 0, 0, 1]
        }

        expected_defs = {
            "a.b.c": [0, 1, 1, 1],
            "a.b.d": [0, 1, 2, 2, 2]
        }

        schema = SchemaTree(locked=True)
        schema.add("a", OPTIONAL, object)
        schema.add("a.b", REQUIRED, object)
        schema.add("a.b.c", REQUIRED, int)
        schema.add("a.b.d", REPEATED, int)

        table = rows_to_columns(data, schema)
        self.assertEqual(table.values, expected_values)
        self.assertEqual(table.reps, expected_reps)
        self.assertEqual(table.defs, expected_defs)


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
