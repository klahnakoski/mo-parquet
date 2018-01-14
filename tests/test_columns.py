# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from jx_python.meta import get_schema_from_list
from mo_dots import Null
from mo_testing.fuzzytestcase import FuzzyTestCase

from mo_parquet import rows_to_columns, value_to_def, REQUIRED, OPTIONAL, REPEATED, value_to_rep


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
        columns = rows_to_columns(DREMEL_DATA, schema)

        self.assertEqual(columns, expected)

    def test_dremel_val_rep(self):
        expected_values = {
            "DocId": [10, 20],
            "Name.Url": ["http://A", "http://B", Null, "http://C"],
            "Links.Forward": [20, 40, 60, 80],
            "Links.Backward": [Null, 10, 30],
            "Name.Language.Code": ["en-us", "en", Null, "en-gb", Null],
            "Name.Language.Country": ["us", Null, Null, "gb", Null]
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
        values, reps = value_to_rep(DREMEL_DATA, schema)
        self.assertEqual(values, expected_values)
        self.assertEqual(reps, expected_reps)

        defs = value_to_def(DREMEL_DATA, schema, restrictions)
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
