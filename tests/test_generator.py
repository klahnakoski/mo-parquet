# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from itertools import product

from mo_dots import join_field
from mo_parquet import rows_to_columns, REPEATED, rows_to_columns, OPTIONAL, REQUIRED
from mo_testing.fuzzytestcase import FuzzyTestCase

counter = [0]


def make_const():
    yield (
        counter[0],
        [counter[0]],
        [0],
        [0]
    )
    counter[0] += 1


def make_required(name, child):
    def output_req():
        v = list(child())
        for ss, vv, rr, dd in v:
            yield (
                {name: ss} if name else ss,
                vv,
                rr,
                dd
            )

    return output_req


def make_optional(name, child):
    def output_opt():
        yield (
            {name: None} if name else None,
            [None],
            [0],
            [0]
        )

        v = list(child())
        for ss, vv, rr, dd in v:
            yield (
                {name: ss} if name else ss,
                vv,
                [0] + [rrr + 1 for rrr in rr[1:]],
                [ddd + 1 for ddd in dd]
            )

    return output_opt


def make_repeated(name, child):
    def output_rep():
        yield (
            {name: []} if name else [],
            [None],
            [0],
            [0]
        )

        v = list(child())
        for ss, vv, rr, dd in v:
            yield (
                {name: [ss]} if name else [ss],
                vv,
                [0] + [rrr + 1 for rrr in rr[1:]],
                [ddd + 1 for ddd in dd]
            )

        v1 = list(child())
        v2 = list(child())
        for (ss1, vv1, rr1, dd1), (ss2, vv2, rr2, dd2) in product(v1, v2):
            yield (
                {name: [ss1, ss2]} if name else [ss1, ss2],
                vv1 + vv2,
                [0] + [rrr1 + 1 for rrr1 in rr1[1:]] + [1] + [rrr2 + 1 for rrr2 in rr2[1:]],
                [ddd1 + 1 for ddd1 in dd1] + [ddd2 + 1 for ddd2 in dd2]
            )

        v1 = list(child())
        v2 = list(child())
        v3 = list(child())
        for (ss1, vv1, rr1, dd1), (ss2, vv2, rr2, dd2), (ss3, vv3, rr3, dd3) in product(v1, v2, v3):
            yield (
                {name: [ss1, ss2, ss3]} if name else [ss1, ss2, ss3],
                vv1 + vv2 + vv3,
                [0] + [rrr1 + 1 for rrr1 in rr1[1:]] + [1] + [rrr2 + 1 for rrr2 in rr2[1:]] + [1] + [rrr3 + 1 for rrr3 in rr3[1:]],
                [ddd1 + 1 for ddd1 in dd1] + [ddd2 + 1 for ddd2 in dd2] + [ddd3 + 1 for ddd3 in dd3]
            )

    return output_rep


class TestGenerated(FuzzyTestCase):
    def test_repeated_repeated(self):
        data, values, rep_level, def_level = zip(*list(make_repeated("a", make_repeated("b", make_const))()))
        expected_values = {"a.b": sum(values, [])}
        expected_reps = {"a.b": sum(rep_level, [])}
        expected_defs = {"a.b": sum(def_level, [])}

        all_names = ["a.b"]
        values, reps = rows_to_columns(list(data), all_names)
        self.assertEqual(values, expected_values)
        self.assertEqual(reps, expected_reps)

        nature = {
            ".": REPEATED,
            "a": REPEATED,
            "a.b": REPEATED
        }
        defs = rows_to_columns(list(data), all_names, nature)
        self.assertEqual(defs, expected_defs)

    def test_required(self):
        self._run_test([
            {"a": REQUIRED}
        ])

    def test_optional(self):
        self._run_test([
            {"a": OPTIONAL}
        ])

    def test_repeated(self):
        self._run_test([
            {"a": REPEATED}
        ])

    def test_repeated_required_repeated(self):
        self._run_test([
            {"a": REPEATED},
            {"b": REQUIRED},
            {"c": REPEATED}
        ])

    def test_optional_required_required(self):
        self._run_test([
            {"a": OPTIONAL},
            {"b": REQUIRED},
            {"c": REQUIRED}
        ])

    def test_optional_required_optional_required(self):
        self._run_test([
            {"a": OPTIONAL},
            {"b": REQUIRED},
            {"c": OPTIONAL},
            {"d": REQUIRED}
        ])




    def _run_test(self, config):
        """
        :param config: list of {name: nature} objects
        :return: test function
        """
        generator = make_const
        for c in reversed(config):
            for k, v in c.items()[:1]:
                generator = nature_to_generator[v](k, generator)

        nature = {".": REPEATED}
        acc = []
        for c in config:
            for k, v in c.items()[:1]:
                acc.append(k)
                nature[join_field(acc)] = v

        full_name = join_field([k for c in config for k, v in c.items()[:1]])

        data, values, rep_level, def_level = zip(*list(generator()))
        expected_values = {full_name: sum(values, [])}
        expected_reps = {full_name: sum(rep_level, [])}
        expected_defs = {full_name: sum(def_level, [])}

        all_names = [full_name]
        values, reps = rows_to_columns(list(data), all_names)
        self.assertEqual(values, expected_values)
        self.assertEqual(reps, expected_reps)

        defs = rows_to_columns(list(data), all_names, nature)
        self.assertEqual(defs, expected_defs)


nature_to_generator = {
    REPEATED: make_repeated,
    OPTIONAL: make_optional,
    REQUIRED: make_required
}


