#!/usr/bin/python3
# SPDX-License-Identifier: MIT
"""Exercise mixed port types, alignment across work calls, gaps and tag rebasing."""
import unittest
import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
from gnuradio import blocks, gr, iqstream
import pmt

KEY = 'rustradio.iq.absolute_sample_index'
GAP = 'rustradio.iq.gap_samples'


def tag(offset, key, value, source=pmt.PMT_F):
    result = gr.tag_t()
    result.offset, result.key = offset, pmt.intern(key)
    result.value, result.srcid = value, source
    return result


def anchor(offset, index):
    return tag(offset, KEY, pmt.from_uint64(index))


def positions(sink):
    anchors = {t.offset: pmt.to_uint64(t.value) for t in sink.tags()
               if pmt.eq(t.key, pmt.intern(KEY))}
    result, index = [], None
    for offset in range(len(sink.data())):
        index = anchors.get(offset, index)
        if index is None:
            raise AssertionError('output is missing its initial absolute index')
        result.append(index)
        index += 1
    return result


class Alignment(unittest.TestCase):
    def align(self, data0, data1, tags0, tags1, complex1=True, chunk=3, type0="f", type1=None):
        type1 = type1 or ("c" if complex1 else "f")
        sizes = {"b": gr.sizeof_char, "s": gr.sizeof_short, "i": gr.sizeof_int,
                 "f": gr.sizeof_float, "c": gr.sizeof_gr_complex}
        first = getattr(blocks, 'vector_source_' + type0)(data0, False, 1, tags0)
        second = getattr(blocks, 'vector_source_' + type1)(data1, False, 1, tags1)
        out0 = getattr(blocks, 'vector_sink_' + type0)()
        out1 = getattr(blocks, 'vector_sink_' + type1)()
        aligner = iqstream.align_streams(sizes[type0], sizes[type1])
        graph = gr.top_block()
        graph.connect(first, (aligner, 0))
        graph.connect(second, (aligner, 1))
        graph.connect((aligner, 0), out0)
        graph.connect((aligner, 1), out1)
        graph.run(max_noutput_items=chunk)
        self.assertEqual(positions(out0), positions(out1))
        self.assertEqual(len(out0.data()), len(out1.data()))
        return out0, out1

    def test_each_input_can_start_earlier(self):
        for ahead in (0, 1):
            with self.subTest(ahead=ahead):
                starts = [100, 100]
                starts[ahead] = 107
                a, b = self.align(range(20), [complex(i, -i) for i in range(20)],
                                  [anchor(0, starts[0])], [anchor(0, starts[1])])
                expected0 = range(0 if ahead == 0 else 7, 13 if ahead == 0 else 20)
                expected1 = range(7 if ahead == 0 else 0, 20 if ahead == 0 else 13)
                self.assertEqual(list(a.data()), list(expected0))
                self.assertEqual(list(b.data()), [complex(i, -i) for i in expected1])
                self.assertEqual(positions(a), list(range(107, 120)))

    def test_delayed_anchor_and_tags(self):
        origin = pmt.intern('origin')
        tags0 = [tag(0, 'discard', pmt.PMT_T), anchor(2, 100),
                 tag(4, 'keep', pmt.from_uint64(2**64 - 1), origin),
                 tag(4, 'keep', pmt.PMT_F, origin)]
        a, b = self.align(range(10), range(10), tags0, [anchor(0, 102)],
                          complex1=False, chunk=1)
        self.assertEqual(list(a.data()), list(range(4, 10)))
        self.assertEqual(list(b.data()), list(range(6)))
        self.assertFalse(any(pmt.eq(t.key, pmt.intern('discard')) for t in a.tags()))
        kept = [t for t in a.tags() if pmt.eq(t.key, pmt.intern('keep'))]
        self.assertEqual([t.offset for t in kept], [0, 0])
        self.assertEqual(pmt.to_uint64(kept[0].value), 2**64 - 1)
        self.assertEqual(kept[1].value, pmt.PMT_F)
        self.assertTrue(all(pmt.eq(t.srcid, origin) for t in kept))
        self.assertFalse(any(pmt.eq(t.key, pmt.intern('keep')) for t in b.tags()))

    def test_gap_and_later_anchor(self):
        tags0 = [anchor(0, 100), tag(3, GAP, pmt.from_uint64(2)), anchor(6, 111)]
        a, b = self.align(range(12), range(20), tags0, [anchor(0, 100)], complex1=False)
        indices = [100, 101, 102, 105, 106, 107, 111, 112, 113, 114, 115, 116]
        self.assertEqual(positions(a), indices)
        self.assertEqual(list(a.data()), list(range(12)))
        self.assertEqual(list(b.data()), [i - 100 for i in indices])

    def test_simultaneous_gap_and_anchor(self):
        tags0 = [anchor(0, 100), tag(2, GAP, pmt.from_uint64(3)), anchor(2, 105)]
        a, _ = self.align(range(5), range(10), tags0, [anchor(0, 100)], complex1=False)
        self.assertEqual(positions(a), [100, 101, 105, 106, 107])

    def test_uint64_indices(self):
        for start in (2**63 + 123, 2**64 - 5):
            with self.subTest(start=start):
                a, b = self.align(range(5), range(3), [anchor(0, start)],
                                  [anchor(0, start + 2)], complex1=False, chunk=1)
                self.assertEqual(list(a.data()), [2, 3, 4])
                self.assertEqual(list(b.data()), [0, 1, 2])
                self.assertEqual(positions(a), [start + 2, start + 3, start + 4])

    def test_missing_anchor_and_no_overlap(self):
        for tags0, tags1 in (([], [anchor(0, 0)]),
                             ([anchor(0, 0)], [anchor(0, 100)])):
            a, b = self.align(range(3), range(3), tags0, tags1, complex1=False)
            self.assertEqual(list(a.data()), [])
            self.assertEqual(list(b.data()), [])

    def test_all_port_types(self):
        for type0 in ("b", "s", "i", "f", "c"):
            for type1 in ("b", "s", "i", "f", "c"):
                with self.subTest(type0=type0, type1=type1):
                    a, b = self.align(list(range(10)), list(range(10)),
                                      [anchor(0, 10)], [anchor(0, 12)],
                                      type0=type0, type1=type1)
                    self.assertEqual(list(a.data()), list(range(2, 10)))
                    self.assertEqual(list(b.data()), list(range(8)))

    def test_grc_mixed_types(self):
        root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(prefix="iqstream-align-grc-") as temporary:
            folder = Path(temporary)
            flow = folder / "alignment.grc"
            flow.write_text((root / "examples/align_streams.grc").read_text())
            environment = os.environ.copy()
            environment["GRC_BLOCKS_PATH"] = ""
            environment["GR_CONF_GRC_GLOBAL_BLOCKS_PATH"] = os.pathsep.join((
                gr.prefs().get_string("grc", "global_blocks_path", ""), str(root / "grc")))
            environment["XDG_CACHE_HOME"] = str(folder / "cache")
            compiled = subprocess.run(["grcc", "-o", temporary, str(flow)],
                                      env=environment, capture_output=True, text=True, timeout=15)
            self.assertEqual(compiled.returncode, 0, compiled.stdout + compiled.stderr)
            spec = importlib.util.spec_from_file_location("alignment_grc", folder / "alignment_grc.py")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            graph = module.alignment_grc()
            graph.run(max_noutput_items=2)
            self.assertEqual(graph.aligned.nitems_written(0), 8)
            self.assertEqual(graph.aligned.nitems_written(1), 8)

    def test_item_sizes(self):
        for sizes in ((0, 4), (4, 0), (2**31, 4)):
            with self.assertRaises((ValueError, RuntimeError)):
                iqstream.align_streams(*sizes)


if __name__ == '__main__':
    unittest.main()
