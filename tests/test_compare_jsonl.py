"""Mutation controls for the full-record JSONL comparison tool."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('comparator', Path(__file__).resolve().parents[1]/'scripts/compare-jsonl.py')
comparator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(comparator)


class ComparisonControls(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='kafka-jsonl-check-')
        self.addCleanup(self.directory.cleanup)
        self.source = [{'event_id': key, 'input_seq': seq, 'payload': {'amount': seq+10},
                        'ingested_at_ms': 1000+seq} for seq,key in enumerate(('A','A','B','B'))]
        self.output = [{**row, 'is_duplicate': flag, 'marked_at_ms': 1100+i}
                       for i,(row,flag) in enumerate(zip(self.source,(False,True,False,True)))]

    def compare(self, rows):
        source, sink = Path(self.directory.name)/'in.jsonl', Path(self.directory.name)/'out.jsonl'
        for path,values in ((source,self.source),(sink,rows)):
            path.write_text(''.join(json.dumps(row)+'\n' for row in values))
        return comparator.compare(source,sink)

    def test_valid_complete_marking(self):
        self.assertTrue(self.compare(self.output)['passed'])

    def test_missing_arrival(self):
        result = self.compare(self.output[:-1])
        self.assertFalse(result['passed']); self.assertEqual(result['missing_arrivals'],1)

    def test_repeated_arrival_sequence(self):
        result = self.compare(self.output+[self.output[0]])
        self.assertFalse(result['passed']); self.assertEqual(result['repeated_arrival_sequences'],1)

    def test_wrong_duplicate_marker(self):
        rows = copy.deepcopy(self.output); rows[1]['is_duplicate'] = False
        result = self.compare(rows)
        self.assertFalse(result['passed']); self.assertEqual(result['flag_mismatches'],1)

    def test_nested_payload_corruption(self):
        rows = copy.deepcopy(self.output); rows[0]['payload']['amount'] += 1
        result = self.compare(rows)
        self.assertFalse(result['passed']); self.assertEqual(result['payload_mismatches'],1)

    def test_extra_arrival(self):
        rows = copy.deepcopy(self.output); rows.append({**rows[0],'input_seq':99})
        result = self.compare(rows)
        self.assertFalse(result['passed']); self.assertEqual(result['unexpected_arrivals'],1)

    def test_integer_is_not_boolean(self):
        rows = copy.deepcopy(self.output); rows[0]['is_duplicate'] = 0
        result = self.compare(rows)
        self.assertFalse(result['passed']); self.assertEqual(result['schema_errors'],1)


if __name__ == '__main__':
    unittest.main()
