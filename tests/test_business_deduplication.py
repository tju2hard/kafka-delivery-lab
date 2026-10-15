"""Conservation controls distinguish equal totals from correct business IDs."""
import importlib.util
import json
from pathlib import Path
import sys
import unittest

ENV=Path(__file__).resolve().parents[1]/'harbor/id-deduplication/environment'
sys.path.insert(0,str(ENV))
spec=importlib.util.spec_from_file_location('business_deduplication',ENV/'deduplication.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)


def row(event_id,sequence,payload):
    return {'key':event_id,'value':json.dumps({'event_id':event_id,'input_seq':sequence,'payload':payload})}


class BusinessConservation(unittest.TestCase):
    def setUp(self):
        self.a=row('a',2,{'n':1});self.b=row('b',0,{'n':2});self.repeat=row('a',1,{'n':3})
        self.source=[self.a,self.b,self.repeat]

    def test_exact_first_partition_arrivals(self):
        self.assertTrue(module.audit_business(self.source,[self.a,self.b])['business_contract_pass'])

    def test_equal_total_can_hide_missing_and_duplicate(self):
        result=module.audit_business(self.source,[self.a,self.a])
        self.assertEqual(result['missing_business_ids'],['b'])
        self.assertEqual(result['duplicate_records'],1)
        self.assertFalse(result['business_contract_pass'])

    def test_minimum_sequence_is_not_first_partition_arrival(self):
        result=module.audit_business(self.source,[self.repeat,self.b])
        self.assertEqual(result['payload_or_first_arrival_mismatches'],1)
        self.assertFalse(result['business_contract_pass'])

    def test_unknown_id_or_mutated_key_is_invalid(self):
        for bad in (row('c',9,{}),{**self.a,'key':'wrong'}):
            self.assertFalse(module.audit_business(self.source,[bad,self.b])['business_contract_pass'])


if __name__=='__main__':unittest.main()
