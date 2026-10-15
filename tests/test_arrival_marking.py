"""Conservation controls: equal counts must not conceal replay, loss or bad flags."""
import importlib.util
import json
from pathlib import Path
import sys
import unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'harbor/arrival-marking-comparison/environment'))
from arrival_comparison import COUNT,audit_arrivals

class ArrivalAuditTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.inputs=[];cls.outputs=[]
        for seq in range(COUNT):
            v={'event_id':str(seq//3),'input_seq':seq,'payload':{'value':seq},'ingested_at_ms':1000}
            cls.inputs.append({'key':v['event_id'],'value':json.dumps(v)})
            cls.outputs.append({'key':v['event_id'],'value':json.dumps({**v,'is_duplicate':seq%3!=0,'marked_at_ms':1001})})
    def test_all_arrivals_retained(self):
        a=audit_arrivals(self.inputs,self.outputs)
        self.assertTrue(a['arrival_contract_pass']);self.assertEqual(a['true_markers'],6000)
    def test_equal_total_hides_loss_and_replay(self):
        a=audit_arrivals(self.inputs,self.outputs[:-1]+self.outputs[:1])
        self.assertFalse(a['arrival_contract_pass']);self.assertEqual(a['missing_arrivals'],1);self.assertEqual(a['replayed_arrival_records'],1)
    def test_wrong_marker_rejected(self):
        rows=self.outputs.copy();v=json.loads(rows[0]['value']);v['is_duplicate']=True;rows[0]={**rows[0],'value':json.dumps(v)}
        self.assertFalse(audit_arrivals(self.inputs,rows)['arrival_contract_pass'])
    def test_complete_payload_type_checked(self):
        rows=self.outputs.copy();v=json.loads(rows[1]['value']);v['payload']['value']=True;rows[1]={**rows[1],'value':json.dumps(v)}
        self.assertFalse(audit_arrivals(self.inputs,rows)['arrival_contract_pass'])
    def test_filtering_is_wrong_contract(self):
        a=audit_arrivals(self.inputs,self.outputs[::3])
        self.assertFalse(a['arrival_contract_pass']);self.assertEqual(a['missing_arrivals'],6000)

if __name__=='__main__':unittest.main()
