"""Conservation controls: equal counts must not conceal replay, loss or bad flags."""
import importlib.util
import json
from pathlib import Path
import sys
import unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'harbor/arrival-marking-comparison/environment'))
from arrival_comparison import COUNT,CASES,SCHEMES,audit_arrivals

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
    def test_pure_redis_before_gap_loses_marker_without_losing_arrival(self):
        rows=self.outputs.copy();v=json.loads(rows[0]['value']);v['is_duplicate']=True
        rows[0]={**rows[0],'value':json.dumps(v)}
        a=audit_arrivals(self.inputs,rows)
        self.assertEqual((a['output_arrivals'],a['false_markers'],a['true_markers']),(9000,2999,6001))
        self.assertEqual((a['missing_arrivals'],a['replayed_arrival_records']),(0,0))
        self.assertEqual((a['business_ids_without_false'],a['flag_mismatches_against_source']),(1,1))
        self.assertFalse(a['arrival_contract_pass'])
    def test_state_redis_before_gap_loses_first_arrival(self):
        a=audit_arrivals(self.inputs,self.outputs[1:])
        self.assertEqual((a['output_arrivals'],a['false_markers'],a['true_markers']),(8999,2999,6000))
        self.assertEqual((a['missing_arrivals'],a['replayed_arrival_records']),(1,0))
        self.assertEqual(a['flag_mismatches_against_source'],0)
        self.assertEqual(a['business_ids_without_false'],1)
        self.assertFalse(a['arrival_contract_pass'])
    def test_after_ack_gap_replays_correct_first_marker(self):
        a=audit_arrivals(self.inputs,self.outputs[:1]+self.outputs)
        self.assertEqual((a['output_arrivals'],a['false_markers'],a['true_markers']),(9001,3001,6000))
        self.assertEqual((a['missing_arrivals'],a['replayed_arrival_records']),(0,1))
        self.assertEqual(a['flag_mismatches_against_source'],0)
        self.assertEqual(a['business_ids_with_multiple_false'],1)
        self.assertFalse(a['arrival_contract_pass'])
    def test_matrix_keeps_marker_and_send_ledger_separate(self):
        self.assertEqual(len(SCHEMES),7);self.assertEqual(len(CASES),14)
        self.assertFalse(any(mode=='bridge' for _,mode,_ in CASES))
        self.assertEqual({mode:fault for _,mode,fault in CASES if fault!='none'},
            {'eos':'snapshot-error','alo':'snapshot-error','streams':'ack-crash',
             'redis-mark-before':'after-redis','redis-mark-after':'after-ack',
             'state-redis-before':'after-redis','state-redis-after':'after-ack'})

if __name__=='__main__':unittest.main()
