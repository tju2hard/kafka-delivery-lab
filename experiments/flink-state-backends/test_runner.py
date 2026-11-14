import importlib.util,json,pathlib,struct,tempfile,unittest
spec=importlib.util.spec_from_file_location('bench',pathlib.Path(__file__).with_name('run.py'));b=importlib.util.module_from_spec(spec);spec.loader.exec_module(b)
class Checks(unittest.TestCase):
 def test_oracle_and_corruptions(self):
  with tempfile.TemporaryDirectory() as td:
   root=pathlib.Path(td);inp,m=b.fixture(root,70);first={}
   for seq,k in enumerate(b.ids(inp)):first.setdefault(k,seq)
   self.assertEqual(m['inputs'],100);self.assertEqual(len(first),70)
   run=root/'run';run.mkdir()
   (run/'operator-0.json').write_text(json.dumps({'processed':100,'unique':70,'duplicates':30}))
   def output(rows):
    with (run/'output-0.bin').open('wb') as f:
     for k,seq in rows:f.write(struct.pack('>qq',k,seq))
   rows=list(first.items());output(rows);self.assertTrue(b.oracle(run,inp,m)['passed'])
   output(rows[:-1]);self.assertFalse(b.oracle(run,inp,m)['passed'])
   output(rows+[rows[0]]);self.assertFalse(b.oracle(run,inp,m)['passed'])
   wrong=rows[:];wrong[0]=(wrong[0][0],wrong[0][1]+1);output(wrong);r=b.oracle(run,inp,m);self.assertEqual(r['wrong_first_arrival_seq'],1);self.assertFalse(r['passed'])
   bad=rows[:];bad[0]=(-1,0);output(bad);self.assertFalse(b.oracle(run,inp,m)['passed'])
 def test_fixture_replay_and_hash(self):
  with tempfile.TemporaryDirectory() as td:
   root=pathlib.Path(td);p,m=b.fixture(root,700);self.assertEqual(b.fixture(root,700)[1],m)
   p.write_bytes(p.read_bytes()+b'corrupt')
   with self.assertRaises(AssertionError):b.fixture(root,700)
if __name__=='__main__':unittest.main()
