import unittest
from os import path
from tempfile import TemporaryDirectory
from shutil import rmtree

from typing import override




from src.data import iter











class PandasProcessorTest(unittest.TestCase):
    
    @override
    def setUp(self):
        self.tmpdir = TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)

        with open(path.join(self.tmpdir.name, "testdata.csv"), "w", encoding="utf-8") as f:
            f.write("""id,name,data\n1,name1,data1\n2,name2,data2""")

        with open(path.join(self.tmpdir.name, "testdata.json"), "w", encoding="utf-8") as f:
            f.write("""[{"id": 1, "name": "name1", "data": "data1"}, {"id": 2, "name": "name2", "data": "data2"}]""")

        with open(path.join(self.tmpdir.name, "testdata.jsonl"), "w", encoding="utf-8") as f:
            f.write("""{"id": 1, "name": "name1", "data": "data1"}\n{"id": 2, "name": "name2", "data": "data2"}""")


    def test_pandasprocessor_csv(self):
        to_test = iter.PandasProcessor.process

        #test
        reads = []
        to_test(
            path.join(self.tmpdir.name, "testdata.csv"),
            lambda x: reads.extend(x["name"]),
            False
        )
        self.assertEqual(reads, ["name1", "name2"])


    def test_pandasprocessor_json(self):
        to_test = iter.PandasProcessor.process

        #test
        reads = []
        to_test(
            path.join(self.tmpdir.name, "testdata.json"),
            lambda x: reads.extend(x["name"]),
            False
        )
        self.assertEqual(reads, ["name1", "name2"])

    def test_pandasprocessor_jsonl(self):
        to_test = iter.PandasProcessor.process

        #test
        reads = []
        to_test(
            path.join(self.tmpdir.name, "testdata.jsonl"),
            lambda x: reads.extend(x["name"]),
            False,
            batch_size=100
        )
        self.assertEqual(reads, ["name1", "name2"])