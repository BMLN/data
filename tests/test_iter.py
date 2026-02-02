import unittest
from os import path, remove
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





class ChunkoutTest(unittest.TestCase):
    
    @override
    def setUp(self):
        self.tmpdir = TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        with open(path.join(self.tmpdir.name, "chunkme.csv"), "w", encoding="utf-8") as f:
            f.write("""col1,col2\n\na,1\n\nb,2\nc,3\n""")

    def test_chunkout(self):
        to_test = iter.chunk_out
        chunked = path.join(self.tmpdir.name, "chunkme.csv")
        chunk = path.join(self.tmpdir.name, "chunk.csv")

        result = []

        while True:
            try:
                with to_test(chunked, chunk, chunk=2, has_header=True) as file_chunk:
                    with open(file_chunk, "r", encoding="utf-8") as f:
                        result.append(f.readlines())
                remove(chunk)

            except BufferError:
                break
        
        self.assertEqual(result, [['col1,col2\n', '\n', 'a,1\n'], ['col1,col2\n', '\n', 'b,2\n'], ['col1,col2\n', 'c,3\n']])
