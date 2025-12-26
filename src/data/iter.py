from tempfile import NamedTemporaryFile
from os import replace, remove, path
from shutil import copy


import pandas as pd

from contextlib import contextmanager



from tqdm import tqdm
from typing import override
from inspect import signature







#!!! line based chunking, linebreaks should be encoded if used !!!
@contextmanager
def chunk_out(file_path, file_out_path=None, chunk=2000, has_header=True):
    if not file_out_path:
        file_out_path = NamedTemporaryFile("w", encoding="utf-8", dir=path.dirname(file_path)).name


    with NamedTemporaryFile("w", encoding="utf-8", dir=path.dirname(file_path), delete=False) as file_chunked, open(file_path, "r", encoding="utf-8") as file_in, open(file_out_path, "a", encoding="utf-8") as file_out:
        i = -1 #so bufferror can be catched

        for i, line in enumerate(file_in):
            if i == 0:
                file_chunked.write(line)
                if has_header and path.getsize(file_out_path):
                    continue

            elif i > chunk:
                break    

            file_out.write(line)
        
        if i < 1:
            raise BufferError("nothing to chunk from original file")

        while file_chunk := file_in.read(64*1024*1024):
            file_chunked.write(file_chunk)
                    
    replace(file_chunked.name, file_path)

    try:
        yield (chunked_file := open(file_out_path, "r", encoding="utf-8"))
        
    finally:
        if path.isfile(file_chunked.name):
            remove(file_chunked.name)
            
        chunked_file.close()
        




        


#TODO: bound function behaviour
#TODO: multiuse impl
def argbind(func, *args, **kwargs):
    output = {}
    
    sig = list(signature(func).parameters)
    i = 0

    for x in range(len(sig)):
        if i < len(args):
            key = sig[i]
            val = args[i]
            i += 1
        elif sig[x] in kwargs:
            key = sig[x]
            val = kwargs[sig[x]]
        else:
            continue
        
        output[key] = val


    return output






#TODO: should make methods take each other as args, so that different processor can be combined
#TODO: args consumption
class FileProcessor():

    @classmethod
    def file_loader(cls, file):
        with open(file) as f:
            yield f

    @classmethod
    def file_processing(cls, processor, x, *args, **kwargs):
        processor(x, *args, **kwargs)

    #TODO: remove messy arg processing
    @classmethod
    def process(cls, file, processor, progress=True, *args, **kwargs):

        loader_args = args[:max(len(signature(cls.file_loader).parameters) - 1, 0)]
        loader_args = argbind(cls.file_loader, *(None,) + loader_args, **kwargs)
        loader_args = dict(list(loader_args.items())[1:])
        
        processor_args = args[len(loader_args):]
        processor_args = argbind(processor, *(None,) + processor_args, **kwargs)
        processor_args = dict(list(processor_args.items())[1:])


        for x in tqdm(cls.file_loader(file, **loader_args), disable=not progress):
            cls.file_processing(
                processor,
                x, 
                **processor_args
            )
        



class BatchProcessor(FileProcessor):

    #TODO: temps
    @classmethod
    @override
    def file_loader(cls, file, batch_size=None, has_header=True):
        chunked = path.join(path.dirname(file), f"{path.basename(file).split(".")[0]}_chunked.{file.split(".")[-1]}")
        chunk = path.join(path.dirname(file), f"{path.basename(file).split(".")[0]}_chunk.{file.split(".")[-1]}")

        assert path.isfile(file)
        assert not path.isfile(chunked)
        assert not path.isfile(chunk)


        copy(file, chunked)

        while True:
            try:
                with chunk_out(chunked, chunk, chunk=batch_size, has_header=has_header) as file_chunk:
                    yield file_chunk
                remove(chunk)

            except Exception as e:
                if not (isinstance(e, BufferError) and "nothing to chunk from original file" in str(e)):
                    raise e

                remove(chunk)
                remove(chunked)
                break






class CsvDataframeProcessor(FileProcessor):
    
    @classmethod
    @override
    def file_loader(cls, file, batch_size=None):
        
        if not batch_size:
            yield ( pd.read_csv(file) )

        else:
            yield from pd.read_csv(file, chunksize=batch_size)


class JsonlDataframeProcessor(FileProcessor):
    
    @classmethod
    @override
    def file_loader(cls, file, batch_size=None):
        
        if not batch_size:
            yield ( pd.read_json(file, lines=True) )

        else:
            yield from pd.read_json(file, lines=True, chunksize=batch_size)