import logging
from os import path, makedirs

import requests
from inspect import signature, Parameter

import gzip
# import ijson
import pandas as pd

from random import randint


logger = logging.getLogger(__name__)









#DATA CONSTS
DPR_SOURCE_DOCS="https://dl.fbaipublicfiles.com/dpr/wikipedia_split/psgs_w100.tsv.gz"
DPR_SOURCE_NQ="https://dl.fbaipublicfiles.com/dpr/data/retriever/biencoder-nq-train.json.gz"






#retrieve
def pull(data_src, data_dest):
    assert path.isfile(data_dest) == False and path.isdir(data_dest) == False
    
    if path.exists(path.split(data_dest)[0]) == False:
        makedirs(path.split(data_dest)[0])

    logging.info(f"pulling {data_src}...")

    with requests.get(data_src) as r:
        with open(data_dest, "wb") as f:
            f.write(r.content)

    logging.info(f"done!")


def sourcelist():
    return { key : value for key, value in globals().items() if all(x.isalpha() == False or x.isupper() for x in str(key)) and isinstance(value, str)}






#read
def from_file(file_path, file_type="csv", separator=",", compression="infer", encoding=None, chunk_size=None):
    assert path.isfile(file_path)
    assert file_type in ["csv", "json", "xlsx", "parquet"]

    match file_type:
        case "csv":
            reader = pd.read_csv
        case "json":
            if not chunk_size:
                reader = pd.read_json
            else:
                reader = lambda f, encoding, chunksize: __json_chunk_iterator(f, encoding, chunksize, lambda x : pd.DataFrame(x))
        case "xlsx":
            reader = pd.read_excel
        case "parquet":
            reader = pd.read_parquet

    kwargs = {
        "sep": separator,
        "compression": compression,
        "encoding": encoding,
        "chunksize": chunk_size
    }
    kwargs = { k: v.default if k not in kwargs else kwargs[k] for k, v in list(signature(reader).parameters.items())[1:] if v.default is not Parameter.empty or k in kwargs }


    data = reader(
        file_path,
        **kwargs
    )
    

    if not isinstance(data, pd.DataFrame):
        logger.warning("Data wasn't loaded as DataFrame")

    return data


#Lazily reads a large JSON file in chunks and processes each chunk using a custom function.
#DEPR
def __json_chunk_iterator(file_path, encoding=None, chunk_size=1000, process_func=None):

    reader = open if not file_path.endswith(".gz") else gzip.open

    #with gzip.open(file_path, "rt", encoding=encoding) as f:
    with reader(file_path, "rt", encoding=encoding) as f:
        rows = []
        for record in ijson.items(f, "item"):  # "item" depends on JSON structure
            rows.append(record)
            
            if len(rows) >= chunk_size:
                yield process_func(rows) if process_func else rows
                rows = []
        
        # "leftover" rows
        if rows:
            yield process_func(rows) if process_func else rows



def __mem_check_line(file_path, sample_count=1000, skip_header=False):
    file_size = path.getsize(file_path)
    byte_range = file_size // sample_count
    samples = []


    if byte_range <= 1:
        raise ValueError(f"too many samples for file: {file_size}/{sample_count}")
        

    with open(file_path, 'rb') as f:
        if skip_header:
            overhead = 0
        else:
            overhead = len(f.readline()) #firstline == expected to be column names   
            #f.read(1) #set pointer on first data line

        for _ in range(sample_count):
            offset = randint(0, min(byte_range, file_size - f.tell())) #offset = 0 kinda meh but alright
            

            if (f.seek(f.tell() + offset - 1)) or f.read(1) in {b"\n", b"\r"} == False: #only take "full/new" line, discard otherwise 
                f.readline()

            if not (line := f.readline()).strip() == b"": #dont want empty lines (+EOFline) in estimation
                samples.append(len(line))

    if not samples:
        raise RuntimeError("No valid lines sampled")


    return (overhead + sum(samples)) / len(samples), len(samples)



#some issues for small files? needs testing
def estimate_chunks(file_path, designated_memorybytes, load=0.5):
    assert path.isfile(file_path)
    assert 0 < load < 1

    sample_count = int(min(path.getsize(file_path) / 1000, 10000))
    sampling = __mem_check_line(file_path, sample_count)
    estimation = (designated_memorybytes * load) /sampling[0]
    
    PD_OVERHEAD = 3

    
    return estimation / PD_OVERHEAD




if __name__ == "__main__":
    from benchmark import memory_benchmark
    import gc
    
    logging.basicConfig(level=logging.INFO)

    print(sourcelist())
    # File path
    file_path = 'D:/HuggingFace/cache/self/'
    d_path = r"D:\HuggingFace\cache\self\biencoder-nq-train.json"
    
    #pull(sourcelist()["DPR_SOURCE_NQ"], path.join(file_path, "nq.json.gz"))    
    #exit()   
    print(estimate_chunks(d_path, 4000000, 0.5))


    #gc.collect()
    #memory_benchmark(lambda: pd.read_json(r"D:\HuggingFace\cache\self\biencoder-nq-train.json"))()   

    gc.collect()
    def iterate_all():
        for x in from_file(d_path, "json", chunk_size=10000):
            del x
            gc.collect()
            raise Exception
            
    memory_benchmark(iterate_all)()
