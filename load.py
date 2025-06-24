import logging
from os import path, makedirs
from io import BufferedIOBase

import requests
from inspect import signature, Parameter


import gzip
import ijson
from json import loads, dumps
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
                reader = lambda fp, compression, encoding, chunksize: pd.read_json(fp, lines=True, compression=compression, encoding=encoding, chunksize=chunksize) #lambda f, encoding, chunksize: __json_chunk_iterator(f, compression, encoding, None, chunksize, lambda x : pd.DataFrame(x))
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
def __json_chunk_iterator(file_path, compression=None, encoding=None,  mode=None, chunk_size=1000, process_func=None):

    reader = open if compression != "gzip" else gzip.open

    #with gzip.open(file_path, "rt", encoding=encoding) as f:
    with reader(file_path, "rt", encoding=encoding) as f:
        rows = []
        
        if mode == "json":
            iter = ijson.items(f, "item")
        elif mode == "ndjson":
            iter = f
        else:
            if mode != None:
                logger.warning(f"passed invalid mode, fallback to default behaviour")

            try:
                #lazy json array iter
                next(ijson.items(f, "item"))
                f.seek(0)
                iter = ijson.items(f, "item")   # "item" depends on JSON structure

            except Exception as e:
                #lazy ndjson iter (general line iteration)
                f.seek(0)
                iter = f


        for item in iter:  
            rows.append(loads(dumps(item)))

            if len(rows) >= chunk_size:
                yield process_func(rows) if process_func else rows
                rows = []
        
        # "leftover" rows
        if rows:
            yield process_func(rows) if process_func else rows




# def __check_file_for_object_size(file_path, byte_limit, sample_count=1000):
#     file_size = path.getsize(file_path)
#     byte_range = file_size // sample_count
#     samples = []


#     if byte_range <= 1:
#         raise ValueError(f"too many samples for file: {file_size}/{sample_count}")
    

#     with open(file_path, "rb") as f:

#         in_object = False
#         in_string = False
#         is_escaped = False
        
#         brace_depth = 0
#         row_buffer = bytearray()


#         for _ in range(sample_count):
#             if samples: #dont jump first line 
#                 f.seek(f.tell() + 0) #TODO: +rand
            
#             in_object = False
#             in_string = False
            
#             brace_depth = 0
#             row_buffer = bytearray()

#             #TODO newline stuff
#             while True:
#                 if not (byte := f.read(1)): #EOF
#                     break

#                 if is_escaped:
#                     is_escaped = False
#                     continue
                    
#                 elif in_string: #kann weg?
#                     pass #check if stringend
                

#                 elif False: #parsed object
#                     break


#                 if byte == b"\"":
#                     in_string = not in_string

#                 elif byte == b"{":
#                     if not in_object:
#                         brace_depth = 0
                    
#                     in_object = True
#                     brace_depth += 1
#                     row_buffer = bytearray()

#                 elif byte == b"}":
#                     brace_depth -= 1
#                     if brace_depth == 0:
#                         pass #new object
#                     elif brace_depth < 0:
#                         pass #err, dont read it

#                 elif byte == b"\n":
#                     pass #finished for this

                
#                 elif byte == b" ": 
                    

#                 else:
#                     pass

#                 row_buffer += byte
                

#                 #check size limit
#                 if len(row_buffer) > byte_limit:
#                     raise BufferError
                
#                 if brace_depth == 0: #das anders
#                     #add to sample
#                     in_object = False
#                     in_string = False #shouldnt be true for depth != 0, right?


def __line_reader(reader, byte_limit=None):
    assert isinstance(reader, BufferedIOBase)


    if reader.tell() == 0 or (reader.tell() > 0 and reader.seek(reader.tell() - 1) and not reader.read(1) in {b"\n", b"\r"}): #only take "full/new" line, discard otherwise 
        if not reader.readline(byte_limit).endswith(b"\n"):
            raise RuntimeError(f"line exceeds linethreshold ({byte_limit}) at pos.{reader.tell()}")


    while (l := reader.readline(byte_limit)) and l.endswith(b"\n"): 
        if l.strip() != b"": #only want non-empty lines (+EOFline)
            break 
    
    if not l.endswith(b"\n"):
        raise RuntimeError(f"line exceeds linethreshold ({byte_limit}) at pos.{reader.tell()}, {l}")
    

    return l



#won't help
def __json_reader(reader, byte_limit=None):
    assert isinstance(reader, BufferedIOBase)


    start_index = reader.tell()
    obj_buffer = bytearray()
    it_buffer = bytearray() 

    in_object = False
    obj_depth = 0

    in_string = False
    is_escaped = False
    

    #TODO newline stuff
    while (byte := reader.read(1)):
        it_buffer.append(byte[0])

        if byte_limit and reader.tell() - start_index > byte_limit:
            raise RuntimeError(f"line exceeds objectthreshold ({byte_limit}) at pos.{reader.tell()}")
        
        #text controls
        if is_escaped:
            is_escaped = False
            continue
                    
        if byte == b"\\":
            is_escaped = True
            continue
            
        elif byte == b"\"":
            in_string = not in_string
            continue

        elif in_string:
            continue
        
        
        #json controls
        if byte == b"{":
            if not in_object:
                obj_depth = 0
                obj_buffer = bytearray()
                it_buffer = bytearray()
                it_buffer.append(byte[0])

            in_object = True
            obj_depth += 1

        elif byte == b"}":
            if in_object:
                obj_depth -= 1
                obj_buffer += it_buffer
                it_buffer = bytearray()

        # elif byte == b"\n":
        #     in_object = False
        #     obj_depth = 0
        #     obj_buffer = bytearray()
                    
        elif byte == b" ": 
            pass
        

        else:
            if in_object:
                if obj_depth < 0: #read partial obj
                    in_object = False
                    obj_depth = 0
                    it_buffer = bytearray()
                    obj_buffer = bytearray()
                
                elif obj_depth == 0:
                    print(start_index, reader.tell())
                    return obj_buffer

    raise BufferError
#








#need to iterate through, dont like that
def __sample_file_object_size(file_path, obj_reader, byte_limit=None, sample_count=1000):
    file_size = path.getsize(file_path)
    byte_range = file_size // sample_count
    samples = []


    if byte_range <= 1:
        raise ValueError(f"too many samples for file: {file_size}/{sample_count}")
    

    with open(file_path, "rb") as f:
        offset = 0
        
        for _ in range(sample_count):
            if f.tell() != 0: #offset, but dont jump first element 
                offset = randint(0, min(byte_range, file_size - f.tell()))
                
            while f.tell() <= f.tell() + offset:
                obj = obj_reader(f, byte_limit)
            
            samples.append(len(obj))
            del obj

    if not samples:
        raise RuntimeError("No valid objects sampled")
    

    return sum(samples) / len(samples), len(samples)



#TODO: readline() with size and raise Error?
def __mem_check_line(file_path, sample_count=1000, line_threshold=None, skip_header=False):
    file_size = path.getsize(file_path)
    byte_range = file_size // sample_count
    samples = []


    if byte_range <= 1:
        raise ValueError(f"too many samples for file: {file_size}/{sample_count}")
        

    with open(file_path, 'rb') as f:
        if skip_header:
            overhead = 0
        else:
            l = f.readline(line_threshold) #firstline == expected to be column names most of the times
            overhead = len(l)
             
            if not l.endswith(b"\n"):
                raise RuntimeError(f"line exceeds objectthreshold ({line_threshold}) at pos.{f.tell()}")
            
            #f.read(1) #set pointer on first data line

        for _ in range(sample_count):
            offset = randint(0, min(byte_range, file_size - f.tell())) #offset = 0 kinda meh but alright
            

            if (f.seek(f.tell() + offset - 1)) or f.read(1) in {b"\n", b"\r"} == False: #only take "full/new" line, discard otherwise 
                l = f.readline(line_threshold)
                if not l.endswith(b"\n"):
                    raise RuntimeError(f"line exceeds objectthreshold ({line_threshold}) at pos.{f.tell()}")

            if not (l := f.readline(line_threshold)).strip() == b"": #dont want empty lines (+EOFline) in estimation
                samples.append(len(l))

                if not l.endswith(b"\n"):
                    raise RuntimeError(f"line exceeds objectthreshold ({line_threshold}) at pos.{f.tell()}, {l}")


    if not samples:
        raise RuntimeError("No valid lines sampled")


    return (overhead + sum(samples)) / len(samples), len(samples)



def estimate_chunks(file_path, designated_memorybytes, load=0.5):
    assert path.isfile(file_path)
    assert 0 < load < 1

    sample_count = min(path.getsize(file_path) // 1000, 10000)
    sampling = __mem_check_line(file_path, sample_count, 100000*8)
    estimation = (designated_memorybytes * load) / sampling[0]
    
    PD_OVERHEAD = 3

    
    return int(estimation // PD_OVERHEAD)




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
            #gc.collect()
            
    memory_benchmark(iterate_all)()
