import logging
from os import path, makedirs

import requests
from inspect import signature, Parameter

import gzip
import ijson
import pandas as pd



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
def __json_chunk_iterator(file_path, encoding=None, chunk_size=1000, process_func=None):

    with gzip.open(file_path, 'rt', encoding=encoding) as f:
        rows = []
        for record in ijson.items(f, "item"):  # "item" depends on JSON structure
            rows.append(record)
            
            if len(rows) >= chunk_size:
                yield process_func(rows) if process_func else rows
                rows = []
        
        # "leftover" rows
        if rows:
            yield process_func(rows) if process_func else rows










if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    print(sourcelist())
    # File path
    file_path = 'Q:/HuggingFace_cache/self/'


    for x in sourcelist().values():
        file_loc = path.join(file_path, path.split(x)[1])
        file_type = path.split(x)[1].split(".")[-2]
        file_type = "csv" if file_type == "tsv" else file_type

        if not path.isfile(file_loc):
            pull(x, file_loc)    
        else:
            print("nopull", x)
            
        
        data = from_file(file_loc, file_type, "\t", "gzip", "utf-8", 1000)

        for i, y in enumerate(data):
            print(y.head())

            if i >= 5:
                break

