import json
import uuid
import os
import glob
import pandas as pd

# problems in the code
# Folder Paths are hard coded     -> use environment variables to avoid it 
# modularization and reusibility  
# modularization -> dividing large piece of code into small pieces

def get_columns(ds):
    BASE_DIR = os.environ.setdefault('BASE_DIR','data/retail_db/schemas.json')
    with open(BASE_DIR) as fp:
        schemas = json.load(fp)
    try:
        schema = schemas.get(ds)
        if not schema:
            raise KeyError
        cols = sorted(schema, key=lambda s: s['column_position'])
        columns = [col['column_name'] for col in cols]
        return columns
    except KeyError:
        print(f'Schema not found for {ds}')
        return

def process_file(SRC_BASE_DIR,TGT_BASE_DIR,ds):
    for file in glob.glob(f'{SRC_BASE_DIR}/{ds}/*'):
        df = pd.read_csv(file, names = get_columns(ds))
        os.makedirs(f'{TGT_BASE_DIR}/{ds}', exist_ok=True)
        df.to_json(
            f'{TGT_BASE_DIR}/{ds}/part-{str(uuid.uuid1())}.json',
            orient = 'records',
            lines = True 
        )
        print(f'Number of records processed for {os.path.split(file)[1]} in {ds} is {df.shape[0]}')

def convert_csv_to_json():
    SRC_BASE_DIR = os.environ.setdefault('SRC_BASE_DIR','data/retail_db')
    TGT_BASE_DIR = os.environ.setdefault('TGT_BASE_DIR', 'data/retail_demo')
    datasets = os.environ.get('DATASETS')
    os.makedirs(f'{TGT_BASE_DIR}', exist_ok=True)
    if not datasets:
        for path in glob.glob(f'{SRC_BASE_DIR}/*'):
            if os.path.isdir(path):
                ds = os.path.split(path)[1]
                process_file(SRC_BASE_DIR, TGT_BASE_DIR, ds)
    else:
        dirs = datasets.split(',')
        for ds in dirs:
            process_file(SRC_BASE_DIR, TGT_BASE_DIR, ds)

if __name__=='__main__':
    convert_csv_to_json()
