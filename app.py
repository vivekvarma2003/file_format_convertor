import json
import uuid
import os
import glob
import pandas as pd


def get_columns(ds):
    with open('data/retail_db/schemas.json') as fp:
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


def main():
    os.makedirs('data/retail_demo', exist_ok=True)
    for path in glob.glob('data/retail_db/*'):
        if os.path.isdir(path):
            ds = os.path.split(path)[1]
            for file in glob.glob(f'{path}/*'):
                df = pd.read_csv(file, names = get_columns(ds))
                os.makedirs(f'data/retail_demo/{ds}', exist_ok=True)
                df.to_json(
                    f'data/retail_demo/{ds}/part-{str(uuid.uuid1())}.json',
                    orient = 'records',
                    lines = True 
                )
                print(f'Number of records processed for {os.path.split(file)[1]} in {ds} is {df.shape[0]}')
if __name__=='__main__':
    main()
