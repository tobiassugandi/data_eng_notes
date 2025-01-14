import argparse 
import os
import pandas as pd
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    data_table_name = params.data_table_name
    lookup_table_name = params.lookup_table_name
    data_url = params.data_url
    lookup_url = params.lookup_url

    # specify file names
    data_name = 'data_output.parquet'
    lookup_name = 'lookup_output.csv'

    # download data
    os.system(f'wget -O {data_name} {data_url}')
    os.system(f'wget -O {lookup_name} {lookup_url}')

    # create interface object
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # read data and lookup table from files
    df = pd.read_parquet(data_name)
    lookup = pd.read_csv(lookup_name)

    # write data and lookup table to database
    df.to_sql(name=data_table_name,
              con=engine,
              if_exists='replace',)
    lookup.to_sql(name=lookup_table_name,
                  con=engine,
                  if_exists='replace',)





if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest Parquet and CSV into Postgres")

    parser.add_argument("--user", help="user name for postgres")
    parser.add_argument("--password", help="password for postgres")
    parser.add_argument("--host", help="host for postgres")
    parser.add_argument("--port", help="port for postgres")
    parser.add_argument('--db', help='database name')
    parser.add_argument("--data_table_name", help="table name for data")
    parser.add_argument('--lookup_table_name', help='zones lookup table name')
    parser.add_argument('--data_url', help='url for data Parquet file')
    parser.add_argument('--lookup_url', help='url for zones lookup CSV file')

    params = parser.parse_args()
    main(params)