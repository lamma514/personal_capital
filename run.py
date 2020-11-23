# Python Standard Library
import argparse
import json
import os
import re
import sys

# pypi
import pandas as pd

# Local Modules
from database_connections import PostgresConn

class PC_Ingestion_Event:

    def __init__(self, src_path, client_types = 'data/ClientType.json', event_types = 'data/EventType.json', file_name_pattern = 'event_\d{10}.csv'):
        
        # Load event file names
        self.event_files = []
        if os.path.isdir(src_path):
            self.event_files = [os.path.join(src_path, f) for f in os.listdir('data') if re.match(file_name_pattern, f)]
        elif os.path.isfile(src_path):
            if re.match(pattern, src_path):
                self.event_files.append(src_path)

        if not self.event_files:
            raise Exception(f'No event files found. Search pattern: [{file_name_pattern}]')

        # Read in client type
        with open(client_types, 'r') as f:
            self.client_types = json.load(f)
            self.client_types = {client['ordinal']: client['name'] for client in self.client_types}

        # Read in event type
        with open(event_types, 'r') as f:
            self.event_types = json.load(f)
            # Replace event name with camel case and '_' with spaces
            self.event_types = {event['ordinal']: event['name'].replace('_', ' ').title() for event in self.event_types}

        # Init database
        self.conn = PostgresConn()

        # Init schema and tables
        sql = f"""

        CREATE SCHEMA IF NOT EXISTS personal_capital;

        CREATE TABLE IF NOT EXISTS personal_capital.event_staging (
        event_uuid varchar(100) not null,
        user_guid varchar(100) not null,
        user_site_id integer,
        client_type integer,
        event_type integer,
        status varchar(100),
        created_date timestamp without time zone,
        updated_date timestamp without time zone,
        client_type_value varchar(100),
        event_type_value varchar(100)
        );

        CREATE TABLE IF NOT EXISTS personal_capital.event (
        event_uuid varchar(100) not null,
        user_guid varchar(100) not null,
        user_site_id integer,
        client_type integer,
        event_type integer,
        status varchar(100),
        created_date timestamp without time zone,
        updated_date timestamp without time zone null,
        client_type_value varchar(100),
        event_type_value varchar(100),
        UNIQUE (event_uuid, user_guid, user_site_id, client_type, event_type, status, created_date, updated_date)
        );        
        """

    def transform(self, df):
        
        # NOTE: Some duplicate event_uuid(s) contains both NULL and datetime value for updated_date
        # Combine event_uuid
        # df2 = df[df.updated_date.isna()==False][['event_uuid', 'updated_date']]
        # df2 = df2.drop_duplicates()
        # df  = df.merge(df2, on='event_uuid', how='left')
        # df['updated_date'] = df['updated_date_x'].fillna(df['updated_date_y'])
        # df  = df.drop(columns=['updated_date_x', 'updated_date_y'], axis=1)

        # Transformation rules
        df['client_type_value'] = df['client_type'].map(self.client_types)
        df['client_type_value'] = df['client_type_value'].fillna(df['client_type'])
        df['event_type_value']  = df['event_type'].map(self.event_types)
        df['event_type_value']  = df['event_type_value'].fillna(df['event_type'])

        # Drop duplicates
        df = df.drop_duplicates()

        return df

    def ingest(self, temp_output_path = 'data/event_staging_temp.csv', staging_tbl_name = 'personal_capital.event_staging'):
        
        frames = []
        for src_file in self.event_files:

            # Read in data
            df = pd.read_csv(src_file, dtype={'user_site_id': 'Int64', 'client_type': int, 'event_type': int, 'status': int})

            # Append frame
            frames.append(df)

        # Union data frames
        result = pd.concat(frames)

        # Apply transformation rules
        result = self.transform(result)

        # Create temp csv file to copy to Postgres database
        result.to_csv(temp_output_path, index=False, header=False, sep=',')

        # Load temp csv file to staging
        self.conn.copy_from(staging_tbl_name, temp_output_path)

        # Remove csv file
        os.remove(temp_output_path)

        staging_ct    = [row for row in self.conn.query('SELECT COUNT(*) FROM personal_capital.event_staging;')][0][0]
        prev_event_ct = [row for row in self.conn.query('SELECT COUNT(*) FROM personal_capital.event;')][0][0]

        # Append to event
        sql = f"""
        INSERT INTO personal_capital.event
        SELECT es.*
        FROM personal_capital.event_staging AS es
        LEFT JOIN (SELECT * FROM personal_capital.event WHERE event_uuid IN (SELECT DISTINCT event_uuid FROM personal_capital.event_staging)) as e
            ON  es.event_uuid = e.event_uuid
            AND es.user_guid  = e.user_guid
            AND COALESCE (es.user_site_id, -1)           = COALESCE (e.user_site_id, -1)
            AND COALESCE (es.client_type, -1)            = COALESCE (e.client_type, -1)
            AND COALESCE (es.event_type, -1)             = COALESCE (e.event_type, -1)
            AND COALESCE (es.status, '')                 = COALESCE (e.status, '')
            AND COALESCE (es.created_date, CURRENT_DATE) = COALESCE (e.created_date, CURRENT_DATE)
            AND COALESCE (es.updated_date, CURRENT_DATE) = COALESCE (e.updated_date, CURRENT_DATE)
        WHERE e.event_uuid IS NULL 
        ;

        DELETE FROM personal_capital.event_staging;
        """
        self.conn.execute(sql)

        curr_event_ct = [row for row in self.conn.query('SELECT COUNT(*) FROM personal_capital.event;')][0][0]

        print(f"{staging_ct:,} records loaded into staging. {curr_event_ct - prev_event_ct:,} inserted into event table.")

if __name__ == '__main__':

    # Create the parser
    my_parser = argparse.ArgumentParser(description='Ingest event data')

    # Add the arguments
    my_parser.add_argument('input_path', type=str, help='Input path to source file or directory')

    args = my_parser.parse_args()

    pc = PC_Ingestion_Event(args.input_path)
    pc.ingest()