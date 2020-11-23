# Personal Capital Challenge

## Instructions
- Append columns `client_type_value` and `event_type_value` to the source data.
- Save `event_type` in camel case and replace `_` with spaces. 
- Return the number if the type doesn't exists.
- `UPSERT` the source data to the `event` table within target database.

## Manifest
```
.
├── README.md
├── data
│   ├── ClientType.json
│   ├── EventType.json
│   └── event_2020071401.csv
├── .env
├── database_connections.py
├── requirements.txt
└── run.py
```

## Installation

`python >= 3.7.0`

```shell
$pyenv virtualenv 3.7.6 personal_capital
$pyenv activate personal_capital
$python -m pip install -r requirements.txt -U
```

## Setup Postgres Connection
Create a `.env` file with the following variables.
```
POSTGRES_PERSONAL_CAPITAL_USER     = <user_name>
POSTGRES_PERSONAL_CAPITAL_HOSTNAME = <host_name>
POSTGRES_PERSONAL_CAPITAL_PORT     = 5432
POSTGRES_PERSONAL_CAPITAL_PASSWORD = <password>
POSTGRES_PERSONAL_CAPITAL_DATABASE = personal_capital
```

## Execute

```
$python run.py <input_path>
```

Example:
```
$python run.py data

3,322 records loaded into staging. 3,322 inserted into event table.
database_connections: Closing connection.
```

Parameters:

- input_path - Path to src file or directory. If directory, any file with the regex pattern `'event_\d{10}.csv'` will be added to the ingestion.


## Description
All src files within the input directory are concatenated into a single `dataframe`, transformed and finally deduped. The `dataframe` is then saved as a `csv` file and bulk inserted into the Postgres database with the `COPY` command into a staging table `personal_capital.event_staging`. The records are then `UPSERT` from `personal_capital.event_staging` into the `personal_capital.event` table using a `LEFT JOIN`.


- *Append columns `client_type_value` and `event_type_value` to the source data.*
- *Return the number if the type doesn't exists.*

Used `pandas` to map the value within the `PC_Ingestion_Event.transform` method.

```python
# Transformation rules
df['client_type_value'] = df['client_type'].map(self.client_types)
df['client_type_value'] = df['client_type_value'].fillna(df['client_type'])
df['event_type_value']  = df['event_type'].map(self.event_types)
df['event_type_value']  = df['event_type_value'].fillna(df['event_type'])
```

- *Save `event_type` in camel case and replace `_` with spaces.*

`EventType.json` is read in and renamed into a private variable. Used `pandas` to map the value within the `PC_Ingestion_Event.transform` method.

```python
# Read in event type
with open(event_types, 'r') as f:
    self.event_types = json.load(f)
    # Replace event name with camel case and '_' with spaces
    self.event_types = {event['ordinal']: event['name'].replace('_', ' ').title() for event in self.event_types}
```

- *`UPSERT` the source data to the `event` table within target database.*

I could not identify a candidate key for the records so a `LEFT JOIN` is used to insert from staging to event.

```sql
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
```
