Coding challenge: Process event data

1. source data
a). event data in attached csv file (event_2020071401.csv)
- there may have duplicates in the file. To deduplicate, you can use event_uuid as primary key and keep the most recent record
b). ClientType.json
Client type key value mapping
c). EventType.json
Event type key value mapping

2. output
a). We will need to include all columns from source plus two new columns:
- client_type_value
The value need to be resolved from ClientType.json attached

- event_type_value
The value need to be resolved from EventType.json attached and save as Camel Case with space in the middle, e.g. USER_LOCKED should be converted to 'User Locked'

Note:
  - json file stores data as key and value. You can fetch value by using key stored in client_type/event_type
  - if enum value can't be found in the json files, simply return and fill with raw value to corresponding value columns, e.g. 102 can't be found in EventType mapping, return 102 as the value.
b). keep data uniqueness in output file
c). output file should be in csv format

3. bonus point
can you save data to 'event' table in db (e.g. mysql, postgres or oracle)
Remember the source can be from multiple files (with different timestamp) which could include duplicates across files too. For example, event uuid 'abc' could exist in file1 and file2 both. To be able to address such case, you may need to deduplicate the data with intermedia storage (e.g. memory, db or filesystem) before saving to master table.
Please describe you steps in detail and how are you going to process duplicates (not just from one single source).

Feel free to use any language (java, python, scala or spark) you are comfortable with to process the file and generate the output.
Timeline: 24 hrs
