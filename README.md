# Instructions


# Installation

```
$pyenv virtualenv 3.7.6 personal_capital 
```

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

## Execute

```
$python main.py <input_path> -o <output_path> 
```

Example:
```
$python main.py data/users.csv -o /data/database.db
```

Parameters:

- input_path - Path to user.txt file

Options:

- output_path - Path to sqlite file

