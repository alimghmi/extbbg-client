# EXTBBG-CLIENT

EXTBBG-CLIENT is a comprehensive solution to manage financial instrument data by utilizing Bloomberg's BEAP API endpoints. The application fetches tickers, creates a universe, and triggers requests to the Bloomberg API to retrieve essential financial data points, such as price, volume, and more. Once the API responds, the application listens for the information and stores it in the database for future use. This application is designed to provide financial analysts and investors with timely and accurate financial data, enabling them to make informed decisions. Bloomberg's API offers a wealth of financial data, and EXTBBG-CLIENT is designed to make it easy to access and manage this information effectively.

## Table of Contents

- [EXTBBG-CLIENT](#extbbg-client)
  - [Table of Contents](#table-of-contents)
  - [Project Structure](#project-structure)
  - [Environment Variables](#environment-variables)
  - [Configuration Files](#configuration-files)
  - [Docker Deployment](#docker-deployment)

## Project Structure
```
├── Dockerfile
├── README.md
├── app
│   ├── __init__.py
│   ├── client.py
│   ├── eod
│   │   ├── __init__.py
│   │   ├── client.py
│   │   └── loader.py
│   ├── loader.py
│   ├── main.py
│   └── utils.py
├── beap
│   ├── __init__.py
│   ├── beap_auth.py
│   └── sseclient.py
├── config
│   ├── __init__.py
│   ├── eod.json
│   ├── eod_isin.json
│   └── intra_isin.json
├── credential.txt
├── db
│   ├── __init__.py
│   └── mssql.py
├── docker-compose.yaml
└── requirements.txt
```

- `main.py` is the entry point of the application.
- The `app` directory contains the core application logic.
- The `beap` directory contains the Bloomberg API authentication and SSE client.
- The `config` directory contains the configuration files for different modes of the application.
- The `db` directory contains the database connection logic.
- `Dockerfile` is used for building the Docker image.

## Environment Variables

Create a `.env` file in the project root directory with the following variables:

```
APP=eod
APP=eod_isin

APP=intra_isin
BBG_CRED='{"client_id":"","client_secret":"","name":"mycred","scopes":["eap","beapData","reportingapi"],"expiration_date":1730180683882,"created_date":1682747083882}'

MSSQL_SERVER=mydb.database.windows.net MSSQL_DATABASE=db MSSQL_USERNAME=user MSSQL_PASSWORD=123456
```

- `APP`: Determines the mode of the application. Possible values: `eod`, `eod_isin`, `intra`, `intra_isin`, `bond`, `pcs`.
- `BBG_CRED`: JSON object containing the Bloomberg API credentials.
- `MSSQL_*`: Variables for connecting to the Microsoft SQL Server.

## Configuration Files

The `config` directory contains configuration files for different modes of the application in JSON format:

- `eod.json`
- `eod_isin.json`
- `intra_isin.json`

Each configuration file contains information such as the application name, description, identifier, input/output tables, field URL, and more.

## Docker Deployment

1. Build the Docker image:

```
docker build -t project-name .
```

2. Run the Docker container:
   
```
docker run --env-file .env -it project-name
```

The application will start in the specified mode and begin processing data from the Bloomberg API.