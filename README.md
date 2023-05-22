# EXTBBG-CLIENT

EXTBBG-CLIENT is a comprehensive solution to manage financial instrument data by utilizing Bloomberg's BEAP API endpoints. The application fetches tickers, creates a universe, and triggers requests to the Bloomberg API to retrieve essential financial data points, such as price, volume, and more. Once the API responds, the application listens for the information and stores it in the database for future use. This application is designed to provide financial analysts and investors with timely and accurate financial data, enabling them to make informed decisions. Bloomberg's API offers a wealth of financial data, and EXTBBG-CLIENT is designed to make it easy to access and manage this information effectively.

## Table of Contents

- [EXTBBG-CLIENT](#extbbg-client)
  - [Table of Contents](#table-of-contents)
  - [Project Structure](#project-structure)
  - [Understanding the Code](#understanding-the-code)
  - [Environment Variables](#environment-variables)
  - [Configuration Files](#configuration-files)
  - [Docker Deployment](#docker-deployment)
  - [Authors](#authors)
  - [Contribution](#contribution)

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


## Understanding the Code

The main.py file is the entry point of the application. It loads the configuration file based on the APP variable and initializes the loader, Client, and app_config objects. The loader object fetches the tickers, and the Client object creates the universe, triggers, and requests. The listen() method listens for responses from Bloomberg and saves the data to the database.

The env file contains environment variables such as BBG_CRED, which is a JSON object that contains the Bloomberg API credentials.

The APP environment variable in this project determines which mode the application should work on. The APP variable is defined in the .env file and can have values such as eod, eod_isin, and intra_isin. Each mode has its own configuration file inside the config folder in .json format, such as eod.json, eod_isin.json, etc.

The configuration files contain information such as the name of the application, the description of the application, whether the identifier is ISIN, the input table, columns, and where clause, the output table, the field URL, and more. The load_app function in the main.py file loads the configuration file based on the APP variable and initializes the loader, Client, and app_config objects.


## Environment Variables

Create a `.env` file in the project root directory with the following variables:

```
APP=eod

BBG_CRED='{"client_id":"","client_secret":"","name":"mycred","scopes":["eap","beapData","reportingapi"],"expiration_date":1730180683882,"created_date":1682747083882}'

MSSQL_SERVER=mydb.database.windows.net 
MSSQL_DATABASE=db 
MSSQL_USERNAME=user 
MSSQL_PASSWORD=123456
```

- `APP`: Determines the mode of the application. Possible values: `eod`, `eod_isin`, `intra_isin`
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

## Authors

- Ali Moghimi ([alimghmi](https://github.com/alimghmi))
- Clemens Struck ([clemstruck](https://github.com/clemstruck))

## Contribution

Contributions are welcomed and greatly appreciated. To contribute to this project, follow these steps:

1. Fork the repository by clicking the "Fork" button on the top right corner of the project's main page.
2. Clone your forked repository to your local machine
3. Create a new branch for your feature: `git checkout -b feature/new-feature`
4. Make your changes and commit them: `git commit -m 'Add new feature'`
5. Push the changes to your forked repository: `git push origin feature/new-feature`
6. Create a Pull Request from your forked repository to the original repository.

Before submitting a Pull Request, please ensure that your code follows the project's coding standards. Also, update the README.md file if necessary.

For major changes or feature requests, please open an issue first to discuss what you would like to change. This allows for better collaboration and a more efficient process.