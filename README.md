# xetra_1234

# What is the project about?

The project uses the Xetra dataset containing public dataset on GitHub of the Deutsche Boerse, the German marketplace organizer for trading of shares and other
securities.
0.The meta_file acts as an input to the data pipeline.It contains the dates for which the data is read from the source S3 bucket.
1.The project intends to read data from the source files available in the source S3 bucket.
2.Process the data and generate KPI's like min price, max price, daily traded volume, % change from previous closing price etc.
3.The target report is then generated and stored in target S3 bucket as parquet files.

# What does the source data look like?

The source data is available as csv files.The filename contains prefix of the date for which the data is available in the file.
The source file contains data as below:

<img width="583" alt="image" src="https://github.com/user-attachments/assets/edb2a67f-0221-4739-b411-285a60576d04">

# What are topics explored by me in the project?

design principles, clean coding, virtual environments, project/folder setup, configuration, logging, exeption handling, linting, dependency management, unit testing
integration testing

# What are the tools used in the project?

Python 3.9,Github,Visual Studio Code,Docker,Python packages Pandas,boto3,pyyaml,awscli,pylint,moto,coverage
memory-profiler.

# How to setup the project?
1. Create a source S3 bucket and update the 'src_bucket' tag in the xetra_report1_config.yml file.
2. Create a target S3 bucket and update the 'trg_bucket' tag in the xetra_report1_config.yml file.
3. Create an AWS user with permissions to read/write from/to S3 bucket.
4. Add the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY of the user created in the earlier step as environment variables.
5. Create a python virtual environment using:- 'pipenv shell –python <add path of the python exe>'
6. Activate the virtual environment using:- 'pipenv shell'
7. Install all the packages using the pipfile:- pipenv install

# Future enhancements to the data pipeline

1. Currently, the solution is very tightly coupled. The code can be modularised based on different stages of data processing for ex: extract, transform and load can be separated into modularised code.
2. The meta file used to get the processed dates can be changed to a sql table capturing processing information like starte date ,end date, files processed etc which will give more information regarding the processing of source files.
3. Ability to run from point of failure in case of data pipeline failure.
4. De-duplication of already processed data, and incremental data load can be added.
5. Exception handling can be improved to capture wide range of errors that can occur at different data processing stages in the pipeline.
6. Connections related to resources like, database connections, s3 etc can be separated as independent functions to facilitate re-use.







