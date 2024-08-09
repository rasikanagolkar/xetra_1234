# xetra_1234

What is the project about?

The project uses the Xetra dataset containing public dataset on GitHub of the Deutsche Boerse, the German marketplace organizer for trading of shares and other
securities.
0.The meta_file acts as an input to the data pipeline.It contains the dates for which the data is read from the source S3 bucket.
1.The project intends to read data from the source files available in the source S3 bucket.
2.Process the data and generate KPI's like min price, max price, daily traded volume, % change from previous closing price etc.
3.The target report is then generated and stored in target S3 bucket as parquet files.

What does the source data look like?

The source data is available as csv files.The filename contains prefix of the date for which the data is available in the file.
The source file contains data as below:

<img width="583" alt="image" src="https://github.com/user-attachments/assets/edb2a67f-0221-4739-b411-285a60576d04">

What are topics explored by me in the project?
design principles, clean coding, virtual environments, project/folder setup, configuration, logging, exeption handling, linting, dependency management, unit testing
integration testing

What are the tools used in the project?
Python 3.9,Github,Visual Studio Code,Docker,Python packages Pandas,boto3,pyyaml,awscli,pylint,moto,coverage
memory-profiler.



