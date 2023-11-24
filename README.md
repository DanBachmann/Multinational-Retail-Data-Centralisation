# Multinational Retail Data Centralisation
Multinational Retail Data Centralisation is a project to demonstrate data handing primarily using Python Pandas and Postgresql as presented by AiCore. The concept takes legacy data from various sources, standardises them and then stores them in a database. It has little or no loops through the data frames allowing Pandas to this work with internal functions for high performance.

## Concepts learned/demonstrated
<ul>
<li>Using lambdas/functions, masks and data series to perform performant large data standardisation.
<li>Unit testing with Python.
<li>Basic threading with Python.
</ul>

## Usage instructions
To execute the project, we can simply run the directory such as:<br/>
<code>python .</code>
<br>
### Optional Parameters
<ul>
<li>checks - perform basic pre-requisite checks
<li>checks_extensive - Performs extensive pre-requisite checks
<li>write_raw - In addition to writing the clean data to the database, write the raw data as well to the same table structure with the prefix _raw. This also includes the basic pre-requisite checks done in 'checks'
<li>Be default, all processes are run; however, any combination can be run by specifying them as:
<ul>
<li>process_users
<li>process_cards
<li>process_stores
<li>process_products
<li>process_orders
<li>process_times
</ul>
<li>do_nothing - If no processes are specified, then don't run all the threads as is the default. This can be useful if only a pre-requisite check is desited.
</ul>
So a use with options to perform basic pre-requisite checks and process just the users and cards would look like:<br/>
<code>python . checks process_users process_cards</code>

## Installation Instructions
This project was written for Python 3 with the following libraries and their dependancies:

      - boto3==1.29.4
      - numpy==1.26.2
      - pandas==2.1.3
      - psycopg2-binary==2.9.9
      - python-dateutil==2.8.2
      - pyyaml==6.0.1
      - requests==2.31.0
      - sqlalchemy==2.0.23
      - tabula-py==2.9.0
More details can be found in the environment_configurations folder which has output from the conda environments used for both Linux and Windows envs.
### Configuration Files
There are two configuration files, db_creds.yaml and api_creds.yaml which contain all the configuration for the legacy source RDS database, target database, API endpoints and credentials. For security reasons, the files are not included in this repository, but templates are provided (db_creds-template.yaml and api_creds-template.yaml) in the environment_configurations folder. After filling these templates in, save them to the project root as db_creds.yaml and api_creds.yaml.

## File Structure and Testing
The file structure is flat with the exception of the environment_configurations folder. Files prefix with tests_ are tests which can be run:
<ul>
<li>tests_unit - tests loading of the configuraton files
<li>tests_externals - tests all external endpoints
</ul>
All of these tests are executed when running the project with the 'checks_extensive' argument. Since this is a relativly simple project, essentially every line of code would be run with:<br/>
<code>
python . checks_extensive write_raw</code>


<p>
<hr>

[![License](https://img.shields.io/badge/License-Boost_1.0-lightblue.svg)](https://www.boost.org/LICENSE_1_0.txt)
