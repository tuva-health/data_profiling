[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.x&color=orange)

# Data Profiling

Check out the [Tuva Data Model](https://docs.google.com/spreadsheets/d/1NuMEhcx6D6MSyZEQ6yk0LWU0HLvaeVma8S-5zhOnbcE/edit#gid=1991587675)

Check out the latest [DAG](https://tuva-health.github.io/data_profiling/#!/overview?g_v=1) for this concept

Check out our [Docs](http://thetuvaproject.com/)

Healthcare data profiling helps to systematically identify problems in your data with a special focus on issues that can cause downstream analysis problems.
These data integrity checks include general fill rates, fill rates specific to claim types, uniqueness, referential integrity, date validation, and valid values (gender, healthcare codes, etc).

The output models of this project are:

* Eligibility Detail - a data profiling table on the eligibility grain with columns for source primary keys and every data quality check performed.
* Medical Claim Detail - a data profiling table on the medical claim line grain with columns for source primary keys and every data quality check performed.
* Claim Summary - a summary table of checks ran on every column in Eligibility Detail and Medical Claim Detail with test fail percentages.
* Snapshots - A "look back in time" of every model that gets generated during `dbt build` (or `dbt run` followed by `dbt snapshot`). 

## Pre-requisites
1. You have claims data (e.g. medicare, medicaid, or commercial) in a data warehouse
2. You have mapped your claims data to the [claim input layer](https://docs.google.com/spreadsheets/d/1NuMEhcx6D6MSyZEQ6yk0LWU0HLvaeVma8S-5zhOnbcE/edit?usp=sharing)
   *Note: this is a prerequisite for running Claims Preprocessing, you will only need to map your data to this claim input layer once.*
    - The claim input layer is at a claim line level and each claim id and claim line number is unique
    - The eligibility input layer is unique at the month/year grain per patient and payer
    - Revenue code is 4 digits in length
4. You have [dbt](https://www.getdbt.com/) installed and configured (i.e. connected to your data warehouse)

[Here](https://docs.getdbt.com/dbt-cli/installation) are instructions for installing dbt.

## Getting Started
Complete the following steps to configure the package to run in your environment.

1. [Clone](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository) this repo to your local machine or environment
2. Create a database called 'tuva' in your data warehouse
    - Note: this is optional, see step 4 for further detail
3. Configure [dbt_project.yml](/dbt_project.yml)
    - Fill in vars (variables):
        - source_name - description of the dataset feeding this project
        - input_database - database where sources feeding this project are stored
        - input_schema - schema where sources feeding this project is stored
        - output_database - database where output of this project should be written.  
        We suggest using the Tuva database but any database will work.
        - output_schema - name of the schema where output of this project should be written
        - terminology_schema - name of the schema where output of this project should be written
4. Review [sources.yml](models/sources.yml).  The table names listed are the same as in the Tuva data model (linked above).  If you decided to rename these tables:
    - Update table names in sources.yml
5. Execute `dbt build` to load seed files, run models, and perform tests.

## Usage Example
Sample dbt command specifying new variable names dynamically:

```
dbt build --vars '{input_database: my_database, input_schema: my_input, output_database: my_other_database, output_schema: i_love_data}'
```

## Contributions
Have an opinion on the mappings? Notice any bugs when installing and running the package? 
If so, we highly encourage and welcome contributions! 

Join the conversation on [Slack](https://tuvahealth.slack.com/ssb/redirect#/shared-invite/email)!  We'd love to hear from you on the #claims-preprocessing channel.

## Database Support
This package has been tested on Snowflake and Redshift.
