# Phinx Snowflake Adapter

[![Unit Tests](https://github.com/szabacsik/phinx-snowflake-adapter/actions/workflows/test.yaml/badge.svg?branch=main)](https://github.com/szabacsik/phinx-snowflake-adapter/actions/workflows/test.yaml)
[![Code Coverage](https://codecov.io/gh/szabacsik/phinx-snowflake-adapter/branch/main/graph/badge.svg)](https://codecov.io/gh/szabacsik/phinx-snowflake-adapter)
[![Infection MSI](https://img.shields.io/endpoint?style=flat&url=https://badge-api.stryker-mutator.io/github.com/szabacsik/phinx-snowflake-adapter/main)](https://dashboard.stryker-mutator.io/reports/github.com/szabacsik/phinx-snowflake-adapter/main#mutant)

This repository contains a **Snowflake Adapter** for PHP **Phinx database migration tool**. The adapter allows Phinx to communicate with a Snowflake database and helps with version control of your Snowflake database. In order to use the adapter, you'll need to have Phinx and the **Snowflake PDO** driver installed and set up. To install the Snowflake PDO driver, you can check out my [Docker Snowflake PDO](https://github.com/szabacsik/docker-apache-php-xdebug-snowflake-pdo) solution which has the driver included. For instructions on how to use and set up the adapter, be sure to check out the official [Phinx Documentation](https://book.cakephp.org/phinx/0/en/configuration.html#supported-adapters) first.
