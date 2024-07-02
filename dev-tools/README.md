# Development Tools

This directory contains scripts and configuration files that are only used during development.

## Contents
- `create-dirs.cmd`: Script for building directory structure (on Windows) needed for custom-data-connector 
- `create-dirs.sh`: Script for building directory structure (on Linux) needed for custom-data-connector
- `docker-compose.yml`: This is a Docker Compose file used to set up a custom-data-connector for use in local development.

## Using custom-data-connector
* Make a github token or use an existing one. [How to](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens#creating-a-personal-access-token-classic)
* Run: ```export TOKEN=<token>``` (linux) or ```set TOKEN=<token>``` (windows)
* Run: ```echo $TOKEN | docker login ghcr.io -u <username> --password-stdin```(linux) or ```echo %TOKEN%  | docker login ghcr.io -u <username> --password-stdin``` (windows)
* Run: ```./create-dirs.sh``` (linux) or ```crate-dirs.cmd``` (windows)
* Run: ```docker-compose up```
* Set environment variabel ```CUSTOM_DATA_CONNECTOR_HOST``` to the value ```localhost:1880```