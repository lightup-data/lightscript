# Lightup Collibra Integration

## Setup environment and get dependencies installed

```bash
# go to the top level of the repo
cd $(git rev-parse --show-toplevel)

# setup environment
source ./dev.sh
```

The above script will install lightctl. However, you should verify that lightctl is
working for you. Please see instructions: https://docs.lightup.ai/docs/lightctl-installation. This will require that you have access to the Lightup cluster and can download the Lightup API token.

Test that lightctl is working with this command:
```bash
lightctl version
```

## Setup env for Collibra credentials

See .env_sample file in this directory for an example of how to
setup the .env file. It needs to contain the Collibra username,
password as well as the URL endpoint.

## Setup the configuration

See `SOURCE_MAP` in `collibra_sync.py` for description of configuration that is
used by the script to map Collibra sources with Lightup sources within
workspaces

## Run the integration

```bash
cd scripts/integrations/collibra
python run_collibra_sync.py
```
