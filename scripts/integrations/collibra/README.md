# Collibra Integration

Lightup can integrate with Collibra to display data quality indicators inside Collibra’s [governance center](https://www.collibra.com/us/en/products/data-governance).  With the help of this integration, users can retrieve data quality checks from Lightup and display them against any given asset such as a column or a table inside Collibra’s data quality dashboard.

Common users of Collibra and Lightup can now see relevant Lightup data quality indicators in Collibra.  With this integration, the following capabilities are unlocked:
* It is possible to publish a “Lightup Data Quality” score to the Quality section of the Collibra asset pages (table, column or any other asset for which you have specified the relationship between the asset and the data quality metric in Collibra)
* Details of the metrics defined in Lightup are also available for consumption in Collibra in the “Quality” section of the asset. For example, if you define a conformity metric in Lightup on a specific Databricks table or a column, in Collibra you can see how many rows conform to the condition defined by that metric, along with a quality score.
* Details of metrics and monitors defined in Lightup are visible in the “Details” section at the table and column level in Collibra. This includes:
  * The name of the monitor along with a link to the Lightup monitor if you want to look into the details inside Lightup.
  * The underlying metric type for the monitor along with a reference to the column asset name if applicable.
  * A count of the number of incidents on this monitor along with a link to the Lightup incidents for further investigation.
  * A health indicator that is either red or green depending on whether a Lightup incident is ongoing  on the asset.

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

## Setup the source map configuration

In order for Collibra sync to be able to map the appropriate datasources on Lightup onto the datasources on Collibra, the user
needs to input a map. This map contains the mapping of the datasources within Lightup to the appropriate datasource on Collibra. When there are incidents on a specific table, these will then be mapped on to the Collibra instance.

An example of this map is defined in the `source_map_config.yaml` file that in the same directory. This
will need to be updated with the configuration on your own instance.

## Run the integration

```bash
cd scripts/integrations/collibra
python run_collibra_sync.py
```
