## Migration Script

This repository contains a script to migrate metrics and monitors from a source workspace to a target workspace using the Lightup API. The migration script is configurable and supports filtering and overriding fields during the migration process.
Note: Currently only `metric` migration is supported.

### Prerequisites

- Python 3.9 or higher
- Lightup API credentials

### Configuration

The migration script is configured using a JSON file located at `config.json`. The configuration file contains the following sections:

- `source`: Configuration for the source workspace.
- `target`: Configuration for the target workspace.
- `migration_settings`: Settings for the migration process.

### Running the Migration

To run the migration script, execute the following command:

```
source dev.sh
python scripts/migration/migrate.py
```

### Dry Run

By default, the migration script runs in dry run mode, meaning no data is actually posted to the target workspace. To disable dry run mode, set `dry_run` to `false` in the `migration_settings` section of the configuration file.

### Logging

The migration script logs its activities to a file specified in the `logging` section of the configuration file. The default log file is `migration.log`.
