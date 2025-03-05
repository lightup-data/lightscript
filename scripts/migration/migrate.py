import os

from migration_handler import MigrationHandler
from models import Config


def main():
    config_path = os.path.join(os.path.dirname(__file__), "config.json")
    config = Config.from_json(config_path)
    handler = MigrationHandler(config)
    handler.migrate()


if __name__ == "__main__":
    main()
