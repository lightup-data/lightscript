# Example usage for CollibraSync
import yaml
from collibra_sync import CollibraSync

with open("source_map_config.yaml") as f:
    SOURCE_MAP = yaml.safe_load(f)


def main():
    collibra_sync = CollibraSync(SOURCE_MAP)

    # uncomment to clear collibra state
    # collibra_sync.clear_collibra()

    # uncomment to prepare collibra (one time)
    # collibra_sync.prepare_collibra()

    collibra_sync.run()


if __name__ == "__main__":
    main()
