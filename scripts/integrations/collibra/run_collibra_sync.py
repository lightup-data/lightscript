# Example usage for CollibraSync
from collibra_sync import CollibraSync

# SOURCE_MAP

# this is an example that needs to be updated to match your
# collibra source and lightup configuration
SOURCE_MAP = {
    "status": "sync",
    "collibra_sources": [
        {
            "collibra_source_id": "83ecd499-ee35-47b1-8333-98d2fc0474cf",
            "lightup_sources": [
                {
                    "workspace_id": "42c046b7-e27c-468f-bb89-fbfdbeaabac3",
                    "lightup_source_id": "60c9c81f-02d1-405d-a0c3-7aeb5bae6ace",
                },
                {
                    "workspace_id": "42c046b7-e27c-468f-bb89-fbfdbeaabac3",
                    "lightup_source_id": "694880c6-9473-4c8a-88b4-0877c600e564",
                },
                {
                    "workspace_id": "c9effbee-69cb-45d5-9a29-9e1a0d6462bc",
                    "lightup_source_id": "55e4af9b-ca30-47c7-9eeb-759ee8e5a27f",
                },
            ],
        }
    ],
}


def main():
    collibra_sync = CollibraSync(SOURCE_MAP)
    # collibra_sync.clear_collibra()
    # collibra_sync.prepare_collibra()
    collibra_sync.run()


if __name__ == "__main__":
    main()
