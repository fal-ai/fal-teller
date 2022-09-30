import shelve

from fal_teller.server._server import TOKEN_DB_PATH

TOKEN_DB = {
    "profiles": {
        "profile_1": {
            "type": "file",
            "params": {"file_system": "file", "root_path": "/tmp/data/profile_1"},
        },
        "profile_2": {
            "type": "file",
            "params": {"file_system": "file", "root_path": "/tmp/data/profile_2"},
        },
    },
    "tokens": {
        "batuhan_token": {"profiles": ["profile_1", "profile_2"]},
        "guest_token": {"profiles": ["profile_1"]},
    },
}

with shelve.open(TOKEN_DB_PATH) as db:
    db["tokens"] = TOKEN_DB["tokens"]
    db["profiles"] = TOKEN_DB["profiles"]
