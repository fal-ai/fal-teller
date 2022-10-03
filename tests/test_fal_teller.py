import shelve
import threading
from contextlib import ExitStack, contextmanager

import pandas
import pytest

from fal_teller.client import TellerClient
from fal_teller.server import TellerServer

_LOCAL_GRPC = "grpc://0.0.0.0"


@contextmanager
def teller_server():
    server = TellerServer("grpc://0.0.0.0:0")
    thread = threading.Thread(target=server.serve)
    thread.start()
    yield _LOCAL_GRPC + ":" + str(server.port)
    server.shutdown()
    thread.join()


@contextmanager
def new_token_db(tokens, profiles, monkeypatch, tmp_path):
    token_db_path = tmp_path / "token.db"
    monkeypatch.setattr("fal_teller.server._server.TOKEN_DB_PATH", str(token_db_path))
    with shelve.open(str(token_db_path)) as db:
        db["tokens"] = tokens
        db["profiles"] = profiles
    yield


@pytest.fixture
def client(monkeypatch, tmp_path):
    tmp_path.mkdir(exist_ok=True)

    data_dirs = {"data": tmp_path / "data", "final": tmp_path / "final"}

    for data_dir in data_dirs.values():
        data_dir.mkdir()

    profiles = {
        dir_name: {
            "type": "file",
            "params": {"file_system": "file", "root_path": str(data_dir)},
        }
        for dir_name, data_dir in data_dirs.items()
    }
    tokens = {"admin": {"profiles": list(profiles.keys())}}
    with new_token_db(tokens, profiles, monkeypatch, tmp_path):
        with teller_server() as url:
            yield TellerClient(
                url,
                token="admin",
                default_profile="data",
            )


def test_writing(client):
    fruits = ["apple", "orange", "mango", "cherry"]
    dataframe = pandas.DataFrame({"fruits": fruits})
    client.write(dataframe, to="healty_foods")


def test_reading(client):
    fruits = ["apple", "orange", "mango", "cherry"]
    dataframe = pandas.DataFrame({"fruits": fruits})
    client.write(dataframe, to="healty_foods")

    dataframe_2 = client.read("healty_foods")
    assert dataframe.equals(dataframe_2)


def test_mixed(client):
    fruits = ["apple"]
    dataframe = pandas.DataFrame({"fruits": fruits, "prices": [1]})
    client.write(dataframe, to="healty_foods")

    vegetables = ["carrot"]
    dataframe = pandas.DataFrame({"vegetables": vegetables, "prices": [1]})
    client.write(dataframe, to="vegetables", profile_name="final")

    healty_foods_df = client.read("healty_foods")

    # We can't read this from the default profile since we saved
    # it to the 'final' profile.
    with pytest.raises(Exception):
        vegetables_df = client.read("vegetables")

    vegetables_df = client.read("vegetables", profile_name="final")

    new_healty_foods_df = pandas.merge(healty_foods_df, vegetables_df, on="prices")
    client.write(new_healty_foods_df, to="healty_foods")

    mixed_df = client.read("healty_foods")
    assert list(mixed_df["fruits"]) == ["apple"]
    assert list(mixed_df["vegetables"]) == ["carrot"]
