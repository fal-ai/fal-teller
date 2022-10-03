import random
from collections import Counter

import pandas as pd

from fal_teller.client import TellerClient


def seed_data(teller_client: TellerClient) -> None:
    fruits = ["apple", "orange", "mango", "cherry"]
    dataframe = pd.DataFrame({"fruits": random.choices(fruits, k=10000)})

    # Write is a generic function that can infer the format of the data
    # by looking at the given dataframe.
    teller_client.write(dataframe, to="healty_foods")


def process_data(teller_client: TellerClient) -> None:
    healty_foods = teller_client.read("healty_foods")

    raw_fruit_stats = Counter(healty_foods["fruits"])
    fruit_stats = pd.DataFrame({"fruits": raw_fruit_stats})
    teller_client.write(fruit_stats, to="fruits_stats")


teller_client = TellerClient(
    "grpc://localhost:1997",
    token="database_token",
    default_profile="profile_3",
)
seed_data(teller_client)
process_data(teller_client)
print(teller_client.read("fruits_stats"))
