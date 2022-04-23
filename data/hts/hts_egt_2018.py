import pandas as pd
import numpy as np

TRIP_COLUMNS = [
    "person_id", "departement_id", "trip_weight",
    "mode_5", "mode_7",
    "origin_departement_id", "destination_departement_id"
]

ROUTE_COLUMNS = [
    "person_id",
    "mode_route_5", "mode_route_7"
]

def check(df_trips, df_routes):
    for column in TRIP_COLUMNS:
        assert column in df_trips

    for column in ROUTE_COLUMNS:
        assert column in df_routes

    if not ("routed_distance" in df_trips or "euclidean_distance" in df_trips):
        assert False

    if "routed_distance" in df_trips:
        assert not (df_trips["routed_distance"].isna()).any()

    if "euclidean_distance" in df_trips:
        assert not (df_trips["euclidean_distance"].isna()).any()
