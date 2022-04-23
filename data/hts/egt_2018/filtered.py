import data.hts.hts_egt_2018 as hts
import pandas as pd
import numpy as np

"""
This stage filters out EGT observations which live or work outside of
Île-de-France.
"""

def configure(context):
    context.stage("data.hts.egt_2018.cleaned")
    context.stage("data.spatial.codes")

def execute(context):
    df_codes = context.stage("data.spatial.codes")
    assert (df_codes["region_id"] == 11).all() # Otherwise EGT doesn't make sense

    df_persons, df_trips, df_routes = context.stage("data.hts.egt_2018.cleaned") # 8485, 34423 rows, 60913

    # Filter for non-residents
    requested_departments = df_codes["departement_id"].unique()
    # f = df_trips["departement_id"].astype(str).isin(requested_departments) # pandas bug!
    # df_trips = df_trips[f]

    # Filter for people going outside of the area (because they have NaN distances)
    remove_ids = set()

    remove_ids |= set(df_trips[
        ~df_trips["origin_departement_id"].astype(str).isin(requested_departments) |
        ~df_trips["destination_departement_id"].astype(str).isin(requested_departments)
    ]["person_id"].unique())

    remove_ids |= set(df_trips[
        ~df_trips["departement_id"].astype(str).isin(requested_departments) # Filter for non-residents
    ]["person_id"].unique())

    df_persons = df_persons[~df_persons["person_id"].isin(remove_ids)] # 8178

    # Only keep trips and households that still have a person
    df_trips = df_trips[df_trips["person_id"].isin(df_persons["person_id"].unique())] # 33354 rows
    df_routes = df_routes[df_routes["person_id"].isin(df_persons["person_id"].unique())] # 58941

    # Finish up
    df_persons = df_persons[["egt_household_person_id", "person_id"]]
    df_trips = df_trips[hts.TRIP_COLUMNS
                        + ["euclidean_distance"]
                        + ["egt_household_id", "egt_person_id", "egt_household_person_id", "egt_trip_id"]]
    df_routes = df_routes[hts.ROUTE_COLUMNS
                        + ["egt_household_id", "egt_person_id", "egt_household_person_id", "egt_trip_id", "egt_route_id"]]

    hts.check(df_trips, df_routes)

    return df_persons, df_trips, df_routes
