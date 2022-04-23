# Discussion
# modes = ["walk",
#          "pt", # pt, walk_pt_walk
#          "car", "car_pt", "pt_car", # walk included
#          "carp", # "carp_pt", "pt_carp", # car_passenger
#          #"2r", "2r_pt_2r", "2r_pt_OR_pt_2r", # moto, scooter
#          "bicycle"] #, "bicycle_pt_bicycle", "bicycle_pt_OR_pt_bicycle"]
import pandas as pd
import numpy as np

# Number of trips
# As a main trip mode
MODES_lst1 = ["walk",
              "pt",
              "car",
              "car_passenger",
              "2RM", # moto, scooter
              "bike"]

# With "car_pt" & "pt_car"
MODES_lst2 = ["walk",
              "pt",     # without "car_pt"&"pt_car" where pt is a maim trip mode
              "car",    # without "car_pt"&"pt_car" where car is a maim trip mode
              "car_pt", "pt_car", # walk included
              "car_passenger",
              "2RM", # moto, scooter
              "bicycle"]

def configure(context):
    context.stage("data.hts.egt_2018.filtered")
    context.config("output_path") # output_path = r"D:\MATSim_egt2018\output_egt2018"

def execute(context):
    df_persons, df_trips, df_routes = context.stage("data.hts.egt_2018.filtered")

    df_trips["mode_5"].unique() # check transport modes: ['pt', 'walk', 'car', 'car_passenger', 'bike']
    df_trips.groupby(["mode_5"])["trip_weight"].sum()

    df_trips["mode_7"].unique() # ['pt', 'walk', 'car', 'other', 'car_passenger', '2RM', 'bike']
    df_trips.groupby(["mode_7"])["trip_weight"].sum()

    # Intermode_5
    df_routes["mode_route_5"] = np.where(df_routes["mode_route_5"].isin(["car","pt"]),df_routes["mode_route_5"],"") # df_routes.head(20)

    df_trips_routes = pd.merge(df_routes, df_trips, how="left",
                               on = ["person_id", "egt_household_id","egt_person_id","egt_household_person_id","egt_trip_id"]) # 58942 rows

    df_trips_routes = df_trips_routes[["person_id","egt_trip_id", "mode_route_5"]]
    df_trips_routes = df_trips_routes.groupby(["person_id","egt_trip_id"], as_index=False).agg({"mode_route_5": lambda x: "".join(x)})

    df_trips_routes = pd.merge(df_trips, df_trips_routes, how="left", on = ["person_id","egt_trip_id"]) # 33354 rows

    df_trips_routes["intermode_5"] = df_trips_routes["mode_5"].astype(str)
    df_trips_routes.loc[df_trips_routes["mode_route_5"].str.contains("carpt", na=False), "intermode_5"] = "car_pt"
    df_trips_routes.loc[df_trips_routes["mode_route_5"].str.contains("ptcar", na=False), "intermode_5"] = "pt_car"

    df_trips_routes["intermode_5"] = df_trips_routes["intermode_5"].astype("category")

    df_trips_routes["intermode_5"].unique() # ['pt', 'walk', 'car', 'car_passenger', 'bike', 'pt_car', 'car_pt']
    df_trips_routes.groupby(["intermode_5"])["trip_weight"].sum()
    df_trips_routes.groupby(["intermode_5"])["trip_weight"].count()

    def ms_mode_cat(df, mode_cat, ms_name):
        ms = df.groupby([mode_cat])["trip_weight"].sum() \
            .reset_index(name = "nb_trips_W").rename(columns = {mode_cat:"mode"})
        ms["percentage"] = 100 * ms.nb_trips_W / ms.nb_trips_W.sum()
        ms.insert(loc=0, column = "modal_share_id", value = ms_name)
        return ms

    # Modal share: Weighted trips, for mode_5 (i.e. 5 categories)
    ms = ms_mode_cat(df_trips_routes, mode_cat = "mode_5", ms_name = "MS_mode_5")
    ms_valid = ms.copy()

    # Modal share: Weighted trips, for intermode_5 (i.e. 5 categories)
    ms = ms_mode_cat(df_trips_routes, mode_cat = "intermode_5", ms_name = "MS_intermode_5")
    ms_valid = pd.concat([ms_valid,ms])

    # Intermode_7
    df_routes["mode_route_7"] = np.where(df_routes["mode_route_7"].isin(["car","pt"]),df_routes["mode_route_7"],"")

    df_trips_routes = pd.merge(df_routes, df_trips, how="left",
                               on = ["person_id", "egt_household_id","egt_person_id","egt_household_person_id","egt_trip_id"]) # 58942 rows

    df_trips_routes = df_trips_routes[["person_id","egt_trip_id", "mode_route_7"]]
    df_trips_routes = df_trips_routes.groupby(["person_id","egt_trip_id"], as_index=False).agg({"mode_route_7": lambda x: "".join(x)})

    df_trips_routes = pd.merge(df_trips, df_trips_routes, how="left", on = ["person_id","egt_trip_id"]) # 33354 rows

    df_trips_routes["intermode_7"] = df_trips_routes["mode_7"].astype(str)
    df_trips_routes.loc[df_trips_routes["mode_route_7"].str.contains("carpt", na=False), "intermode_7"] = "car_pt"
    df_trips_routes.loc[df_trips_routes["mode_route_7"].str.contains("ptcar", na=False), "intermode_7"] = "pt_car"

    df_trips_routes["intermode_7"] = df_trips_routes["intermode_7"].astype("category")

    # df_trips_routes["intermode_7"].unique() # ['pt', 'walk', 'car', 'other', ..., '2RM', 'bike', 'pt_car', 'car_pt']

    # Modal share: Weighted trips, for mode_7 (i.e. 5 categories)
    ms = ms_mode_cat(df_trips_routes, mode_cat = "mode_7", ms_name = "MS_mode_7")
    ms_valid = pd.concat([ms_valid,ms])

    # Modal share: Weighted trips, for intermode_7 (i.e. 5 categories)
    ms = ms_mode_cat(df_trips_routes, mode_cat = "intermode_7", ms_name = "MS_intermode_7")
    ms_valid = pd.concat([ms_valid,ms])

    return ms_valid