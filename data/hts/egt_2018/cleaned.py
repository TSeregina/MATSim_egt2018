from tqdm import tqdm
import pandas as pd
import numpy as np
import data.hts.hts_egt_2018 as hts

"""
This stage cleans the regional HTS (EGT).
"""

def configure(context):
    context.stage("data.hts.egt_2018.raw")

# MODP_H7
MODES_MAP_5 = {
    "A1_TC" : "pt",
    "A2_Voiture conducteur" : "car",
    "A3_Voiture passger" : "car_passenger",
    "A4_2RM" : "car",
    "A5_Vélo" : "bike",
    #"A6_Autres" : "pt", # default (other)
    "A7_Marche" : "walk"
}

MODES_MAP_7 = {
    "A1_TC" : "pt",
    "A2_Voiture conducteur" : "car",
    "A3_Voiture passger" : "car_passenger",
    "A4_2RM" : "2RM",
    "A5_Vélo" : "bike",
    #"A6_Autres" : "other", # default (other)
    "A7_Marche" : "walk"
}

MODES_ROUTE_MAP_7 = {
    "B11" : "pt",               # Transports collectifs : Train ou RER
    "B12" : "pt",               # Transports collectifs : Métro
    "B13" : "pt",               # Transports collectifs : Tramway
    "B14" : "pt",               # Transports collectifs : Bus
    "B15" : "pt",               # Transports collectifs : Autres
    "B511" : "other",	        # Taxi, Uber ou autres VTC + Taxi (artisan taxi, G7, taxis-bleus…)
    "B512" : "other",	        # Taxi, Uber ou autres VTC + Über
    "B513" : "other",	        # Taxi, Uber ou autres VTC + Autres VTC
    "B51" : "other",	        # Taxi, Uber ou autres VTC + non réponse
    "B521" : "outside",         # Avion
    "B522" : "outside",         # TER ou TGV
    "B529" : "outside",         # Cars interurbains dits "cars Macron"
    "B524" : "outside",         # Transports collectifs hors Ile-de-France (bus, car, tramway, métro, TER…)
    "B525" : "pt",              # Autres modes + Ramassage scolaire
    "B526" : "pt",              # Autres modes + Transport employeur, navette d'entreprise
    "B527" : "pt",              # Autres modes + Transport à la demande (TAD)
    "B528" : "pt",              # Autres modes + Transport spécialisé pour les personnes à mobilité réduite (dont PAM)
    "B21" : "car",              # Voiture + Conducteur
    "B31" : "2RM",              # Moto, scooter + Conducteur
    "B22" : "car_passenger",    # Voiture + Passager
    "B32" : "2RM",              # Moto, scooter + Passager
    "B40" : "bike",             # Vélo
    "B52" : "other",            # Autres modes + non réponse
    "B523" : "other",           # Autres modes + Autre
    "B622" : "walk",            # Fauteuil roulant + Fauteuil roulant motorisé ou scooter PMR
    "B621" : "walk",            # Fauteuil roulant + Fauteuil roulant manuel
    "B62" : "walk",             # Fauteuil roulant + non réponse
    "B615" : "walk",            # Marche + Trottinette électrique
    "B614" : "walk",            # Marche + Trottinette
    "B616" : "walk",            # Marche + Autres
    "B611" : "walk",            # Marche + A pied
    "B61" : "walk"              # Marche + non réponse
}

MODES_ROUTE_MAP_5 = {k:("pt" if v == "other" else
                        "car" if v == "2RM" else v) for (k,v) in MODES_ROUTE_MAP_7.items()}

def execute(context):
    df_trips, df_routes = context.stage("data.hts.egt_2018.raw") # df_trips = df_deplacements; df_routes = df_trajets
    # Make copies
    df_trips = pd.DataFrame(df_trips, copy = True)
    df_routes = pd.DataFrame(df_routes, copy = True)

    df_trips = df_trips.rename({"IDENT":"egt_household_id",
                                "NP":"egt_person_id",
                                "ND":"egt_trip_id"}, axis=1)

    df_routes = df_routes.rename({"IDENT":"egt_household_id",
                                  "NP":"egt_person_id",
                                  "ND":"egt_trip_id",
                                  "NT":"egt_route_id"}, axis=1)

    df_trips["egt_household_person_id"] = df_trips.egt_household_id.astype(str) + "_" + df_trips.egt_person_id.astype(str)
    df_routes["egt_household_person_id"] = df_routes.egt_household_id.astype(str) + "_" + df_routes.egt_person_id.astype(str)

    df_persons = df_trips[["egt_household_person_id"]].copy()
    df_persons = df_persons.drop_duplicates()
    df_persons["person_id"] = np.arange(len(df_persons))+1 # 8492 rows

    df_trips = pd.merge(df_trips, df_persons, on = "egt_household_person_id")
    df_routes = pd.merge(df_routes, df_persons, on = "egt_household_person_id")
#    pd.set_option('display.max_columns', None)

    # df_tmp = pd.merge(df_routes, df_trips, how="left", on = ["egt_household_id","egt_person_id","egt_household_person_id","egt_trip_id"])
    # df_tmp.to_csv("%s/egt_2018/EGT_2018_trip_route.csv" % data_path, sep = ";", index = None)

    # Weight
    df_trips["trip_weight"] = df_trips["POIDS"].astype(np.float)

    # Clean departement
    df_trips["departement_id"] = df_trips["RESDEP"].astype(str).str[:2] # extract only department in cases s.a. "75113"
    df_trips["origin_departement_id"] = df_trips["ORDEP"].astype(str).str.split(".").str[0].astype("category") # clean cases s.a. "#VALUE"
    df_trips["destination_departement_id"] = df_trips["DESTDEP"].astype(str).str.split(".").str[0].astype("category") # clean cases s.a. "#VALUE"

    # Trip mode
    df_trips["mode_5"] = "pt"
    df_trips["mode_7"] = "other"
    for category, mode in MODES_MAP_5.items():
        df_trips.loc[df_trips["MODP_H7"] == category, "mode_5"] = mode
    for category, mode in MODES_MAP_7.items():
        df_trips.loc[df_trips["MODP_H7"] == category, "mode_7"] = mode

    df_trips["mode_5"] = df_trips["mode_5"].astype("category")
    df_trips["mode_7"] = df_trips["mode_7"].astype("category")

    # Route mode
    df_routes["MOYEN"] = df_routes["MOYEN"].str.split(",").str[0] # clean case s.a. "B616,SKATEBOARD"
    df_routes["mode_route_5"] = "pt"
    df_routes["mode_route_7"] = "other"
    for category, mode in MODES_ROUTE_MAP_5.items():
        df_routes.loc[df_routes["MOYEN"] == category, "mode_route_5"] = mode
    for category, mode in MODES_ROUTE_MAP_7.items():
        df_routes.loc[df_routes["MOYEN"] == category, "mode_route_7"] = mode

    df_routes["mode_route_5"] = df_routes["mode_route_5"].astype("category")
    df_routes["mode_route_7"] = df_routes["mode_route_7"].astype("category")

    # Further trip attributes
    df_trips["euclidean_distance"] = df_trips["DPORTEE"] * 1000.0 # convert to meters

    # Trip times
    df_trips["departure_time"] = df_trips["ORHOR"] * 60.0
    df_trips["arrival_time"] = df_trips["DESTHOR"] * 60.0

    # Passenger attribute
    df_trips["is_passenger"] = df_trips["mode_5"] == "car_passenger"

    # Drop people that have NaN departure or arrival times in trips
    # Filter for people with NaN departure or arrival times in trips
    f = df_trips["departure_time"].isna()
    f |= df_trips["arrival_time"].isna()

    f = df_trips["person_id"].isin(df_trips[f]["person_id"].unique())

    nan_count = len(df_trips[f]["person_id"].unique())
    total_count = len(df_trips["person_id"].unique())

    print("Dropping %d/%d persons because of NaN values in departure and arrival times" % (nan_count, total_count))

    df_trips = df_trips[~f] # 34423 rows, i.e. 34448 - 25
    df_persons = df_persons[df_persons["person_id"].isin(df_trips["person_id"].unique())] # 8485 rows, i.e. 8492-7
    df_routes = df_routes[df_routes["person_id"].isin(df_persons["person_id"].unique())] # 60913 rows

    return df_persons, df_trips, df_routes
