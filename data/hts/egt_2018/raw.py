import simpledbf
from tqdm import tqdm
import pandas as pd
import os

"""
This stage loads the raw data of the Île-de-France HTS (EGT).
"""

DEPLACEMENTS_COLUMNS = [
    "IDENT",        # Identifiant ménage créé par le logiciel
    "RESDEP",       # Couronne de résidence (1: Paris, 2: Petite Couronne, 3: Grande Couronne)
    "NP",           # Numéro de la personne dans le ménage
    "ND",           # Numéro du déplacement réalisé par la personne
    "INTERNE_IDF",  # Type de déplacements en Île-de-France (1 - Interne à l'ÎdF, 2 - Echange avec l'ÎdF, 3 - Externe à l'ÎdF (poids null par défaut))
    "POIDS",        # Poids du déplacement (= poids de la personne ayant réalisé le déplacement)
    "ORDEP",        # Département d'origine du déplacement
    "ORHOR",        # Horaire de départ du déplacement (après minuit : 24h et plus)
    "DESTDEP",      # Département de destination du déplacement
    "DESTHOR",      # Horaire d'arrivée du déplacement (après minuit : 24h et plus)
    "DPORTEE",      # Portée (distance à vol d'oiseau) du déplacement en km : apurement à continuer (filtrer les valeurs étranges si besoin)
    "MODP_H7"]      # Mode principal en 7 catégories (A1_TC, A2_Voiture conducteur, A3_Voiture passger, A4_2RM, A5_Vélo, A6_Autres, A7_Marche)

TRAJETS_COLUMNS = [
    "IDENT",        # Identifiant ménage créé par le logiciel
    "NP",           # Numéro de la personne dans le ménage
    "ND",           # Numéro du déplacement réalisé par la personne
    "NT",           # Numéro du trajet dans le déplacement
    "MOYEN"]        # Moyen de transport utilisé (cf. onglet Motifs&Moyens)

def configure(context):
    context.config("data_path") # data_path = r"D:/L_MATSim_IdF/data"

def execute(context):
    df_deplacements = pd.read_excel(
        "%s/egt_2018/EGT_2018.xlsx" % context.config("data_path"), engine='openpyxl', sheet_name='2_depl', usecols = DEPLACEMENTS_COLUMNS
    ) # 34447 rows

    df_trajets = pd.read_excel(
        "%s/egt_2018/EGT_2018.xlsx" % context.config("data_path"), engine='openpyxl', sheet_name='2_traj', usecols = TRAJETS_COLUMNS
    ) # 60952 rows

    return df_deplacements, df_trajets

def validate(context):
    name = "EGT_2018.xlsx"
    if not os.path.exists("%s/egt_2018/%s" % (context.config("data_path"), name)):
        raise RuntimeError("File missing from EGT: %s" % name)

    return [
        os.path.getsize("%s/egt_2018/EGT_2018.xlsx" % context.config("data_path"))
    ]
