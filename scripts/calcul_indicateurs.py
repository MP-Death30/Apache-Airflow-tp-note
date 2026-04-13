#!/usr/bin/env python3
import json
import logging
import os
from typing import Optional, List
import numpy as np
import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def calculer_zscore(
    valeur_actuelle: float, historique: List[float]
) -> Optional[float]:
    valeurs_valides = [v for v in historique if v is not None]
    if len(valeurs_valides) < 3:
        logger.warning(f"Historique insuffisant ({len(valeurs_valides)} saisons)")
        return None
    moyenne = np.mean(valeurs_valides)
    ecart_type = np.std(valeurs_valides, ddof=1)
    if ecart_type == 0:
        return 0.0
    return float((valeur_actuelle - moyenne) / ecart_type)

def classifier_statut_ias(
    valeur_ias: float, seuil_min: Optional[float], seuil_max: Optional[float]
) -> str:
    if seuil_max is not None and valeur_ias >= seuil_max:
        return "URGENCE"
    if seuil_min is not None and valeur_ias >= seuil_min:
        return "ALERTE"
    return "NORMAL"

def classifier_statut_zscore(
    z_score: Optional[float], seuil_alerte_z: float = 1.5, seuil_urgence_z: float = 3.0
) -> str:
    if z_score is None:
        return "NORMAL"
    if z_score >= seuil_urgence_z:
        return "URGENCE"
    if z_score >= seuil_alerte_z:
        return "ALERTE"
    return "NORMAL"

def classifier_statut_final(statut_ias: str, statut_zscore: str) -> str:
    if "URGENCE" in (statut_ias, statut_zscore):
        return "URGENCE"
    if "ALERTE" in (statut_ias, statut_zscore):
        return "ALERTE"
    return "NORMAL"

def calculer_r0_simplifie(
    series_hebdomadaire: List[float], duree_infectieuse: int = 5
) -> Optional[float]:
    series_valides = [v for v in series_hebdomadaire if v is not None and v > 0]
    if len(series_valides) < 2:
        return None
    croissances = [
        (series_valides[i] - series_valides[i-1]) / series_valides[i-1]
        for i in range(1, len(series_valides))
    ]
    if not croissances:
        return None
    return max(0.0, float(1 + np.mean(croissances) * (duree_infectieuse / 7)))