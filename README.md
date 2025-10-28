
# Projet : Scrapers Interpol (Red & Yellow Notices)

Ce projet développe des outils de scraping  en Python pour collecter l'intégralité des notices publiques d'Interpol. Il inclut deux scripts distincts :

1.  **`main.py`** : Pour les **Notices Rouges** (personnes recherchées pour arrestation).
2.  **`yellow_scraper.py`** : Pour les **Notices Jaunes** (personnes disparues).

Chaque scraper est optimisé pour garantir une **collecte exhaustive** des données (en contournant les limites de l'API) et un **traitement efficace**. Le scraper des Notices Rouges utilise le **multi-threading** pour la performance, tandis que celui des Notices Jaunes intègre des phases d'**auto-vérification et de rattrapage** pour assurer la complétude des données.

## Utilisation

Sur l'environnement **Onyxia**, vous pouvez exécuter directement les scripts.

### 1\. Scraper les Notices Rouges (`main.py`)

Ce script est optimisé pour la performance grâce au parallélisme.

```bash
python3 main.py [OPTIONS]
```

#### Arguments :

  * `--output <fichier.csv>` : Nom du fichier CSV de sortie. *Défaut :* `interpol_red_notices.csv`
  * `--workers <nombre>` : Nombre de threads pour le traitement parallèle (Phase 3). *Défaut :* `20`
  * `--delay <secondes>` : Délai entre les appels API *pendant les phases de collecte* (Phases 1 & 2). *Défaut :* `0.5`
  * `--max-pages <nombre>` : (Optionnel) Limite les pages de la Phase 1 pour des tests rapides.

#### Exemples :

```bash
# Lancement simple avec les paramètres par défaut
python3 main.py

# Lancement personnalisé avec 30 workers et un fichier de sortie "red_notices_complet.csv"
python3 main.py --workers 30 --output red_notices_complet.csv
```

-----

### 2\. Scraper les Notices Jaunes (`yellow_scraper.py`)

Ce script est optimisé pour l'exhaustivité et l'auto-correction. Il exécute toutes ses phases automatiquement.

```bash
python3 yellow_scraper.py
```

#### Fichiers générés :

  * `interpol_yellow_smart_all.csv` : Données brutes initiales.
  * `yellow_missing_report.csv` : Rapport de vérification de complétude.
  * `interpol_yellow_smart_all_final.csv` : Fichier final consolidé après toutes les phases de rattrapage.

-----

## Architecture des Scrapers

### Architecture Commune (Notices Rouges & Jaunes)

Les deux scripts partagent une logique fondamentale pour la collecte exhaustive :

  * **Collecte Globale :** Une première passe rapide pour les données facilement accessibles.
  * **Collecte Récursive :** Une stratégie de "diviser pour régner" (par pays, sexe, puis tranches d'âge) est utilisée pour contourner les limites de pagination de l'API et garantir la découverte de 100% des notices.

### Spécificités du Scraper des Notices Rouges (`main.py`)

  * **Traitement Parallèle (Phase 3) :** Utilise un `ThreadPoolExecutor` pour télécharger et normaliser simultanément les détails de milliers de notices, accélérant considérablement l'exécution.
  * **Enrichissement :** Calcul de l'âge, conversion des codes pays, classification des infractions.

### Spécificités du Scraper des Notices Jaunes (`yellow_scraper.py`)

  * **Approche Séquentielle :** Le volume de données étant moindre, le traitement se fait séquentiellement.
  * **Champs Spécifiques :** Collecte des données propres aux notices jaunes (ex: `birth_name`, `country_of_birth`, `images_url`).
  * **Auto-Vérification & Rattrapage :** Inclut des étapes post-collecte pour comparer les données locales à l'API et relancer des requêtes ciblées pour compléter les éventuels manques (y compris par "pays de naissance").

-----
