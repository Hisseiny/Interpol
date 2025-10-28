# Interpol

# Projet : Scraper des Notices Rouges d'Interpol

Ce script Python (`main.py`) est conçu pour collecter, traiter et exporter l'intégralité des notices rouges publiques d'Interpol.

Son architecture résout deux défis majeurs :

1.  **Les limites de l'API :** L'API publique d'Interpol limite les résultats (par ex. 160 par requête et 50 pages au total). Ce script implémente une stratégie de collecte récursive pour contourner ces limites et assurer une collecte exhaustive.
2.  **Le temps d'attente (I/O) :** Le téléchargement des détails de milliers de notices est un goulot d'étranglement. Le script utilise le multi-threading pour paralléliser ces téléchargements, réduisant le temps d'exécution de plusieurs heures à quelques minutes.

## Fonctionnalités Principales

  * **Collecte Exhaustive :** Garantit la collecte de 100% des notices en divisant récursivement les requêtes (par pays, sexe, puis âge) pour rester sous la limite de 160 résultats par appel.
  * **Traitement Parallèle :** Utilise un `ThreadPoolExecutor` pour télécharger et normaliser simultanément les détails de milliers de notices.
  * **Enrichissement des Données :** Nettoie les données brutes, calcule l'âge à partir de la date de naissance, convertit les codes pays ISO en noms complets et classifie les infractions dans des catégories claires.
  * **Déduplication :** Assure un export unique pour chaque notice en gérant les doublons via un `set` (`seen_ids`) durant la collecte.

## Architecture du Script en 4 Phases

L'exécution est divisée en phases logiques pour séparer la collecte (lente, séquentielle) du traitement (rapide, parallèle).

### Phase 1 : Collecte Globale (Initiale)

  * **Objectif :** Obtenir un premier jeu de données large et rapide.
  * **Méthode :** Parcourt les pages globales de l'API (limitées à \~50 pages) et stocke les tâches brutes dans la file d'attente `tasks_to_fetch`.

### Phase 2 : Collecte Récursive (Exhaustive)

  * **Objectif :** Atteindre 100% des notices en contournant les limites de l'API.
  * **Méthode :** Itère sur chaque pays et applique une stratégie de "diviser pour régner". Si un pays a plus de 160 notices, la requête est divisée par sexe, puis récursivement par tranches d'âge, jusqu'à ce que chaque sous-requête soit gérable.

### Phase 3 : Traitement Parallèle (Téléchargement & Normalisation)

  * **Objectif :** Le cœur de l'optimisation. Télécharger les détails de chaque tâche de la file d'attente le plus vite possible.
  * **Méthode :** Un `ThreadPoolExecutor` déploie de multiples *workers* (threads). Ces workers appellent `fetch_detail()` et `normalize_notice()` simultanément, éliminant le temps d'attente lié aux requêtes réseau.

### Phase 4 : Exportation Finale (CSV)

  * **Objectif :** Sauvegarder les résultats.
  * **Méthode :** Écrit toutes les données normalisées et collectées dans un fichier `.CSV` propre.

## Prérequis

  * Python 3.7+
  * Bibliothèques Python : `beautifulsoup4` (pour le nettoyage de texte HTML).

## Utilisation

Le script s'exécute directement depuis le terminal.

```bash
python3 main.py [OPTIONS]
```

### Arguments de la Ligne de Commande

  * `--output <fichier.csv>`
      * Spécifie le nom du fichier CSV de sortie.
      * *Défaut :* `interpol_parallel.csv`
  * `--workers <nombre>`
      * Définit le nombre de threads à utiliser pour la Phase 3 (traitement parallèle).
      * *Défaut :* `20`
  * `--delay <secondes>`
      * Définit le délai (en secondes) entre les appels API *pendant les phases de collecte* (Phases 1 & 2).
      * *Défaut :* `0.5`
  * `--max-pages <nombre>`
      * (Optionnel) Limite le nombre de pages à scraper pendant la Phase 1. Utile pour des tests rapides.

### Exemples

```bash
# Lancement simple avec les paramètres par défaut
python3 main.py

# Lancement personnalisé avec 25 workers et un fichier de sortie nommé "data.csv"
python3 main.py --workers 25 --output data.csv
```

## Structure du Fichier de Sortie (`.CSV`)

Le fichier CSV généré contient les 18 colonnes suivantes :

`name`, `forename`, `date_of_birth`, `age` (calculé), `sex`, `place_of_birth`, `nationality` (nom complet), `height`, `weight`, `hair_color`, `eye_color`, `distinguishing_marks`, `languages`, `entity_id`, `notice_id`, `warrant_country` (nom complet), `url` (lien vers la notice), `infractions` (catégories classifiées).
