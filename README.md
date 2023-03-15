<h1 align="center">Airline Spark</h1>
<p align="center">Simon HEBAN & Ludwig SILVAIN</p>
<p align="center">
    <a href="https://www.kaggle.com/datasets/bulter22/airline-data">
        Dataset sur les vols sur Kaggle
    </a>
    <a href="https://raw.githubusercontent.com/datasets/airport-codes/master/data/airport-codes.csv">
        Dataset sur les aéroports
    </a>
    <a href="https://www.faa.gov/licenses_certificates/aircraft_certification/aircraft_registry/releasable_aircraft_download">
        Dataset sur les immatriculations d'avions
    </a>
</p>

## Description

Pour notre projet, nous avons sélectionné un dataset regroupant l’ensemble des vols commerciaux aux Etats Unis entre
octobre 1987 et Avril 2008. Ce jeu de données est donc conséquent : 12Go pour environ 120 Millions d’entrées.

Dataset :
- shuffled_airline.csv : Un sous-ensemble du dataset original, contenant 10 000 vols
- dataset_join/airport-codes.csv : Le dataset des aéroports
- dataset_join/DEREG.csv : Le dataset des avions déréférencés
- dataset_join/ACFTREF.csv : Le dataset des références pour les immatriculations d'avions

## Compilation

Pour compiler le projet, il suffit de lancer la commande suivante :

```bash
mvn clean compile assembly:single -U
```

## Exécution

Pour exécuter le projet, il suffit de lancer la commande suivante :

```bash
spark-submit --deploy-mode client --class fr.airline.spark.Main target/SparkAirline-1.0-SNAPSHOT-jar-with-dependencies.jar <chemin sur le HDFS du dataset des vols> <chemin sur le HDFS du dataset des aéroports> <chemin sur le HDFS du dataset des avions déréférencés> <chemin sur le HDFS du dataset des références pour les immatriculations d\'avions> <chemin sur le HDFS pour l\'exporation parquet>
```

Exemple :

```bash
spark-submit --deploy-mode client --class fr.airline.spark.Main funWithSpark.jar hdfs://localhost:9000/shuffled_airline.csv hdfs://localhost:9000/airport-codes.csv hdfs://localhost:9000/DEREG.csv hdfs://localhost:9000/ACFTREF.csv hdfs://localhost:9000/shuffled_airline.parquet
```

**Attention : le dataset ne doit pas contenir les entêtes.**

## Structure du projet

Le projet est composé de 3 packages :

- `fr.airline.spark.model` : contient les classes représentant les données du dataset
- `fr.airline.spark` : contient le point d'entrée du programme


## Auteurs

- Simon HEBAN
- Ludwig SILVAIN
