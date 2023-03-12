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

## Compilation

Pour compiler le projet, il suffit de lancer la commande suivante :

```bash
mvn clean compile assembly:single -U
```

## Exécution

Pour exécuter le projet, il suffit de lancer la commande suivante :

```bash
spark-submit --deploy-mode client --class fr.airline.spark.Main target/SparkAirline-1.0-SNAPSHOT-jar-with-dependencies.jar <chemin sur le HDFS du dataset des vols> <chemin sur le HDFS du dataset des aéroports> <chemin sur le HDFS du dataset des avions déréférencés> <chemin sur le HDFS du dataset des références pour les immatriculations d\'avions>
```

## Structure du projet

Le projet est composé de 3 packages :

- `fr.airline.spark.model` : contient les classes représentant les données du dataset
- `fr.airline.spark` : contient le point d'entrée du programme


## Auteurs

- Simon HEBAN
- Ludwig SILVAIN
