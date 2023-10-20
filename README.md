# LLDM-gcp-project


Projet réalisé dans le cadre de l’UE de Gestion des données distribuées à large échelle.

**Enseignement** : Molli Pascal

**Diplome** : Master 2 Informatique, Université de Nantes

**Campus** : Michelet-Sciences

**Date** : 20/10/2023



## Authors

- [@LouAnneSVTR](https://www.github.com/LouAnneSVTR)


## Introduction

L'algorithme PageRank est un algorithme clé pour le classement des pages web, inventé par les fondateurs de Google, Larry Page et Sergey Brin. Il évalue la pertinence des pages en fonction du nombre et de la qualité des liens pointant vers elles. PageRank a révolutionné les moteurs de recherche et est au cœur de l'analyse de réseaux.

Ce projet vise à  comparer les performances de deux cadres de traitement de données populaires, Pig et PySpark, dans le contexte de l'application de l'algorithme PageRank à des graphes massifs. Cette comparaison est inspirée d'une démonstration qui a eu lieu lors de la conférence NDSI 2012, où les Resilient Distributed Datasets (RDD) ont été présentés comme un outil révolutionnaire pour le traitement de données distribuées.
Nous effectuerons cette comparaison avec l'outil Google Cloud PLatform (GCP).


# Objectifs du Projet
Les objectifs de notre projet sont les suivants :

1. **Rédaction du Code Python pour PageRank** : Développement d'un code Python pour mettre en œuvre PageRank avec Pig et PySpark, en prenant en charge 2, 4 et 6 nœuds.

2. **Tests Initiaux** : Effectuer des tests préliminaires de PageRank avec 2 nœuds et un petit fichier de 20 mo pour évaluer les performances initiales.

3. **Tests Finaux** : Réaliser des tests finaux en utilisant un fichier de données de 800 mo pour évaluer les performances sur des graphes de plus grande envergure.

4. **Comparaison** : Annalyse comparative entre nos résultats pour Pig et PySpark.

Ces objectifs aideront à évaluer l'efficacité des implémentations PageRank sur GCP, en vue de futurs projets d'analyse de graphes à grande échelle.

Les fichiers sources : https://github.com/momo54/large_scale_data_management

## 1 - Configuration

Afin d'évaluer les performances entre les implémentations Pig et PySpark, nous avons utilisé le service d'exécution de tâches Dataproc de la suite Google Cloud. NOus avons décidé d'utiliser Python pour réaliser notre projet. Aussi nous avons comme fichiers : 

**Code sources Pig et PySpark** : large_scale_data_management
/dataproc.py et large_scale_data_management/pyspark
/pagerank.py.

**Configuration du Cluster** : Nous avons défini la région du cluster sur "europe-central2", type de machine "machine_type_uri": "n1-standard-4.

**Nombre de Nœuds** : Selon la consigne, ous avons utilisé 2, 4 et 6 nœuds en fonction des restrictions de quota et des besoins en puissance de calcul pour exécuter les algorithmes.

**Données d'Entrée** : Nous avons préchargé le jeu de données "lddm_data/small_page_links.nt 3"" dans le bucket public gs://public_lddm_data/small_page_links.nt 3" au sein de l'impémentation Pig et dasn la commande de lancement de PySpark.

**Licences** : http://www.apache.org/licenses/LICENSE-2.0 pour PySpark.
