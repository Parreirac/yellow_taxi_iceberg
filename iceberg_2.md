Nous avons mis en avant les promesses d’Iceberg, il s’agit maintenant de prouver nos dires. Nous devons nous frotter à un véritable environement big data, nous utiliserons pour cela un cluster Hadoop. Nous testerons un jeu de données standard. Comme Iceberg n’est qu’un format de table, nous devons choisir un moteur de calcul. Nous utiliserons Spark, _via_ son interface en python.

Nous allons montrer ici comment parametrer Iceberg, nous utiliserons Iceberg pour réaliser des premiers traitements sur un jeu de données réelles. Ceci nous permettra de tirer des premiers enseignements sur l’utilisation d’Iceberg.

Cette article est le deuxième d'une série de trois :

* Iceberg 1 : pourquoi encore un nouveau format de table ? 
* Iceberg 2 : New York taxis sur TDP, mise en place et premiers enseignements ;
* Iceberg 3 : mesures de performances.

# taxis et élephant jaunes avec Iceberg


## Le contexte

Adaltas contribue au soutien d’une solution Hadoop open source dénommée TDP pour trunk data plateform (en rapport à la trompe de l’éléphant, trunk en  anglais). Cette solution est portée par l’association TOSIT pour The Open Source I Trust, dont le [dépôt github](https://github.com/TOSIT-IO/tdp-getting-started/), fournit le guide d’installation.

Iceberg est implémenté dans de nombreux outils mais n’est pas compatible avec la version actuelle de Hive (XXX) de TDP. En revanche pour Spark (3.2.2-0.0) l’ajout d’un simple fichier jar (téléchargeable [ici](https://iceberg.apache.org/releases/#downloads)) permet d’utiliser les fonctionnalités portées par Iceberg. Python étant un langage répandu, nous utiserons pySpark.

Un site de support existe pour TDP. Il fournit des guides pour les différents composants et également des jeux de données comme celui sur [l’emploi des taxis verts à New York](https://www.alliage.io/en/academy/datasets/ny-green-taxi), qui fait 1.2 Go, avec un fichier Parquet par mois. Précisons, que les taxis sont jaunes en « centre » ville et verts à la périphérie, d’ou le nom des jeu de données. Nous utiliserons `--dataset yellow`, 27,9 Go de données sont téléchargées sur votre cluster.


## Paramètrage de pySpark

Afin de pouvoir utiliser des `spark-submit` nous devons définir les variables d’environnement suivantes :

```bash
export HADOOP_CONF_DIR=/etc/hadoop/conf
export SPARK_HOME=/opt/tdp/spark3
```

Nous devons paramétrer la configuration de spark pour utiliser Iceberg : 

```python
conf = SparkConf().setAppName("Iceberg vs Parquet with PySpark") \
                  .setAll([
      ("spark.jars" ,"/opt/tdp/spark3/jars/iceberg-spark-runtime-3.2_2.12-1.3.1.jar"),\
      ("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),\
      ("spark.sql.catalog.spark_catalog","org.apache.iceberg.spark.SparkSessionCatalog"),\
      ("spark.sql.catalog.spark_catalog.type","hive"),\
      ("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog"),\
      ("spark.sql.catalog.local.type","hadoop"),\
      ("spark.sql.catalog.local.warehouse","hdfs:///user/tdp_user/warehouse_hadoop_iceberg"),\
      ("spark.sql.defaultCatalog","local")])
```

On précise ici le chemin du fichier jar que nous avons téléchargé, puis notre catalogue, le point d’entrée des tables Iceberg. On précise son type : Spark sur le modèle Hive, avec un cluster Hadoop. Ce catalogue sera le catalog par défaut. Les données seront stockées dans un entrepot (warehouse) dont on donne le chemin. 

Le chargement d'un ensemble de fichier parquet dans un dataframe se fait en donnant le chemin des fichiers :

```python
parquet_path = "hdfs://mycluster:8020/user/tdp_user/data/nyc_yellow_taxi_trip/*.parquet"
trips_df  = spark.read.format("parquet").load(parquet_path)
```

Pour l'équivalent Iceberg on procède en deux temps : définir le schéma pour créer une table vide, puis charger les données :

```python
df = spark.createDataFrame([], CustomSchema)
df.writeTo("local.nyc.taxis_one_step").create()
trips_df = spark.read.format("parquet").load(parquet_path)
```

Dans le catalogue local, nous créons la base `nyc`, qui contient la table `taxis_one_step`. 
On a donc deux tables qui contienent les mêmes données : une table au format Parquet, l’autre au format Iceberg.

L'ensemble du code utilisé est disponible [ici](https://github.com/Parreirac/yellow_taxi_iceberg).

## Premiers enseignements


Hélas, les choses ne sont pas aussi simples, les schémas évoluent avec le temps, et cela pose problème. Green Taxis n'échappe pas à cette règle : les types des colonnes changent. Dans notre cas aucun probleme de dépassement de capacité, mais pour Spark, c’est non. Pour valider le code développé, une solution est de restreindre le jeu de données sur une periode plus courte, sans changement de schéma, par exemple d’octobre 2018 à décembre 2021. Ce faisant le jeu de données ne fait qu'environ 600 Mo et on constate dans ce cas les performances entre Parquet et Iceberg sont proches.

> Enseignement 1 : sur un petit jeu de données la différence de performance est négligeable.

Il convient de tester avec plus de données. Tentons de mettre en oeuvre les différentes fonctionnalités offertes par Iceberg. Il s'agit de pouvoir charger dans une table des données avec des  « irrégularités », nous pourrons alors extraire de cette table des fichiers par mois, sur un schema unique, qui nous serviront à produire un comparatif Iceberg / Parquet. Les détails sont données [ici](https://github.com/Parreirac/yellow_taxi_iceberg/blob/main/readme_fr.md). Nous indiquerons ici les principaux enseignements tirés de cet exercice.

Le nettoyage des schémas fonctionne avec Iceberg, d'une facon plus simple qu'avec Spark seul. En effet, nous aurrions dû tout charger en type String, puis convertir spécifiquement dans le type voulu. Pour Green Taxi, une simple boucle sur les fichiers suffit.

> Enseignement 2 : Iceberg gère mieux les changements de types.

Notons que cette astuce ne fonctionne que parce que nous n'avons pas de champ sous forme de String. Dans ce cas, un cast devrait être spécifiquement codé.

Autant le dire tout de suite, avec l’ensemble du jeu de données on a un gain de performance, qui peut être important suivant la requête. Nous avons donc une nouvelle question, comment évolue ce gain avec un jeu de données encore plus grand ? Passons aux taxis jaunes qui commence en 2009 ! On passe ainsi de 1.2 Go à 27.9 Go.

Attention, sur ce jeu de données plus ancien, on a de nouveaux problèmes :
* les colonnes changent de nom avec typiquement des majuscules ou des abréviations, qui vont et viennent ;
* une colonne éphémère nommée « __index_level_0__00 », qui sera simplement supprimée ;
* deux fois deux colonnes qui fusionnent, occasionnant un changement profond dans l’ordre des colonnes. 

Il y a 11 schémas différents, mais si l’on fait abstraction des erreurs de nommage et de types, on n’a que deux schémas : avant et apres janvier 2011. En effet, afin d’anonymiser les données, les positions ne sont plus données en latitude et longitude, mais _via_ un identifiant de zone. La carte est ainsi subdivisée en 263 zones. 

Les problèmes de types sont gérés par Iceberg. Pour corriger les problemes de nommage, on renomme les colonnes suivant les deux cas suivant : 
- le fichier à 18 colonnes ou le nom de la derniere colonne commence par « _ » ; 
- les autres cas.

Nous devons maintenant traiter le problème des coordonnées. La difficulté ici est le temps de calcul pour déterminer si un point est dans un polygone car cette opération est chronophage.
On peut créer une table contenant, suivant un pas fixé toutes les coordonnées possibles et la zone correspondante. Indiquons qu'à New York, un deplacement de 0.001 degré est environ 10 mètres. La creation de cette table de conversion nécessite 10 minutes de calcul, sur un bon ordinateur portable. Pour la conversion, on n'a plus qu'à réaliser une simple jointure.

Les deux tables sont maintenant identiques, à l’ordre près. Bonne nouvelle, ceci ne pose aucun problème à Iceberg !

> Enseignement 3 : gère mieux les colonnes des tables

Export des fichiers : 

Notons encore un élément, sur l'occupation du disque dur :
* sur les fichiers de départ :
  * l'ensemble des fichiers parquet fait au départ 27,9 Go ;
  * la table Iceberg fait seulement 18,6 Go ;
* sur les fichiers corrigés :
  * l'ensemble des fichiers modifié fait 26,3 Go ;
  * la table Iceberg corrigée fait 18,9 Go.
La transformation a peu réduit la taille des fichiers, probablement parce que nous ne paramétrons pas la production des fichiers parquet. En revanche la table au format parquet est toujours plus petite. Le repertoire data n'est pas une simple copie des données chargée. En dépit des snap.....

> Enseignement 4 : Iceberg optimise automatiquement la taille des fichers parquets.


20 h de calculs sur ma machine.

traiter tous les fichiers de décembre :
20 min pour les transfo
80 min pour la production des fichiers.
