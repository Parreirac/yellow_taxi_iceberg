# trouver un nom

Green Taxis est un jeu de données réel sujet à des aléas fréquents dans la vie du data engenieurs : les types des colonnes changent. Dans notre cas aucun probleme de dépassement de capacité, mais pour Spark, c’est non. Pour valider le code développé, une solution est de restreindre le jeu de données sur une periode plus courte, sans changement de schéma, par exemple d’octobre 2018 à décembre 2021. Ce faisant le jeu de données ne fait qu'environ 600 Mo. Dans ce cas les performances entre Parquet et Iceberg sont proches.

> Enseignement 1 : sur un petit jeu de données la différence de performance est négligeable.

Il convient de tester avec plus de données. Tentons de mettre en œuvre Iceberg et sa gestion des schémas : chargons l’intégralité du jeu de données dans une table, de façon incrémentale avec un snapshot par mois. Puis on peut en appliquant un filtre sur le mois et l’année regénerer les fichier parquet, avec un unique schéma. Ceci nous permettra de pouvoir produire un comparatif des performances.

La difficulté ici est, en python, de parcourir un répertoire d’un cluster Hadoop. On peut trouver differentes solutions par exemple [ici](https://www.perceptivebits.com/a-comprehensive-guide-to-finding-files-via-spark/). Cette solution n’est pas exactement dans les canons de la beauté. PySpark demeure une interface vers Spark qui utilise scala, dans notre code nous allons appeler directement la JVM, qui est accessible via la variable sparkcontexte : 

```python
URI           = sc._gateway.jvm.java.net.URI
Path          = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem    = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration
       
fs = FileSystem.get(URI("hdfs://mycluster:8020"), Configuration())
status = fs.globStatus(Path(parquet_path))
# Parcours des fichiers et traitement

for fileStatus in status:
    path = str(fileStatus.getPath().toUri().getRawPath())
    trips_df1  = spark.read.format("parquet").load(path)
```

Mais ce qui compte ici, c’est que pour Iceberg, les différences de types des colonnes ne posent pas de problème, cela fonctionne !

> Enseignement 2 : Iceberg gère mieux les changements de types.

Notons deux points :

* cette astuce fonctionne car nous n'avons pas de champ sous forme de String. Dans ce cas, un cast devrait être spécifiquement codé ;
* cette opération nous fait passer de 114 à 372 fichiers. Il s’avère qu’il y a des erreurs sur les dates, certaines sont d’avant 2013 d’autres sont d’après 2030. Nous pourrions supprimer ces données, ou les réatribuer au mois et à l’année du fichier source, nous n'en ferrons rien car c’est les performances qui nous interessent et non les valeurs des calculs.

Autant le dire tout de suite, avec l’ensemble du jeu de données on a un gain de performance, qui peut être important suivant la requête. Nous avons donc une nouvelle question, comment évolue ce gain avec un jeu de données encore plus grand ? Passons aux taxis jaunes qui commence en 2009 !

Par rapport au tutoriel alliage, le script est modifié pour permettre le chargement de tous les dataset disponibles. Pour se faire, on utilise l’option dataset (par defaut à green). Ainsi avec l’option `--dataset yellow`, 27,9 Go de données sont téléchargées sur votre cluster.

# Les problèmes continuent 

Attention, sur ce jeu de données plus ancien, on a de nouveaux problèmes :

* les colonnes changent de nom avec typiquement des majuscules ou des abréviations, qui vont et viennent ;
* une colonne éphémère nommée « __index_level_0__00 » ;
* deux fois deux colonnes qui fusionnent, occasionnant un changement profond dans l’ordre des colonnes. 

Il y a 11 schémas différents, mais si l’on fait abstraction des erreurs de nommage et de types, on n’a que deux schémas : avant et apres juillet 2016. En effet, à cette date, afin d’anonymiser les données, les positions ne sont plus données en latitude et longitude, mais _via_ un identifiant de zone. La carte est ainsi subdivisée en 263 zones.

Les problèmes de types sont gérés par Iceberg. Pour corriger les problemes de nommage, on renomme les colonnes suivant les deux cas suivant : 
- le fichier à 18 colonnes ou le nom de la derniere colonne commence par « _ » ; 
- les autres cas.

Nous devons maintenant traiter le problème des coordonnées. La difficulté ici est le temps de calcul pour déterminer si un point est dans un polygone car cette opération est chronophage.
On peut créer une table contenant, suivant un pas fixé toutes les coordonnées possibles et la zone correspondante. Indiquons qu'à New York, un deplacement de 0.001 degré est environ 10 mètres.

Pour se faire on peut utiliser [GeoPandas](https://geopandas.org/en/stable/#), qui gère de facon transparente les coordonnées dans [le fichier shapefile fourni](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip) avec le jeu de données (qui n'est pas en latitude/longitude). 
On va devoir faire de nombreuse fois le test « point dans un polygone », qui est un calcul lent. Une solution ici est d’utiliser les fonctionnalités offertes par géoPandas, ici la [triangulation de Delaunay](https://fr.wikipedia.org/wiki/Triangulation_de_Delaunay). Chaque zone est découpée en triangle on n’a plus qu’a transformer les triangles en pixels (raster), voir par exemple [ici](http://www.sunshine2k.de/coding/java/TriangleRasterization/TriangleRasterization.html).

La creation de cette table de conversion nécessite 10 minutes de calcul, sur un bon ordinateur portable. Pour la conversion, on n'a plus qu'à réaliser une simple jointure :

```python
df1 = df.withColumn("latitude", round(df["Start_lat"] * 1000))\
        .withColumn("longitude", round(df["Start_lon"] * 1000))

df2 =  df1.join(locId_df,on = ["latitude","longitude"],how='inner')\
          .drop(*("Start_lon","Start_lat","longitude","latitude"))\
          .withColumnRenamed("LocationID", "PULocationID")
```
Notons que pour passer de l'ancien au nouveau format, la colonne « extra » est renommée en « surcharge », qui est en fait un « surcharge and extra ».

Les deux tables sont maintenant identiques, à l’ordre près. Bonne nouvelle, ceci ne pose aucun problème à Iceberg !

> Enseignement 3 : gère mieux les colonnes des tables 

# Des problèmes pour finir

A ce stade, nous avons presque terminé notre conversion des fichiers sur un schéma unique, mais un nouveau problème apparait : les incohérences dans les dictionnaires.

## Store_and_fwd_flag

`Store_and_forward_flag` est un paramètre technique qui indique si le taxi disposait d'une connexion au serveur durant la course. Les valeurs sont oui (Y) ou non (N). Or les valeurs réelles sont : null, Y, N, 0, 1, 1.0 0.0 et nan.
Ici la correspondance est simple.

## Vendor_ID

Le fournisseur du jeu de données indique que `Vendor_ID` représente le fournisseur de l'enregistrement :

1. Creative Mobile Technologies, LLC
2. VeriFone

Or, sur les fichiers récents, les valeurs vont jusqu'à 6. Il y a un défaut de documentation, c'est un problème courant mais ce n'est pas rédibitoire.

Sur les fichiers plus ancien, par exemple avec le site des [données ouvertes de la ville de New York](
https://data.cityofnewyork.us/Transportation/2009-Yellow-Taxi-Trip-Data/6phq-6kwz
), on a pour 2009 :

| Vendor_Name | Count (Millions) |
| :------:    | :-----------:    |
| CMT         | 76.8             |
| VTS         | 83.9             |
| DDS         | 10.1             |

Une recherche Internet permet de trouver la signification de VTS :  VeriFone Transportation Systems (VTS), c'est bien la valeur 2. DDS est donc la valeur 3.

## RateCodeID

`RateCodeID` est un code de tarification allant de 1 à 6.
Dans les faits, il y a plus d'une vingtaine de valeurs distinctes. On ne peut rien faire, mais  cela ne pose pas de problème pour la suite.

## Payment_Type

Comme son nom l'indique Payment_Type est un dictionnaire correspondant au mode de paiement :
1. Credit card
2. Cash
3. No charge
4. Dispute
5. Unknown
6. Voided trip

Or, les valeurs trouvées sont des entiers de 0 à 5, Cre CRE CRD Credit CREDIT, CAS  Cas CSH CASH Cash, Dis DIS Dispute, No NOC No Charge NA

Il semble que, O -> unk. Pas de 6. 


si on regarde 2009 on a 2/3 cas 1/ credit card

0 à 5, Cre CRE CRD Credit CREDIT CAS  Cas CSH CASH Cash Dis DIS Dispute No NOC No Charge NA

