# Mesure et comparaison des performances

Les objectifs :

* mesurer un écart de performance entre Iceberg et Parquet sous Spark en fonction de la nature de la requete. 
* mesurer l'impact de la taille table, puis du nombre de snapshots.

Choix du dataset New York Yellow Taxis, plus axé OLAP qui est notre cible, alors que www.tpc.org est plus sur un axe OLTP. En outre ce choix permet de mettre en oeuvre les fonctionnalités offertes par Iceberg.

## Methode 

* liste de requetes par nature, simple se trouve sur Internet.
* chronometrer les requetes, N fois. On fait un calculde moyenne et d'écart type. Nous servira pour comparer les résultats
* jeter la première requête qui est bien plus lente que les autres (STD élevée). A priori, Spark doit charger des librairies.

Sur Green Taxi (1.2) :
		sans snap	avec
GB count	moyenne	1,512452171	1,116800573
	ecarttype	0,11290709	0,110071188
GB avg	moyenne	1,430729751	1,092951456
	ecarttype	0,294611627	0,205862758
where	moyenne	2,562078511	1,344238166
	ecarttype	0,854488229	0,10515185
			
GB count ou avg, proche.		1.47 +/- .20	
where		2.5	

Sur mon Yellow taxis, premier tests avec 15 requetes. 5 split 5 mesures par requets.
J'élimine les requetes "equivalentes", je garde donc la requete type et la meilleure.

Je passe à une relance avec plus de split, géométrique. Pour observer l'inflexion.

Avec ce jeu de données, je peux observer que Q4 Q5 est similaire en perf à Q2. Q3 est en best
Q8 est médian Q7 est best
9 est médian Q11 est best

## Outils complementaires 

En amont : la planification des requetes
En aval : l'historique des requetes, 

## Précaution à prendre

Sur un ordinateur on peut controler qui accède au cluster. (Pas de concurence).
Mais le cluster demeure sous dimensionné par rapport à nos tests.

Gros, mais pas trop.
Dans tous les cas si STD est trop grand, c'est qu'il y a un pb (et en général les log sont alors clair sur le manque de ressource).


hdfs://mycluster/spark3-logs coté serveur d'historique

```bash
vagrant ssh edge-01
sudo su
mkdir /opt/tdp/spark3/logs
sudo chmod o+w /opt/tdp/spark3/logs/
```

## Résultats

### Perf Parquet

### Perf Iceberg

### Comparaison 

### Impact des snapshots

## Analyse

## Conclusion

On n'a pas utiliser de partitions, nos tests sont sur le cas "basique". 
De nombreux travaux existent dans l'open source conduisant à la mise en place d'autres mécanismes.

From  Introduction to Apache Doris(incubating)
•	Support sorted compound key index: Up to three columns can be specified to form a compound sort key. With this index, data can be effectively pruned to better support high concurrent reporting scenarios.
•	Z-order index ：Using Z-order indexing, you can efficiently run range queries on any combination of fields in your schema.
•	MIN/MAX indexing: Effective filtering of equivalence and range queries for numeric types
•	Bloom Filter: very effective for equivalence filtering and pruning of high cardinality columns
•	Invert Index: It enables the fast search of any field
