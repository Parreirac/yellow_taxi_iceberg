Apache Iceberg est un format de table qui a le vent en poupe, mais pourquoi un tel engouement ? Dans cette série d’articles nous montrerons l’intérêt général de l’outil, puis comment l'utiliser sur un jeu de données réel sur un cluster Hadoop. Ceci nous permettra de tirer des premiers enseignements, puis nous montrerons que les promesses sont tenues sur les performances.

Cette série se compose de trois articles :

* Iceberg 1 : pourquoi encore un nouveau format de table ? 
* Iceberg 2 : New york taxis sur TDP, mise en place et premiers enseignements ;
* Iceberg 3 : mesures de performances.

# Pourquoi encore un format de table ?

## L’origine du besoin : se simplifier la vie

Pourquoi introduire un nouveau format de table, pourriez-vous demander ? Après tout, nous sommes familiers avec des formats tels que ORC, Parquet, Avro. La réponse est qu'Iceberg n'est pas un nouveau format de fichier mais un format de table. C'est une [spécification](https://iceberg.apache.org/spec/) robuste et performante de la manière d'organiser les données et informations d'une table dans un contexte de big data, c'est-à-dire sur de **multiples** fichiers.  D’ailleurs Iceberg utilise les formats de fichier standards. Ainsi une table Iceberg est stockée sous la forme d'une arborescence de fichiers, comprenant :

* un répertoire « data » qui contient les données, au format Parquet par défaut,
* un répertoire « metadata » avec des fichiers JSON et Avro, entre autres.

Mais ne pouvait-on pas déjà avoir des tables sur plusieurs fichier ? Oui, il est possible de charger une table composée de plusieurs fichiers, par exemple avec un outil comme Spark. Cependant, cela peut entraîner des désagréments majeurs lorsque les fichiers n'ont pas le même schéma. Les résultats peuvent varier en fonction de l'ordre de lecture des fichiers, voire l'exécution d'une requête peut échouer. Ce n'est alors pas la lecture des fichiers qui est en cause. Par conséquent, si une colonne change de nom ou de type, et c’est fréquent dans les données réels, le traitement va s’interrompre. Bien que Spark propose de fonctionnalités pour y remédier cela représente des désagréments pour tous. Avec Iceberg, ces problèmes d'évolution de schéma sont largement atténués.

Mais Iceberg fait bien plus, car il est d’abord conçu pour simplifier la vie des ingénieurs en données. Ainsi un autre défi concerne la gestion des partitions. Il est nécessaire de partitionner les données en fonction des requêtes les plus fréquentes afin d'optimiser les traitements. Cependant, les requêtes évoluent, ce qui signifie que les partitions devraient être revues régulièrement. Malheureusement, les ingénieurs en données rechignent souvent à entreprendre cette tâche, car elle est longue et peu gratifiante. De plus, pendant la période de transition les données peuvent devenir inaccessibles. 

Si vous choisissez, par exemple, de partitionner vos données en sur l'année pour une table avec des dates, les systèmes classiques crééent en interne une colonne année. L'utilisateur devra explicitement faire référence à cette colonne virtuelle dans sa requête, sinon l'ensemble de la table est parcouru. 
Avec Iceberg, cette étape n'est plus nécessaire, car Iceberg conserve le lien entre les colonnes et les partitions. En d'autres termes, Iceberg permet de masquer le partitionnement aux utilisateurs, ce que l'on appelle « hidden partitioning ». Le partitionnement devient une fonctionnalité de gestion des données, transparente pour les utilisateurs. De plus, un changement de partition n'entraîne aucune interruption de service, car il n'affecte que les nouvelles données insérées dans la table.

Au-dela, Iceberg se veut également être un format moderne, il est conçu de manière entièrement indépendante du système de stockage, qu'il s'agisse de HDFS ou d'un système de stockage objet. Il est également indépendant du moteur de calcul. Ainsi, Iceberg propose une [extension SQL](https://www.dremio.com/resources/webinars/deep-dive-into-iceberg-sql-extensions/), cette approche permet de détacher l’administration des tables, par exemple son historique, de la plateforme mise en œuvre. Là encore, cela simplifie considérablement la vie des ingénieurs en données. 


## Comment est-ce possible ?

Pour tenir ses promesses, Iceberg fait deux choses : revoir en profondeur l’implémentation d’une table et utiliser intensivement les métadonnées.

Pour les métadonnées c’est un mécanisme qui existe déjà, par exemple dans Parquet. Des outils tels que parquet-tools permettent de visualiser pour chaque colonne et par blocs de lignes : la valeur minimale, la valeur maximale et le nombre de valeurs nulles. Si une requête recherche une valeur spécifique qui n’est pas comprise entre ces bornes, il n’est pas nécessaire de lire les données. Il s’agit donc d'élaguer les informations à lire. Iceberg va plus loin en utilisant plus de métadonnées, incluant le nombre de valeurs distinctes et également un [filtre de Bloom](https://fr.wikipedia.org/wiki/Filtre_de_Bloom). Il s’agit de calculer une petite table de hachage des valeurs présentes. Si on recherche une valeur particulière il y a alors deux cas :

* son hachage n’est pas stocké, alors on est certain que cette valeur n’est pas présente ; 
* son hachage est stocké, mais en raison des possibles collisions il n’est pas certain que la valeur soit présente, on doit lire le bloc.

Ceci permet d’augmenter l’élagage et donc les performances. 

En outre, comme dans Iceberg ces informations sont stockées séparément, il n’est donc pas nécessaire d’accéder au répertoire data pour commencer un travail sur une table.

La refonte de l’organisation des informations est plus complexe. Les formats de table comme Delta Table de Databricks ou Apache Hudi, s'appuient sur un mécanisme proposé par Apache Hive en 2009. Les données sont organisées sous forme d'une arborescente de fichiers, avec des subdivisions en fonction des partitions de la table. En parallèle, des fichiers (_in fine_ une base de données) tracent les mises à jour de la table : les données nouvelles et les données à supprimer. Ceci permet de garantir les propriétés ACID dans un environnement de data warehouse. Cependant, si les partitions sont nombreuses cette solution devient un point de congestion de la plateforme. Par exemple, une transaction pourra nécessiter de lire de trop nombreux fichiers, ce qui est lent. De même une mise à jour sur plusieurs partition entrainera la mise en place de nombreux verrous bloquants les accès concurrents.

Avec Iceberg, chaque transaction génère un ensemble de fichiers appelé « snapshot » qui permet de connaître l’état courant de la table. Une transaction « n'écrase » donc pas des valeurs antérieures. Ceci permet : 

* l'isolation des transactions sans nécessiter la mise en place de verrous ; 
* de revenir à un état anterieur de la table, simplement en changeant l'index du snapshot courant, c'est le « time travel / roll back ».

Les informations d’un snapshot se repartissent sur trois niveaux :

* dans le repertoire data, les fichiers de données ;
* dans le repertoire metadata :
  * des manifest files, des métadonnées associées à un lot de fichiers de données ; 
  * un manifest list, des métadonnées spécifiques au snapshots et liées à un lot de manifest files.

Cela permet de pouvoir réutiliser des fichiers sur plusieurs snapshots, car un manifest files peut être mutualisé sur plusieurs manifest list. Ceci accélère les traitements et de réduit le volume de données. Ajoutons que chaque snapshot contient le schéma associé à sa transaction. Chaque insertion existe avec le « même poids », plus besoin de se référer au schéma initial de la table, le schéma peut évoluer librement. Cette capacité permet à Iceberg de construire des tables sur un datalake, dans lequel les fichiers sont stockés dans leur format d’origine.

Cependant, si un grand nombre de snapshots s'accumulent, l'exécution des requêtes peut ralentir. Pour faire face à cette situation, Iceberg propose un mécanisme de péremption qui consiste à supprimer les snapshots trop anciens (par défaut, ceux datant de plus de 5 jours). Il convient de noter que cette opération peut être lente, car elle peut nécessiter la réécriture potentielle de l'ensemble des fichiers de données.

Pour réaliser tout cela, et bien plus encore puisque la communauté Iceberg est très active, les développeurs ont dû produire beaucoup de travail, d'où le choix du nom « Iceberg ». Pour « quelques » fonctionnalités visibles au-dessus de la surface, un grand nombre de fonctionnalités restent cachées sous la surface.

## Vous en voulez encore ?

Iceberg a vu le jour chez Netflix en 2017, puis est devenu un projet de la Fondation Apache en 2018. Chez Netflix, les journaux des serveurs étaient d'une telle ampleur que la planification de requêtes prenait plusieurs minutes et leur exécution des heures, limitant ainsi les requêtes à une semaine de données au maximum. Grâce à Iceberg, les requêtes sont aujourd'hui plus rapides et peuvent porter sur une plage temporelle s'étendant jusqu'à un mois. La documentation Iceberg mentionne un gain performance pouvant aller jusqu'à un [facteur 10]( https://conferences.oreilly.com/strata/strata-ny-2018/cdn.oreillystatic.com/en/assets/1/event/278/Introducing%20Iceberg_%20Tables%20designed%20for%20object%20stores%20Presentation.pdf). 

Cependant les besoins de Netflix ne sont pas nécessairement représentatifs de ceux de tout un chacun, ce qui justifie la réalisation de mesures sur des jeux de données standards. Mais même en l'absence de résultats chiffrés, il convient de noter que de nombreux fournisseurs de plateformes de données ont annoncé récemment qu'ils prennent désormais en charge Iceberg. Cela suggère que les avantages sont bien réels. Au-delà des quelques fonctionnalités présentées ici, ajoutons qu'Iceberg dispose d'une communauté dynamique proposant :

* de nombreux [blogs de qualité](https://iceberg.apache.org/blogs/) ;
* des outils open source, citons par exemple [pyIcerberg](https://py.iceberg.apache.org/) qui permet de manipuler vos tables en ligne de commande à l’instar d’un parquet-tools ;
* et même pour les clients exigents, il existe du [support commercial](https://iceberg.apache.org/vendors/). 

En conclusion, il convient de souligner qu'Iceberg ne prétend pas résoudre tous les problèmes. Si vous avez besoin de mettre fréquemment à jour les données d'une table plutôt que de les ajouter, Apache Hudi peut être un choix plus adapté. Cependant, pour la plupart des cas d'utilisation, même au sein d'un environnement de data warehouse, l'adoption de ce format est bénéfique.


