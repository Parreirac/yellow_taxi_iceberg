Apache Iceberg est un format de table qui a le vent en poupe, mais pourquoi un tel engouement ? Dans cette série d’articles nous montrerons l’intérêt général de l’outil, puis comment l'utiliser sur un jeu de données réel sur un cluster Hadoop. Ceci nous permettra de tirer des premiers enseignements, puis nous montrerons que les promesses sont tenues sur les performances.

Cette série se compose de trois articles :

* Iceberg 1 : pourquoi encore un nouveau format de table ? 
* Iceberg 2 : New York taxis sur TDP, mise en place et premiers enseignements ;
* Iceberg 3 : mesures de performances.

# Pourquoi encore un format de table ?

## Ce n'est pas un nouveau format de fichier

Pourquoi introduire un nouveau format de table, pourriez-vous demander ? Après tout, nous sommes familiers avec des formats tels que ORC, Parquet, Avro. La réponse est qu'Iceberg n'est pas un nouveau format de fichier mais un format de table. C'est une [spécification](https://iceberg.apache.org/spec/) robuste et performante de la manière d'organiser les données et informations d'une table dans un contexte de big data, c'est-à-dire sur de **multiples** fichiers.  D’ailleurs Iceberg utilise les formats de fichier standards. Ainsi une table Iceberg est stockée sous la forme d'une arborescence de fichiers, comprenant :

* un répertoire « data » qui contient les données, au format Parquet par défaut,
* un répertoire « metadata » avec des fichiers JSON et Avro, entre autres.

Mais ne pouvait-on pas déjà avoir des tables sur plusieurs fichier ? Oui, il est possible de charger une table composée de plusieurs fichiers, par exemple avec un outil comme Spark. Mais Iceberg permet des traitements moins sensibles aux aléas pratiques, et souvent plus rapides. 

Pour réaliser tout cela, et bien plus encore puisque la communauté Iceberg est très active, les développeurs ont dû produire beaucoup de travail, d'où le choix du nom « Iceberg ». Pour « quelques » fonctionnalités visibles au-dessus de la surface, de nombreuses autres restent cachées sous la surface.

## L’origine du besoin : se simplifier la vie

Lorsqu'on exploite une table sous forme d'un ensemble de fichiers, cela peut occasionner d'importants problèmes lorsque ces fichiers ne suivent pas rigoureusement le même schéma. Cela peut conduire à des résultats variables en fonction de l'ordre dans lequel les fichiers sont lus, voire à l'échec de l'exécution d'une requête. Ce n'est alors pas la lecture des fichiers qui est en cause. Par conséquent, si une colonne change de nom ou de type, et c’est fréquent dans les données réels, le traitement va s’interrompre. Bien que Spark propose de fonctionnalités pour y remédier cela représente des désagréments pour tous. 

Grâce à Iceberg, ces défis liés à l'évolution de schémas disparaissent. Pour les types, la seule exception nécessitant une intervention manuelle concerne le cas où un type est modifié pour devenir une chaîne de caractères (string). Tous les autres cas sont gérés de manière transparente. Pour les noms, les colonnes peuvent se gérer par leur index masquant au besoin les changements.  

Mais Iceberg fait bien plus, car il est d’abord conçu pour simplifier la vie des ingénieurs en données. Ainsi un autre défi concerne la gestion des partitions. Il est nécessaire de partitionner les données en fonction des requêtes les plus fréquentes afin d'optimiser les traitements. Cependant, les requêtes évoluent, ce qui signifie que les partitions devraient être revues régulièrement. Malheureusement, les ingénieurs en données rechignent souvent à entreprendre cette tâche, car elle est longue et peu gratifiante. De plus, pendant la période de transition les données peuvent devenir inaccessibles. 

Si vous choisissez, par exemple, de partitionner vos données sur l'année pour une table avec des dates, les systèmes classiques créent en interne une colonne année. L'utilisateur devra explicitement faire référence à cette colonne virtuelle dans sa requête, sinon l'ensemble de la table est parcouru. 
Avec Iceberg, cette étape n'est plus nécessaire, car Iceberg conserve le lien entre les colonnes et les partitions. En d'autres termes, Iceberg permet de masquer le partitionnement aux utilisateurs, ce que l'on appelle « hidden partitioning ». Le partitionnement devient une fonctionnalité d'administration de la plateforme transparente pour les utilisateurs. De plus, toute modification apportée à une partition n'occasionne aucune interruption de service, car elle n'affecte que les futures insertions dans la table.

De plus, Iceberg vise à être un format moderne, totalement indépendant du système de stockage, que ce soit HDFS ou un système de stockage objet dans le cloud. Il est également détaché du moteur de traitement des données. Par conséquent, Iceberg offre une [extension SQL](https://www.dremio.com/resources/webinars/deep-dive-into-iceberg-sql-extensions/) qui permet de séparer la gestion des tables, y compris leur historique, de la plateforme sous-jacente. Cette approche simplifie grandement la tâche des utilisateurs car il n'y a plus à connaitre une syntaxe spécifique pour Hive et une autre pour Spark par exemple. Ce sera toujours la même.

## Comment est-ce possible ?

Pour tenir ses promesses, Iceberg fait deux choses : utiliser intensivement les métadonnées et revoir en profondeur l’implémentation d’une table.

### Plus de métadonnées

En ce qui concerne les métadonnées, ce mécanisme est déjà en place, comme c'est le cas avec Parquet par exemple. Il existe des outils permettant de visualiser, pour chaque colonne et par blocs de lignes, des informations telles que la valeur minimale, la valeur maximale et le nombre de valeurs nulles contenu dans le fichier. Lors de l'utilisation, si une requête cherche une valeur spécifique qui ne se situe pas entre ces bornes, il n'est pas nécessaire de lire l'ensemble des données. Ainsi, il est possible d'optimiser la sélection des informations à lire. C'est l'élagage ou « pruning ».

Iceberg va encore plus loin en utilisant davantage de métadonnées, notamment le nombre de valeurs distinctes, ainsi qu'un [filtre de Bloom](https://fr.wikipedia.org/wiki/Filtre_de_Bloom). Ce filtre probabiliste consiste à créer une petite table de hachage des valeurs présentes. Lorsque vous recherchez une valeur spécifique, deux cas se présentent :

* son hachage n'est pas stocké, vous pouvez être certain que cette valeur n'est pas présente ;
* son hachage est stocké, en raison de possibles collisions, il n'est pas garanti que la valeur soit présente, et vous devrez lire le bloc correspondant.

Cela permet d'améliorer considérablement l'élagage des données, ce qui se traduit par des performances accrues.

En outre, comme dans Iceberg ces informations sont stockées séparément, il n’est donc pas nécessaire d’accéder au répertoire data pour commencer un travail sur une table. Certaines requêtes simple peuvent être traitées sans aucun accès au répertoire des données, seulement par un accès au répertoire des métadonnées. 

### Une nouvelle organisation des informations

La refonte de l’organisation des informations est plus complexe. Les formats de table comme Delta Table de Databricks ou Apache Hudi, s'appuient sur un mécanisme proposé par Apache Hive en 2009. Les données sont organisées sous forme d'une arborescente de fichiers, avec des subdivisions en fonction des partitions de la table. En parallèle, des fichiers, _in fine_ une base de données, tracent les mises à jour de la table : les données nouvelles et les données à supprimer. Ceci permet de garantir les propriétés ACID dans un environnement de data warehouse. Cependant, si les partitions sont nombreuses cette solution devient un point de congestion de la plateforme. Par exemple, une transaction pourra nécessiter de lire de trop nombreux fichiers, ce qui est lent. De même une mise à jour sur plusieurs partitions entrainera la mise en place de nombreux verrous, bloquants ainsi les accès concurrents.

Avec Iceberg, chaque transaction génère un ensemble de fichiers appelé « snapshot » qui permet de connaître l’état courant de la table. Une transaction « n'écrase » donc pas des valeurs antérieures. Ceci permet : 

* l'isolation des transactions sans nécessiter la mise en place de verrous ; 
* de revenir à un état anterieur de la table, simplement en changeant l'index du snapshot courant, c'est le « time travel / roll back ».

Les informations d’un snapshot se repartissent sur trois niveaux :

* dans le repertoire data, les fichiers de données ;
* dans le repertoire metadata :
  * des manifest files, des métadonnées associées à un lot de fichiers de données ; 
  * un manifest list, des métadonnées spécifiques au snapshots et une référence vers les manifests files utilisés.

Cette architecture permet la réutilisation de fichiers entre plusieurs snapshots, ce qui accélère les traitements en lecture et en écriture, et diminue le volume de données.

En outre, chaque snapshot contient le schéma associé à sa transaction. Ce qui signifie que chaque insertion a le même poids, sans nécessiter de référence au schéma initial de la table. Cette capacité permet de construire des tables sur un datalake, car tous les fichiers peuvent rester dans leur format d'origine.

Cependant, en cas d'accumulation d'un grand nombre de snapshots, plusieurs problèmes peuvent survenir. Tout d'abord, l'exécution des requêtes peut ralentir, car les informations se fragmentent sur un grand nombre de manifests lists. Il peut également y avoir un impact sur l'espace disque. Il est d'ailleurs à noter qu'avec Iceberg `TRUNCATE` vide une table mais sans libérer d'espace disque, car un snapshot sans donnée est créé, mais les snapshots antérieurs sont toujours disponibles.

Face à ces défis, Iceberg propose diverses fonctionnalités, la solution la plus simple consiste en un mécanisme de péremption qui invalide automatiquement les snapshots trop anciens (par défaut, ceux datant de plus de cinq jours). Mais cette opération peut prendre du temps car elle peut nécessiter la réécriture de nombreux fichiers.
Ainsi, Iceberg n'a pas pour vocation de se substituer au système de sauvegarde de votre plateforme. Au lieu de cela, il offre une flexibilité accrue dans la gestion des données sans imposer de charge de travail aux administrateurs.

## Pas encore convaincu ? 

Iceberg a vu le jour chez Netflix en 2017, puis est devenu un projet de la Fondation Apache en 2018. Chez Netflix, les journaux des serveurs étaient d'une telle ampleur que la planification de requêtes prenait plusieurs minutes et leur exécution des heures, limitant ainsi les requêtes à une semaine de données au maximum. Grâce à Iceberg, les requêtes sont aujourd'hui plus rapides et peuvent porter sur une plage temporelle s'étendant jusqu'à un mois. La documentation Iceberg rapporte que Netflix a observé un gain de performance pouvant aller jusqu'à un [facteur 10]( https://conferences.oreilly.com/strata/strata-ny-2018/cdn.oreillystatic.com/en/assets/1/event/278/Introducing%20Iceberg_%20Tables%20designed%20for%20object%20stores%20Presentation.pdf). 

Cependant les besoins de Netflix ne sont pas nécessairement représentatifs de ceux de tout un chacun, ce qui justifie la réalisation de mesures sur des jeux de données standards. Mais même en l'absence de résultats chiffrés, il convient de noter que de nombreux fournisseurs de plateformes de données ont annoncé récemment qu'ils prennent désormais en charge Iceberg. Cela suggère que les avantages sont bien réels. Au-delà des quelques fonctionnalités présentées ici, ajoutons qu'Iceberg dispose d'une communauté dynamique proposant :

* de nombreux [blogs de qualité](https://iceberg.apache.org/blogs/) ;
* des outils open source, citons par exemple [pyIcerberg](https://py.iceberg.apache.org/) qui permet de manipuler vos tables en ligne de commande à l’instar d’un parquet-tools ;
* et même pour les clients exigents, il existe du [support commercial](https://iceberg.apache.org/vendors/). 

En conclusion, il est important de noter qu'Iceberg ne prétend pas être la solution à tous les problèmes. Si vous avez besoin de mettre à jour fréquemment les données d'une table plutôt que de simplement les ajouter, vous pourriez trouver qu'Apache Hudi est une option plus adaptée. De plus, dans le domaine des solutions payantes, Databricks propose un outil performant, bien plus que sa version gratuite qui ne propose pas l'ensemble de ces fonctionnalités. Cependant, pour la plupart des cas d'utilisation, même au sein d'un environnement de data warehouse, l'adoption du format Iceberg s'avère avantageuse.

