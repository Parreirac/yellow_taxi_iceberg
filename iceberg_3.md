# Mesure et comparaison des performances

objectif :
mesurer un écart de performance, en fonction de la nature de la requete.
Impact de la taille table et du nombre de snapshots.


## Methode 

* liste de requetes par nature
* chronometrer des requetes
* jeter la première (chargement de librairie)
* faire n fois chaque mesure et faire la moyenne.



## outils complementaires 

la planification des requetes
l'historique des requetes
hdfs://mycluster/spark3-logs coté serveur d'historique

```bash
vagrant ssh edge01
sudo su
mkdir /opt/tdp/spark3/logs
sudo chmod o+w /opt/tdp/spark3/logs/
```
## Résultats


