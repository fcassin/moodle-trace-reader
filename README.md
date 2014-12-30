moodle-trace-reader
===================

Ce projet est un extracateur de logs moodle sous forme de trace, inspriré par le format de stockage décrit par la norme ktbs et utilisé par le projet ktbs4js.

Les données de logs sont extraites depuis leur base de données MySQL, que ce soit en local (via un dump du serveur de production), ou directement depuis un serveur moodle.

Les informations de clefs étrangères contenues dans la colonne info de la table de log sont exploitées pour obtenir plus d'informations sur les traces étudiées, notamment du point de vue de l'utilisation des forums.

Toutes les informations relatives à l'utilisateur à l'origine du log généré, ainsi qu'au cours concerné et à sa hiérarchie, sont également extraites à la volée.


Les données sont transformées vers le format JSON utilisé par ktbs4js et stockées dans une base de données mongo. L'outil recherche par défaut une base de données MongoDB disponible en local, avec une configuration par défaut. Il est possible de changer l'adresse de connexion à la base dans le fichier ktbs_mongo.js.


Ce projet peut être utilisé de manière indépendante. Cependant le projet compagnon moodle-trace-analytics permet d'accéder plus facilement au traces via une interface web ainsi qu'une API web de requêtage.

Le projet est compatible avec la version 2.4 de Moodle et devrait être compatible avec les versions supérieures à la 2.4.

Une explication plus avancée des fichiers majeurs du projet se trouve ci-dessous:

moodle_extractor.js
-------------------

Ce fichier est la classe principale de l'application. Il se charge de la lecture des logs mySQL, du chargement (si besoin) des informations associées, de leur transformation au format JSON et de leur stockage dans la base MongoDB.

L'application se connecte à la base Mongo pour vérifier l'identifiant du dernier log traité, et reprend le traitement sur les logs suivants, si de nouveaux ont été enregistré depuis la dernière exécution. Il est donc tout à fait envisageable d'automatiser son exécution de manère nocturne via un cron.

Pour lancer l'application, exécutez la commande suivante :

```
node moodle_extractor.js
```

Si besoin, il est possible de modifier les informations de connexion de MySQL dans ce fichier. Elles se trouvent au début, dans la fonction createMySqlConnection().

ktbs_mongo.js
-------------

Ce fichier contient les informations de connexion vers la base de données MongoDB, la structure de données utilisées pour le stockage des traces, ainsi que plusieurs méthodes utilitaires pour faciliter l'accès aux données et leur traitement.

aggregate.js, aggregate*.js, order*.js
--------------------------------------

 Ces fichiers sont utilisés pour pré-calculer plusieurs aggrégats sur les données de la base MongoDB. Ces calculs étant relativement longs lorsque le volume de la base devient important, il est intéressant de les automatiser (via cron) pour éviter des temps de chargement trop long sur des métriques simples. 

 Ces métriques sont utilisées par l'application moodle-trace-analytics, l'exécution de ces requêtes au préalable accélère grandement le rendu des pages de l'application.

 Le format de données utilisé est très légèrement différent de celui d'une trace classique. Notamment, le type de la trace stockée est 'aggregate'.

Le fichier aggregate.js utilise le framework async pour lancer toutes les autres méthodes d'aggrégat contenues dans les différents fichiers. Pour lancer son exécution, utilisez la commande suivante :

```
node aggregate.js
```

 A noter, il n'existe pas à l'heure actuelle de suppression automatique de ces aggregats. Cela ne pose pas de problèmes à l'heure actuelle, et leur automatisation pourrait créer des difficultés si d'autres méthodes de création et de stockage d'aggrégats étaient utilisées à l'avenir. L'ajout de cette fonctionnalité pourrait cependant s'avérer intéressante. 

find_logs.js
------------

Ce fichier utilise l'API find classique fournie par mongoose pour collecter des logs en fonction d'un filtre passé en paramètre. Un exemple de son utilisation se trouve dans le fichier export_to_csv.js

export_to_csv.js
----------------

Ce fichier permet d'extraire l'intégralité des logs contenus dans la base MongoDB pour la promotion PACES du jeu de données exemple, l'Université de Nantes.

Il utilise simplement le finder de find_logs.js et formatte les données au format CSV, créant un fichier pour chaque mois de log identifié.