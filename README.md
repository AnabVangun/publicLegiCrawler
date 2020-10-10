# publicLegiCrawler

Ce script permet de parcourir la base Légifrance pour en extraire des 
informations d'intérêt sur des textes répondant à des critères de recherche. 
Il est rédigé en Python 3.7.7.

Pour le faire tourner, il est nécessaire de :
1. disposer d'une connexion Internet permettant de joindre le site 
https://developer.aife.economie.gouv.fr/
2. disposer d'un compte sur ledit site avec une application Sandbox dans 
laquelle l'API Légifrance est activée (ou *a minima* des identifiants OAuth pour
l'API Légifrance)
3. disposer d'une base de données PostgreSQL accessible
4. créer dans le package legiCrawler les fichiers suivants :
    1. secret.py, assignant les variables :
        1. CLIENT_ID et CLIENT_SECRET correspondant aux identifiants OAuth déterminés à l'étape 2
        2. DB_NAME, DB_USER et DB_PW correspondant au nom de la base de données et aux identifiants permettant de s'y connecter
    2. dummies.py, si et seulement si l'option "dummy" du LegiConnector est mise à True:
        1. getCidList(page:int, pageSize:int)->List[int] : renvoie une liste de CID comme Légifrance pourrait le faire
        2. getText(cid:str) -> dict : renvoie un texte Légifrance au format dictionnaire Python
    3. dbStructure.py : description ADU (fichier définissant la structure de la base de données)
    4. legiStructure.py : description ADU (fichier définissant le filtre de recherche à utiliser pour requêter Légifrance et la structure des textes récupérés par le filtre)
5. si la base de données n'est pas accesssible à l'adresse 127.0.0.1:5432, quelques
adaptations des scripts seront nécessaires.

Cette application est conçue pour séparer trois éléments : 
1. La définition de la base de données :
	- Portée par le fichier `dbStructure.py`
	- Partiellement modifiable pour s'adapter au cas d'usage à traiter : la 
	structure globale de la base ne change pas, mais les informations stockées 
	dedans dépendent des textes à analyser
2. La définition des textes à analyser :
	- Portée par les fichiers `basePattern.py` et `legiStructure.py`
	- Le fichier `basePattern.py` ne dépend pas du cas d'usage
	- Le fichier `legiStructure.py` définit la structure des textes à analyser 
	et la façon de requêter Légifrance pour les obtenir
3. La logique applicative :
	- Portée par les autres fichiers
	- Elle ne dépend pas du cas d'usage, et s'appuie sur la structure des 
	deux autres éléments pour requêter Légifrance, parser les textes et 
	envoyer les données d'intérêt à la base de données pour les stocker.
	
Le fichier `main.py` définit deux booléens, `init_db` et `runTest`. Le premier
permet d'initialiser les types et tables nécessaires dans une base de données 
PostgreSQL déjà initialisée, et le deuxième de requêter Légifrance pour 
récupérer les données d'intérêt et les stocker dans la base.
