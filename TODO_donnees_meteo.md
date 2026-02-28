## Données météorologiques

### Météo France

- (base des deux API disponible) : https://donneespubliques.meteofrance.fr/

- API climatologie
    - [référence](https://portail-api.meteofrance.fr/web/fr/api/DonneesPubliquesClimatologie)
    - [documentation](https://confluence-meteofrance.atlassian.net/wiki/spaces/OpenDataMeteoFrance/pages/854261785/API+Donn+es+Climatologiques)
    - https://www.data.gouv.fr/datasets/donnees-climatologiques-de-base-horaires
        - (identique) https://meteo.data.gouv.fr/datasets/6569b4473bedf2e7abad3b72
    - Données qualifiées, c'est-à-dire avec un code qualité pour de nombreux paramètres
    - Est-ce qu'on peut utiliser ces données en "temps réel" (toutes les heures) ?
    - API asynchrone (commande-fichier)
    - API pour obtenir la liste des stations concernées : liste-stations/horaire
        - par département
        - envoie aussi les stations anciennes
        - peut filtrer les stations par type de paramètres mesurés (pression, vent,
          rayonnement ...)
    - À vérifier : renvoie 196 colonnes tout le temps pour toutes les stations ?

- API observation
    - [référence](https://portail-api.meteofrance.fr/web/fr/api/DonneesPubliquesObservation)
    - [documentation API ciblée](https://confluence-meteofrance.atlassian.net/wiki/spaces/OpenDataMeteoFrance/pages/853639294/API+Cibl+e+Donn+es+d+Observation)
    - [documentation API paquet](https://confluence-meteofrance.atlassian.net/wiki/spaces/OpenDataMeteoFrance/pages/854851588/API+Paquet+Observations)

- Liste stations avec historique complet
    - https://www.data.gouv.fr/datasets/informations-sur-les-stations-metadonnees
    - avantages: contient la liste exhaustive des paramètres que mesurent les stations
    - inconvénients: cette liste est publiée avec les noms des paramètres, mais sans les
      codes associés ce qui complique les jointures avec les jeux de données
      observations/climatologie

- Qualité des données
    - https://confluence-meteofrance.atlassian.net/wiki/spaces/OpenDataMeteoFrance/pages/621510657/Donn+es+climatologiques+de+base
        - par exemple, les stations météo de type 0 sont les plus précises, y a-t-il une
          différence flagrante de précision avec les autres et est-ce que ça impacte la
          qualité de l'analyse ?

### Info climat

- liste stations
    - https://www.data.gouv.fr/datasets/liste-des-stations-en-open-data-du-reseau-meteorologique-infoclimat-static-et-meteo-france-synop
    - https://www.infoclimat.fr/opendata/stations_xhr.php?format=geojson

### Données supplémentaires ?

- https://www.copernicus.eu/en