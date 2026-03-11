# Rapport : pertinence des datasets « production par groupe RTE » et « éCO2mix métropoles »

Date : 2026-03-10

## Contexte du projet

Le projet corrèle **production électrique renouvelable** et **météo** via une architecture
medallion. L'état actuel :

| Couche | Données clés                                      | Granularité          |
|--------|---------------------------------------------------|----------------------|
| Silver | `fact_eco2mix_tr` / `fact_eco2mix_cons_def`        | Horaire × Région     |
| Silver | `fact_meteo_horaire`                               | Horaire × Station    |
| Silver | `dim_installations` (registre national, code EIC)  | Installation × IRIS  |
| Silver | `dim_stations_meteo` + `dim_contours_iris`         | Station / IRIS       |
| Gold   | `installations_renouvelables_avec_stations_meteo`  | Installation → station la plus proche (KNN PostGIS) |

**Le chaînon manquant** : le gold layer relie déjà chaque installation renouvelable à sa
station météo la plus proche, mais il n'y a pas encore de **production horaire par
installation** — éCO2mix ne donne la production qu'au niveau régional.

## 1. Production réalisée par groupe (RTE Actual Generation API)

### Description

L'API [Actual Generation][rte-api] de RTE expose la production **horaire** de chaque
**groupe de production** raccordé au réseau de transport, identifié par **code EIC**.

| Caractéristique          | Détail                                                        |
|--------------------------|---------------------------------------------------------------|
| Endpoint clé             | `/actual_generations_per_unit`                                |
| Granularité temporelle   | **Horaire** (H+1)                                            |
| Granularité géographique | **Par unité de production** (code EIC)                        |
| Types renouvelables      | `WIND_ONSHORE`, `WIND_OFFSHORE`, `SOLAR`, `HYDRO_RUN_OF_RIVER`, `HYDRO_WATER_RESERVOIR`, `HYDRO_PUMPED_STORAGE`, `BIOMASS` |
| Champs retournés         | code EIC, nom, type de production, valeur (MW), timestamps    |
| Historique               | Depuis mi-décembre 2014                                      |
| Authentification         | OAuth 2.0 (gratuit, inscription requise sur [data.rte-france.com][rte-portal]) |
| Limites                  | 7 jours max par requête pour per-unit                        |
| **Géolocalisation**      | **Non incluse dans l'API**                                   |

Autres endpoints disponibles :

- `/actual_generations_per_production_type` — production horaire agrégée par filière
- `/generation_mix_15min_time_scale` — mix de production au pas 15 minutes
- `/water_reserves` — niveaux hebdomadaires des réservoirs hydrauliques

### Valeur ajoutée pour le projet : FORTE

C'est **exactement** le chaînon manquant. Le projet dispose déjà de la clé de jointure :
le registre national (`dim_installations`) contient le `code_eic_resource_object` et la
localisation IRIS. Le gold layer fait déjà le KNN vers la station météo la plus proche.

La chaîne complète devient :

```
RTE Actual Generation (horaire, par EIC)
        │
        │  JOIN ON code_eic
        ▼
dim_installations (IRIS, type_energie, puissance)
        │
        │  déjà fait dans le gold layer (KNN PostGIS)
        ▼
installations_renouvelables_avec_stations_meteo
        │
        │  JOIN ON station_id + date_heure
        ▼
fact_meteo_horaire (rayonnement, vent, température, ...)
```

**Résultat** : pour chaque groupe de production renouvelable RTE, on obtient :

- Sa **production horaire réelle** (MW)
- La **météo horaire** à la station la plus proche (rayonnement solaire, vitesse du vent,
  nébulosité, température …)
- Ses **caractéristiques** (puissance installée, filière, technologie)

### Analyses rendues possibles

- **Solaire** : production vs rayonnement global (`rayonnement_global`) et durée
  d'insolation — courbe de rendement en fonction de l'irradiance
- **Éolien** : production vs vitesse du vent (`vitesse_vent`) et rafales — avec la loi
  cubique théorique P ∝ v³
- **Facteur de charge** : production réelle / puissance installée, corrélé aux conditions
  météo locales
- **Saisonnalité** : patterns jour/nuit, été/hiver par type d'énergie et par zone
  géographique
- **Hydraulique** : production vs précipitations et niveaux de réservoir

### Limitations

1. **Couverture partielle des renouvelables** : l'API ne couvre que les unités raccordées
   au **réseau de transport RTE** (haute/très haute tension). Les petites installations
   raccordées au réseau de distribution Enedis ne sont **pas** incluses. En pratique :
   - ✅ Grands parcs éoliens (typiquement > 12 MW)
   - ✅ Grandes centrales solaires au sol
   - ✅ Toute l'hydraulique (barrages, fil de l'eau)
   - ❌ Panneaux solaires résidentiels
   - ❌ Petites installations < 12 MW

2. **Jointure EIC non triviale** : le code EIC dans l'API RTE identifie un **groupe**
   (unité de production), tandis que le registre national peut avoir un EIC au niveau
   **installation** (qui regroupe plusieurs groupes). Il faudra vérifier la correspondance
   — possiblement via le champ `parent_eic_code` ou un mapping RTE.

3. **Fenêtre de requête limitée** : 7 jours max par appel API → nécessite une boucle de
   pagination pour constituer un historique.

4. **Pas de géolocalisation directe** dans l'API → dépendance au registre national pour
   la localisation (ce qui est déjà en place dans le projet).

### Intégration dans le pipeline

Nouveau flux à implémenter :

```
Landing  :  API RTE OAuth → JSON
Bronze   :  Normalisation, extraction par type renouvelable
Silver   :  fact_production_par_groupe
            (code_eic, date_heure, production_mw, type_production)
Gold     :  JOIN avec installations_renouvelables_avec_stations_meteo
            + fact_meteo_horaire
            → table d'analyse production_renouvelable_meteo
```

## 2. éCO2mix Métropoles temps réel

### Description

Le dataset [eco2mix-metropoles-tr][odre-metropoles] publié par RTE sur l'ODRÉ.

| Caractéristique          | Détail                                              |
|--------------------------|-----------------------------------------------------|
| Granularité temporelle   | **Quart d'heure**                                   |
| Granularité géographique | **22 métropoles** (code EPCI)                       |
| Historique               | Depuis février 2017 (selon la métropole)            |
| Mise à jour              | 4 fois/jour (0h, 6h, 12h, 18h)                     |
| Authentification         | Aucune (API ODS publique, quota 50 000 appels/mois) |

### Colonnes

| Champ                | Type     | Contenu                        |
|----------------------|----------|--------------------------------|
| `code_insee_epci`    | text     | Code EPCI de la métropole      |
| `libelle_metropole`  | text     | Nom de la métropole            |
| `date_heure`         | datetime | Horodatage                     |
| `consommation`       | int      | **Consommation uniquement** (MW)|
| `production`         | text     | Champ texte (non exploitable)  |
| `echanges_physiques` | text     | Échanges physiques             |

### 22 métropoles couvertes

| Date de mise en service | Métropoles |
|------------------------|------------|
| Février 2017           | Grand Paris |
| Mars 2017              | Nantes |
| Mai 2017               | Lille, Aix-Marseille-Provence, Lyon |
| Juillet 2017           | Nice, Grand Nancy, Toulouse, Rennes, Grenoble-Alpes |
| Septembre 2017         | Montpellier, Bordeaux |
| Octobre 2017           | Brest, Rouen Normandie |
| Juin 2018              | Strasbourg |
| Avril 2019             | Clermont Auvergne, Dijon, Orléans, Saint-Étienne, Toulon-Provence-Méditerranée, Tours Val de Loire |

### Valeur ajoutée pour le projet : FAIBLE

**Le dataset ne contient que la consommation**, pas la production par filière
renouvelable. Le champ `production` est un champ texte (catégoriel ou vide), pas un
volume de production exploitable. C'est une version géographiquement plus fine d'éCO2mix
mais **sans le détail production** qui fait l'intérêt d'éCO2mix régional.

### Intérêt secondaire possible

Si le projet évolue vers une **analyse de la thermosensibilité de la consommation** :

- Consommation au quart d'heure × 22 métropoles
- Corrélation avec température (`temperature` dans `fact_meteo_horaire`) → effet
  chauffage/climatisation
- Maille EPCI plus fine que la région → meilleure corrélation locale avec les stations
  météo

Mais ce n'est **pas** l'objectif actuel du projet (production renouvelable ↔ météo).

## 3. Synthèse et recommandation

| Dataset                        | Pertinence      | Effort d'intégration | Valeur analytique |
|--------------------------------|-----------------|---------------------|-------------------|
| **RTE Actual Generation**      | **Très forte**  | Moyen (API OAuth, pagination, mapping EIC) | Corrélation directe production horaire ↔ météo par installation |
| **éCO2mix Métropoles**         | **Faible**      | Faible (même pattern ODRÉ qu'éCO2mix régional) | Uniquement consommation, pas de production renouvelable |

### Recommandation

**Priorité 1 : intégrer l'API RTE Actual Generation.** C'est le dataset à plus forte
valeur ajoutée. Il comble le fossé entre :

- L'actuel éCO2mix (production horaire mais régionale, ~13 régions)
- Le registre national (par installation mais statique, pas de séries temporelles de
  production)

**éCO2mix métropoles** peut être mis de côté — à reconsidérer uniquement si le projet
s'étend à l'analyse de la demande.

## 4. Contexte réglementaire : pourquoi la maille fine n'existe pas

Les données de consommation ou de production au croisement **maille géographique fine ×
pas temporel fin** n'existent pas en open data en France. Le secret statistique (CNIL /
règle des 10 points) empêche de publier des données qui permettraient la ré-identification
de consommateurs individuels. Les données publiées respectent donc toujours l'un des deux
compromis :

- **Géo fin + temps grossier** : IRIS ou commune, mais annuel uniquement (Enedis, Agence
  ORE)
- **Temps fin + géo grossier** : demi-heure ou quart d'heure, mais national ou régional
  (RTE, Enedis)

L'exception notable est l'API RTE Actual Generation **par groupe** : la production d'une
centrale identifiée n'est pas soumise au même secret statistique que la consommation
d'un foyer. C'est pourquoi cette source est la seule à offrir à la fois granularité
temporelle fine et identification géographique précise.

## Sources

- [API RTE Actual Generation — documentation][rte-api]
- [Portail API Data RTE][rte-portal]
- [Generation achieved by unit — RTE Services][rte-gen-unit]
- [éCO2mix Métropoles TR — ODRÉ][odre-metropoles]
- [Registre national des installations — ODRÉ][registre]
- [Géolocalisation des installations — Forum Agence ORE][agence-ore-geo]

[rte-api]: https://data.rte-france.com/catalog/-/api/doc/user-guide/Actual+Generation/1.1
[rte-portal]: https://data.rte-france.com
[rte-gen-unit]: https://www.services-rte.com/en/view-data-published-by-rte/generation-achieved-by-unit.html
[odre-metropoles]: https://odre.opendatasoft.com/explore/dataset/eco2mix-metropoles-tr/
[registre]: https://odre.opendatasoft.com/explore/dataset/registre-national-installation-production-stockage-electricite-agrege/
[agence-ore-geo]: https://www.agenceore.fr/forum-expert/est-il-possible-davoir-acces-la-geolocalisation-des-installations-de-production
