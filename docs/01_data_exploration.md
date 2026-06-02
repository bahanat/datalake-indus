## Hétérogénéité des schémas

Les fichiers présentent plusieurs différences de nommage :

| Nom standard | Variantes observées |
|--------------|--------------------|
| temperature | Temperature, temperature |
| pressure | Pressure, pressure |
| elapsed_time | Elapsed_time, elapsed_time |

Décision :
- Tous les noms de colonnes seront convertis en minuscules
- Les espaces seront remplacés par des underscores
- Le schéma cible sera harmonisé dans la couche staging