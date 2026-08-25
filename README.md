# Steam's videogames platform - EDA in PySpark (Databricks)

> Mandatory project for **block 2** (Exploratory, descriptive and inferential data
> analysis) of the French **CDSD certification** - Concepteur Développeur en Science
> des Données | RNCP35288 | JEDHA

## Problem statement

Ubisoft wants to better understand the Steam's ecosystem before launching a new videogame. The idea is to analyze the more than 55,000 videogames available on the platform to identify trends, high-performing genres, and the factors that drive a title's popularity.

## Deliverable on Databricks

- **Part 1** - Analysis at the "macro" level:
> https://dbc-1513e7d9-850d.cloud.databricks.com/editor/notebooks/682786244233309?o=7474647522711152

- **Part 2** - Genres analysis:
> https://dbc-1513e7d9-850d.cloud.databricks.com/editor/notebooks/682786244233311?o=7474647522711152

- **Part 3** - Platform analysis:
> https://dbc-1513e7d9-850d.cloud.databricks.com/editor/notebooks/682786244233310?o=7474647522711152

## Technical stack

Pyspark on Databricks

## Project structure

```
Projet-CDSD_Bloc-2_Steam/
├── data/
│   └── raw/
│       └── steam_game_output.json          # raw dataset
│       └── steam_games.csv                 # dataset converted in .csv file
├── docs/
│   └── Project_Steams-videogames-platform_instructions.ipynb        # project statement
├── notebooks/
│   └── Steam_data-conversion-csv.ipynb     # conversion data to csv notebook
│   └── Steam_Part1_Macro-Analysis.py
│   └── Steam_Part2_Genre-Analysis.py
│   └── Steam_Part3_Platform-Analysis.py
├── reports/
│   └── figures/                            # data visualization
└── README.md
```

## Data

**Source**: s3://full-stack-bigdata-datasets/Big_Data/Project_Steam/steam_game_output.json

## Approach

EDA using PySpark on Databricks.
The analysis was carried out in three stages:

**Part 1 - Analysis at the "macro" level:**
- Which publishers release the most titles?
- Which years saw the highest number of releases?
- How are prices distributed?
- Which languages ​​dominate?
- How many games are restricted to adults?

**Part 2 - Genre analysis:**
- Which genres dominate the catalog?
- Which have the best ratio of positive reviews?
- Which genres do major publishers favor?
- Which are the most lucrative?

**Part 3 - Platform analysis:**
- Windows/Mac/Linux distribution
- Are certain genres linked to specific platforms?

## Author

**Geoffrey MADALINSKI** - Certification CDSD (RNCP35288) - JEDHA

---
