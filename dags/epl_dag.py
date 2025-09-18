from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from datetime import datetime
import pandas as pd
import os

@dag(
    "premier_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@once",
    catchup=False,
)
def epl_dag():
    os.makedirs("/tmp/data/pl_2024-25/stats", exist_ok=True)
    os.makedirs("/tmp/data/pl_2024-25/youth_aggregation", exist_ok=True)

    fetch_data = BashOperator(
        task_id="fetch_data",
        bash_command=(
            'curl -L --fail --max-time 60 '
            '-o /tmp/data/pl_2024-25/fbref_PL_2024-25.csv '
            '"https://github.com/olamideiyelolu/EPL-2024-2025_player_stats/releases/download/epl/fbref_PL_2024-25.csv"'
        ),
    )

    @task
    def clean_data():
        df = pd.read_csv("/tmp/data/pl_2024-25/fbref_PL_2024-25.csv")
        df_cleaned = df.dropna(subset=["Player", "Squad"])
        stat_cols = [c for c in df_cleaned.columns if c not in ["Player","Nation", "Pos", "Squad"]]
        df_cleaned[stat_cols] = df_cleaned[stat_cols].fillna(0)
        df_cleaned.to_csv("/tmp/data/pl_2024-25/stats/player_stats_cleaned.csv")
        print("Cleaned Player stats")


    upload_to_s3 = BashOperator(
        task_id="upload_to_s3",
        bash_command=(
        'aws s3 cp /tmp/data/pl_2024-25/stats/player_stats_cleaned.csv '
        's3://my-premier-league-data/2024-25/dataset/player_stats.csv'
        )
    )

    spark_job = SparkSubmitOperator(
        task_id="youthful_clubs",
        application="epl-etl/dags/epl_spark.py",
        name="",
        application_args=[
            "--player_stats_file", "/tmp/data/pl_2024-25/stats/player_stats_cleaned.csv",
            "--output_path", "/tmp/data/pl_2024-25/youth_aggregation"
        ],
        conn_id='spark_booking'
    )

    @task
    def load_to_postgres():
        file_path = "/tmp/data/pl_2024-25/stats/player_stats_cleaned.csv"
        df = pd.read_csv(file_path)
        #Split large data into three dataframes(for star schema model)
        clubs = df[["Squad"]].drop_duplicates().reset_index(drop=True)
        clubs["club_id"] = clubs.index + 1
        clubs = clubs.rename(columns={"Squad": "club_name"})
        players = df[["Player","Nation", "Pos", "Squad","Age","Born"]]
        players = players.drop_duplicates(subset=["Player","Squad"])
        players = players.merge(clubs, how="left", left_on="Squad", right_on="club_name")
        players = players.drop(columns=["Squad", "club_name"])
        players = players.rename(columns={
            "Player": "player_name",
            "Pos": "position",
            "Nation": "nationality",
            "Age": "age",
            "Born": "year_born"
        })

        players = players.reset_index(drop=True)
        players["player_id"] = players.index + 1
        stats = df[["Player","Squad", "MP","Starts","Min","90s","Gls","Ast","G+A","G-PK","PK","PKatt","CrdY",
                    "CrdR","xG","npxG","xAG","npxG+xAG","PrgC","PrgP","PrgR","Gls.1","Ast.1","G+A.1",
                    "G-PK.1","G+A-PK","xG.1","xAG.1","xG+xAG","npxG.1","npxG+xAG.1"]]
                    
        stats = stats.merge(clubs, left_on="Squad", right_on="club_name", how="left")
        stats = stats.merge(players[["player_id","player_name","club_id"]], how="left", left_on=["Player","club_id"], right_on=["player_name","club_id"])
        stats = stats.drop(columns=["Player","player_name","Squad","club_name"])
        stat_cols = [c for c in [
            "MP","Starts","Min","90s","Gls","Ast","G+A","G-PK","PK","PKatt",
            "CrdY","CrdR","xG","npxG","xAG","npxG+xAG","PrgC","PrgP","PrgR",
            "Gls.1","Ast.1","G+A.1","G-PK.1","G+A-PK","xG.1","xAG.1","xG+xAG",
            "npxG.1","npxG+xAG.1"
        ] if c in stats.columns]
        stats = stats[["player_id","club_id"] + stat_cols]
        stats = stats.rename(columns={
            "90s": "n90s"
        })        

        def sanit(c): return (c.lower()
                          .replace("+","_plus_")
                          .replace("-","_minus_")
                          .replace(".","_")
                          .replace("%","pct")
                          .replace(" ","_"))
        players.columns = [sanit(c) for c in players.columns]
        stats.columns   = [sanit(c) for c in stats.columns]
        clubs.columns   = [sanit(c) for c in clubs.columns]

        pg_hook = PostgresHook(postgres_conn_id="postgres_epl")

        query = """
            CREATE TABLE IF NOT EXISTS dim_clubs (
                club_id   INT PRIMARY KEY,
                club_name TEXT UNIQUE NOT NULL
            );

            CREATE TABLE IF NOT EXISTS dim_players (
                player_id   INT PRIMARY KEY,
                player_name TEXT NOT NULL,
                nationality TEXT,
                position    TEXT,
                age         INT,
                year_born   INT,
                club_id     INT REFERENCES dim_clubs(club_id)
            );

            -- Fact table (keep just core metrics; add more columns later as needed)
            CREATE TABLE IF NOT EXISTS fact_player_stats (
                player_id INT NOT NULL REFERENCES dim_players(player_id),
                club_id   INT NOT NULL REFERENCES dim_clubs(club_id),
                mp        INT,
                starts    INT,
                min       INT,
                n90s      FLOAT,
                gls       INT,
                ast       INT,
                g_plus_a        FLOAT,
                g_minus_pk        FLOAT,
                pk                   INT,
                pkatt                INT,
                crdy                 INT,
                crdr                 INT,
                xg                   FLOAT,
                npxg                 FLOAT,
                xag                  FLOAT,
                npxg_plus_xag        FLOAT,
                prgc                 INT,
                prgp                 INT,
                prgr                 INT,
                gls_1                FLOAT,
                ast_1                FLOAT,
                g_plus_a_1           FLOAT,
                g_minus_pk_1         FLOAT,
                g_plus_a_minus_pk    FLOAT,
                xg_1                 FLOAT,
                xag_1                 FLOAT,
                xg_plus_xag          FLOAT,
                npxg_1               FLOAT,
                npxg_plus_xag_1      FLOAT,
                PRIMARY KEY (player_id, club_id));

            TRUNCATE TABLE dim_clubs RESTART IDENTITY CASCADE;
            TRUNCATE TABLE dim_players RESTART IDENTITY CASCADE;
            TRUNCATE TABLE fact_player_stats;

        """
        pg_hook.run(query)

        # Convert DataFrames to list of tuples
        clubs_data = [tuple(row) for row in clubs.to_numpy()]
        players_data = [tuple(row) for row in players.to_numpy()]
        stats_data = list(stats.itertuples(index=False, name=None))

        # Insert data in PostgreSQl warehouse
        pg_hook.insert_rows(
            table="dim_clubs",
            rows=clubs_data,
            target_fields=list(clubs.columns)
        )

        pg_hook.insert_rows(
            table="dim_players", 
            rows=players_data,
            target_fields=list(players.columns)
        )
        print(stats.columns.tolist())

        pg_hook.insert_rows(
            table="fact_player_stats",
            rows=stats_data, 
            target_fields=list(stats.columns),
            schema="public"
        )



    clean = clean_data()
    load = load_to_postgres()
    fetch_data >> clean >> upload_to_s3 >> spark_job >> load


demo_dag = epl_dag()



