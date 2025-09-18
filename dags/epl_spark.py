from pyspark.sql import SparkSession
from pyspark.sql.functions import count
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--player_stats_file", required=True, help="Path to players stats")
    parser.add_argument("--output_path", required=True, help="Output path for the cleaned results")
    args = parser.parse_args()

    print(f"Reading player stats from {args.player_stats_file}")
    spark = SparkSession.builder.appName("ClubYouthContributions").getOrCreate()


    player_stats = spark.read.csv(args.player_stats_file,
        header = True,
        inferSchema=True,
        sep=",",
        quote='"',
        escape='"',
        multiLine=True,
        mode="PERMISSIVE"            
    )


    youth_contributions = player_stats \
        .filter(player_stats["Age"] < 26) \
        .groupBy("Squad") \
        .agg(
            count("G+A").alias("youth_goals_assists")
        ) \
        .orderBy("youth_goals_assists", ascending=[False])

    youth_contributions.write.mode("overwrite").csv(args.output_path)

    print(f"Aggregated results written to {args.output_path}")
    spark.stop()

if __name__ == "__main__":
    main()