#!/usr/bin/env python
# coding: utf-8
# The assumption this script makes is that you have a freshly migrated database. Please run "alembic upgrade head" beforehand

import argparse
from datetime import datetime, timezone
from uuid import uuid4

import pandas as pd
from sqlalchemy import create_engine, text

# Fill in your DB info
user_name = "postgres"
password = "password"
db_name = "postgres"
db_ip = "localhost"
port = "5432"
# Connect to the database
engine = create_engine(f"postgresql://{user_name}:{password}@{db_ip}:{port}/{db_name}")


def create_dataframes():
    dfs_by_table_name = {}
    # Create df_rotation, since no equivalent exists
    rotation_id = uuid4()
    df_rotation = pd.DataFrame(
        data=[("CTF", datetime.now(timezone.utc), rotation_id, False)],
        columns=["name", "created_at", "id", "is_random"],
    )
    # Load each csv file into a DataFrame
    df_queue = pd.read_csv("queue.csv", keep_default_na=False)
    df_queue_region = pd.read_csv("queue_region.csv", keep_default_na=False)
    df_player = pd.read_csv("player.csv", keep_default_na=False)
    df_player_region_trueskill = pd.read_csv(
        "player_region_trueskill.csv", keep_default_na=False
    )
    df_finished_game = pd.read_csv("finished_game.csv", keep_default_na=False)
    df_finished_game_player = pd.read_csv(
        "finished_game_player.csv", keep_default_na=False
    )
    df_map = pd.read_csv("map.csv", keep_default_na=False)

    # rotation
    print(df_rotation.head)

    # map
    # drop the extra columns after each rotation_map has been created, since we need them for creating rotation_map entries
    df_map["created_at"] = datetime.now(timezone.utc)

    # Create df_rotation_map, since no equivalent exists
    data = []
    columns = [
        "created_at",
        "id",
        "raffle_ticket_reward",
        "is_random",
        "random_probability",
        "ordinal",
        "rotation_id",
        "map_id",
        "is_next",
        "updated_at",
    ]
    for i, row in df_map.iterrows():
        data.append(
            (
                datetime.now(timezone.utc),
                uuid4(),
                0,
                False,
                0,
                row["rotation_index"],
                df_rotation["id"].values[0],
                row["id"],
                True if row["full_name"] == "Elite" else False,
                datetime.now(timezone.utc),
            )
        )
    df_rotation_map = pd.DataFrame(
        data=data,
        columns=columns,
    )
    print(df_rotation_map.head)

    # map (drop the extra columns)
    df_map = df_map.drop(
        labels=[
            "rotation_index",
            "rotation_weight",
            "is_votable",
        ],
        axis=1,
        errors="ignore",
    )
    print(df_map.head)

    # queue_region -> category
    df_queue_region["is_rated"] = True
    df_queue_region["created_at"] = datetime.now(timezone.utc)
    print(df_queue_region.head)

    # Migrate Queue
    df_queue.rename(columns={"queue_region_id": "category_id"}, inplace=True)
    df_queue["is_sweaty"] = False
    df_queue["mu_max"] = None
    df_queue["mu_min"] = None
    df_queue["ordinal"] = 0
    df_queue["rotation_id"] = rotation_id
    df_queue["move_enabled"] = False
    df_queue["currency_award"] = None
    df_queue["vote_threshold"] = None
    df_queue.loc[df_queue["name"] == "NA", ["ordinal"]] = 1
    df_queue.loc[df_queue["name"] == "EU", ["ordinal"]] = 2
    df_queue.loc[df_queue["name"] == "NaWest", ["ordinal"]] = 3
    print(df_queue.head)

    # player
    df_player["raffle_tickets"] = 0
    df_player["leaderboard_enabled"] = True
    df_player["stats_enabled"] = True
    df_player["move_enabled"] = False
    df_player["currency"] = 0
    df_player = df_player.drop(
        labels=["unrated_trueskill_mu", "unrated_trueskill_sigma"], axis=1
    )
    print(df_player.head)

    # player_region_trueskill -> player_category_trueskill
    df_player_region_trueskill = df_player_region_trueskill.drop(
        labels=[
            "unrated_trueskill_mu",
            "unrated_trueskill_sigma",
        ],
        axis=1,
        errors="ignore",
    )
    df_player_region_trueskill["rank"] = df_player_region_trueskill[
        "rated_trueskill_mu"
    ] - (3 * df_player_region_trueskill["rated_trueskill_sigma"])
    df_player_region_trueskill.rename(
        columns={
            "queue_region_id": "category_id",
            "rated_trueskill_mu": "mu",
            "rated_trueskill_sigma": "sigma",
        },
        inplace=True,
    )
    print(df_player_region_trueskill.head)

    # finished_game
    df_finished_game.rename(
        columns={"queue_region_name": "category_name"}, inplace=True
    )
    print(df_finished_game.head)

    # finished_game_player
    df_finished_game_player = df_finished_game_player.drop(
        labels=[
            "unrated_trueskill_mu_before",
            "unrated_trueskill_sigma_before",
            "unrated_trueskill_mu_after",
            "unrated_trueskill_sigma_after",
        ],
        axis=1,
        errors="ignore",
    )
    print(df_finished_game_player.head)

    dfs_by_table_name["category"] = df_queue_region
    dfs_by_table_name["map"] = df_map
    dfs_by_table_name["rotation"] = df_rotation
    dfs_by_table_name["rotation_map"] = df_rotation_map
    dfs_by_table_name["queue"] = df_queue
    dfs_by_table_name["player"] = df_player
    dfs_by_table_name["player_category_trueskill"] = df_player_region_trueskill
    dfs_by_table_name["finished_game"] = df_finished_game
    dfs_by_table_name["finished_game_player"] = df_finished_game_player
    return dfs_by_table_name


def create_table_csvs():
    """
    copy queue to '/tmp/queue.csv' with (format csv, header);
    copy queue_region to '/tmp/queue_region.csv' with (format csv, header);
    copy player to '/tmp/player.csv' with (format csv, header);
    copy player_region_trueskill to '/tmp/player_region_trueskill.csv' with (format csv, header);
    copy finished_game to '/tmp/finished_game.csv' with (format csv, header);
    copy finished_game_player to '/tmp/finished_game_player.csv' with (format csv, header);
    copy map to '/tmp/map.csv' with (format csv, header);
    """
    with engine.connect() as con:
        con.execute(text("copy queue to '/tmp/queue.csv' with (format csv, header)"))
        print("Created /tmp/queue.csv")
        con.execute(
            text(
                "copy queue_region to '/tmp/queue_region.csv' with (format csv, header)"
            )
        )
        print("Created /tmp/queue_region.csv")
        con.execute(text("copy player to '/tmp/player.csv' with (format csv, header)"))
        print("Created /tmp/player.csv")
        con.execute(
            text(
                "copy player_region_trueskill to '/tmp/player_region_trueskill.csv' with (format csv, header)"
            )
        )
        print("Created /tmp/player_region_trueskill.csv")
        con.execute(
            text(
                "copy finished_game to '/tmp/finished_game.csv' with (format csv, header)"
            )
        )
        print("Created /tmp/finished_game.csv")
        con.execute(
            text(
                "copy finished_game_player to '/tmp/finished_game_player.csv' with (format csv, header)"
            )
        )
        print("Created /tmp/finished_game_player.csv")
        con.execute(text("copy map to '/tmp/map.csv' with (format csv, header)"))
        print("Created /tmp/map.csv")


def clear_db_tables():
    # Ensure the corresponding tables are clean
    with engine.connect() as con:
        con.execute(text("DELETE FROM finished_game_player"))
        con.execute(text("DELETE FROM finished_game"))
        con.execute(text("DELETE FROM player_category_trueskill"))
        con.execute(text("DELETE FROM player"))
        con.execute(text("DELETE FROM queue"))
        con.execute(text("DELETE FROM category"))
        con.execute(text("DELETE FROM rotation_map"))
        con.execute(text("DELETE FROM map"))
        con.execute(text("DELETE FROM rotation"))
        con.commit()
        print("Tables cleared")


# def insert(df_queue_region, df_map, df_rotation, df_rotation_map, df_queue, df_player, df_player_region_trueskill, df_finished_game, df_finished_game_player):
def commit(dfs_by_table_name: dict[str, pd.DataFrame]):
    with engine.connect() as con:  # for some reason, this only works with raw_connections
        for table_name, df in dfs_by_table_name.items():
            df.to_sql(table_name, con=con, if_exists="append", index=False)


def parse_args() -> dict[str, any]:
    parser = argparse.ArgumentParser(
        description="Soft reset a queue region. Defaults to 'dry run' unless specifically told to overwrite data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help='Emits a "DELETE FROM" on the related tables, useful for running the script multiple times. Stops the script after.',
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help="Inserts the dataframes into into the DB",
    )
    parser.add_argument(
        "--generate_csv",
        action="store_true",
        help="Creates the necessary CSV files for the script to run in /tmp. If postgres is running inside a docker container, then you will need to docker cp them out. Stops the script after",
    )
    arguments = parser.parse_args()
    return vars(arguments)


def main():
    input_args = parse_args()
    if input_args["generate_csv"]:
        create_table_csvs()
        print(
            'Please copy the csv files to the same directory as this script and re-run without "--generate_csv"'
        )
        return
    if input_args["clean"]:
        clear_db_tables()
        return
    dfs_by_table_name = create_dataframes()
    if input_args["commit"]:
        commit(dfs_by_table_name)


if __name__ == "__main__":
    main()
