import logging
from datetime import datetime, timedelta, timezone
from typing import List, Literal

from discord import Colour, Embed, Interaction, app_commands
from discord.ext.commands import Bot
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.session import Session as SQLAlchemySession

import discord_bots.config as config
from discord_bots.checks import is_admin_app_command, is_command_channel
from discord_bots.cogs.base import BaseCog
from discord_bots.config import MAP_VOTE_THRESHOLD
from discord_bots.models import (
    InProgressGame,
    InProgressGamePlayer,
    Map,
    MapVote,
    Player,
    Queue,
    Rotation,
    RotationMap,
    Session,
    SkipMapVote,
    VotePassedWaitlist,
)
from discord_bots.utils import update_next_map_to_map_after_next

_log = logging.getLogger(__name__)


class VoteCommands(BaseCog):
    def __init__(self, bot: Bot):
        super().__init__(bot)

    group = app_commands.Group(name="vote", description="Vote commands")

    def get_maps_str(self):
        maps: list[Map] = Session().query(Map).all()
        return ", ".join([map.short_name for map in maps])

    @group.command(
        name="setmapthreshold", description="Set the number of votes required to pass"
    )
    @app_commands.check(is_admin_app_command)
    @app_commands.check(is_command_channel)
    @app_commands.guild_only()
    @app_commands.describe(threshold="Number of votes required")
    async def setmapvotethreshold(self, interaction: Interaction, threshold: int):
        """
        Set the number of votes required to pass
        # TODO move to db-config, make dependent on queue size if possible
        """
        global MAP_VOTE_THRESHOLD
        MAP_VOTE_THRESHOLD = threshold

        await interaction.response.send_message(
            embed=Embed(
                description=f"Map vote threshold set to {MAP_VOTE_THRESHOLD}",
                colour=Colour.green(),
            )
        )

    @group.command(
        name="skipgamemap",
        description="Vote to skip to the next map for an in-progress game",
    )
    @app_commands.check(is_command_channel)
    @app_commands.guild_only()
    async def skipgamemap(self, interaction: Interaction):
        """
        Vote to skip to the next map for an in-progress game
        """
        if not interaction.channel:
            await interaction.response.send_message(
                embed=Embed(
                    description="Command must be run from a guild text channel",
                    colour=Colour.red(),
                ),
                ephemeral=True,
            )
            return

        session: SQLAlchemySession
        with Session() as session:
            ipgp: InProgressGamePlayer | None = (
                session.query(InProgressGamePlayer)
                .filter(InProgressGamePlayer.player_id == interaction.user.id)
                .first()
            )
            if not ipgp:
                await interaction.response.send_message(
                    embed=Embed(
                        description="You must be in a game to use this",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return

            session.add(SkipMapVote(interaction.channel.id, interaction.user.id))
            try:
                session.commit()
            except IntegrityError:
                await interaction.response.send_message(
                    embed=Embed(
                        description="You have already voted",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                session.rollback()
                return

            ipg: InProgressGame | None = (
                session.query(InProgressGame)
                .filter(InProgressGame.id == ipgp.in_progress_game_id)
                .first()
            )

            skip_map_votes = (
                session.query(SkipMapVote)
                .join(
                    InProgressGamePlayer,
                    InProgressGamePlayer.player_id == SkipMapVote.player_id,
                )
                .filter(InProgressGamePlayer.in_progress_game_id == ipg.id)
                .all()
            )

            queue_vote_threshold = (
                session.query(Queue.vote_threshold)
                .join(InProgressGame, InProgressGame.queue_id == Queue.id)
                .filter(InProgressGame.id == ipg.id)
                .scalar()
            )

            if len(skip_map_votes) >= queue_vote_threshold:
                rotation: Rotation | None = (
                    session.query(Rotation)
                    .join(Queue, Queue.rotation_id == Rotation.id)
                    .join(InProgressGame, InProgressGame.queue_id == Queue.id)
                    .filter(InProgressGame.id == ipg.id)
                    .first()
                )
                new_map: Map | None = (
                    session.query(Map)
                    .join(RotationMap, RotationMap.map_id == Map.id)
                    .join(Rotation, Rotation.id == RotationMap.rotation_id)
                    .filter(Rotation.id == rotation.id)
                    .filter(RotationMap.is_next == True)
                    .first()
                )

                ipg.map_full_name = new_map.full_name
                ipg.map_short_name = new_map.short_name
                for skip_map_vote in skip_map_votes:
                    session.delete(skip_map_vote)
                session.commit()

                await self.send_success_message(
                    f"Vote to skip the current map passed!  All votes removed.\n\nNew map: **{ipg.map_full_name} ({ipg.map_short_name})**"
                )
                await update_next_map_to_map_after_next(rotation.id, True)
            else:
                await self.send_success_message("Your vote has been cast!")

    @group.command(name="unvote", description="Remove all of a player's votes")
    @app_commands.check(is_command_channel)
    @app_commands.guild_only()
    async def unvote(self, interaction: Interaction):
        """
        Remove all of a player's votes
        """
        session: SQLAlchemySession
        with Session() as session:
            session.query(MapVote).filter(
                MapVote.player_id == interaction.user.id
            ).delete()
            session.query(SkipMapVote).filter(
                SkipMapVote.player_id == interaction.user.id
            ).delete()
            session.commit()

        await interaction.response.send_message(
            embed=Embed(
                description="All map votes deleted",
                colour=Colour.green(),
            )
        )

    @group.command(
        name="unvotemap", description="Remove all of a player's votes for a map"
    )
    @app_commands.check(is_command_channel)
    @app_commands.guild_only()
    @app_commands.describe(map_name="Map to remove votes from")
    async def unvotemap(self, interaction: Interaction, map_name: str):
        """
        Remove all of a player's votes for a map
        Use irrespective of rotation/queue because that seems like a super niche use case
        TODO: Unvote for many maps at once
        """
        session: SQLAlchemySession
        with Session() as session:
            map: Map | None = (
                session.query(Map).filter(Map.short_name.ilike(map_name)).first()
            )
            if not map:
                await interaction.response.send_message(
                    embed=Embed(
                        description="Map not found",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return

            map_votes: list[MapVote] | None = (
                session.query(MapVote)
                .join(RotationMap, RotationMap.id == MapVote.rotation_map_id)
                .filter(
                    MapVote.player_id == interaction.user.id,
                    RotationMap.map_id == map.id,
                )
                .all()
            )
            if not map_votes:
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"You don't have any votes for {map_name}",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return

            for map_vote in map_votes:
                session.delete(map_vote)
            session.commit()
            await interaction.response.send_message(
                embed=Embed(
                    description=f"Your vote for {map.short_name} was removed",
                    colour=Colour.green(),
                )
            )

    @group.command(
        name="unskip", description="Remove all of a player's votes to skip the next map"
    )
    @app_commands.check(is_command_channel)
    @app_commands.guild_only()
    async def unvoteskip(self, interaction: Interaction):
        """
        Remove all of a player's votes to skip the next map
        Same disregard for rotation/queue as with unvotemap
        """
        session: SQLAlchemySession
        with Session() as session:
            skip_map_votes: List[SkipMapVote] | None = (
                session.query(SkipMapVote)
                .filter(SkipMapVote.player_id == interaction.user.id)
                .all()
            )
            if not skip_map_votes:
                await interaction.response.send_message(
                    embed=Embed(
                        description="You don't have a vote to skip the current map.",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return

            for skip_map_vote in skip_map_votes:
                session.delete(skip_map_vote)
            session.commit()
            await interaction.response.send_message(
                embed=Embed(
                    description="Your vote to skip the current map was removed.",
                    colour=Colour.green(),
                )
            )

    @group.command(name="mock", description="Generates 6 mock votes for testing")
    @app_commands.check(is_admin_app_command)
    @app_commands.check(is_command_channel)
    @app_commands.guild_only()
    @app_commands.describe(
        type="Map: Mocks MapVote for first rotation_map | skip: Mocks SkipMapVote for first rotation",
        count="Number of mock records",
    )
    async def mockvotes(
        self, interaction: Interaction, type: Literal["map", "skip"], count: int
    ):
        """
        Generates 6 mock votes for testing
        Testing must be done quick because afk_timer_task clears the votes every minute

        map: mocks MapVote entries for first rotation_map
        skip: mocks SkipMapVote entries for first rotation
        """
        lesser_gods = [
            115204465589616646,
            347125254050676738,
            508003755220926464,
            649029546749853706,
        ]
        if interaction.user.id not in lesser_gods:
            await interaction.response.send_message(
                embed=Embed(
                    description="Only special people can use this command",
                    colour=Colour.red(),
                ),
                ephemeral=True,
            )
            return
        if not interaction.channel:
            await interaction.response.send_message(
                embed=Embed(
                    description="Command must be run from a guild text channel",
                    colour=Colour.red(),
                ),
                ephemeral=True,
            )
            return

        session: SQLAlchemySession
        with Session() as session:
            if type == "map":
                rotation_map: RotationMap | None = session.query(RotationMap).first()
                if not rotation_map:
                    await interaction.response.send_message(
                        embed=Embed(
                            description="No rotations found",
                            colour=Colour.red(),
                        ),
                        ephemeral=True,
                    )
                    return

                player_ids = [
                    x[0]
                    for x in session.query(Player.id)
                    .filter(Player.id.not_in(lesser_gods))
                    .limit(count)
                    .all()
                ]

                for player_id in player_ids:
                    session.add(
                        MapVote(
                            interaction.channel.id,
                            player_id,
                            rotation_map.id,
                        )
                    )

                queue_name = (
                    session.query(Queue.name)
                    .join(Rotation, Rotation.id == Queue.rotation_id)
                    .filter(Rotation.id == rotation_map.rotation_id)
                    .first()[0]
                )
                map_short_name = (
                    session.query(Map.short_name)
                    .filter(Map.id == rotation_map.map_id)
                    .first()[0]
                )
                final_vote_command = f"!votemap {queue_name} {map_short_name}"
            elif type == "skip":
                rotation: Rotation | None = session.query(Rotation).first()
                if not rotation:
                    await interaction.response.send_message(
                        embed=Embed(
                            description="No rotations found",
                            colour=Colour.red(),
                        ),
                        ephemeral=True,
                    )
                    return

                player_ids = [
                    x[0]
                    for x in session.query(Player.id)
                    .filter(Player.id.not_in(lesser_gods))
                    .limit(count)
                    .all()
                ]

                for player_id in player_ids:
                    session.add(
                        SkipMapVote(
                            interaction.channel.id,
                            player_id,
                            rotation.id,
                        )
                    )

                queue_name = (
                    session.query(Queue.name)
                    .join(Rotation, Rotation.id == Queue.rotation_id)
                    .filter(Rotation.id == rotation.id)
                    .first()[0]
                )
                final_vote_command = f"!voteskip {queue_name}"
            elif type == "skipgame":
                player_ids = [
                    x[0]
                    for x in session.query(InProgressGamePlayer.player_id)
                    .filter(InProgressGamePlayer.player_id.not_in(lesser_gods))
                    .limit(count)
                    .all()
                ]
                for player_id in player_ids:
                    session.add(SkipMapVote(interaction.channel.id, player_id))
                final_vote_command = "!skipgamemap"
            else:
                await interaction.response.send_message(
                    embed=Embed(
                        description="Usage: !mockvotes <map|skip|skipgame>",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return

            session.commit()

            await interaction.response.send_message(
                embed=Embed(
                    description=f"Mock votes added!\nTo add your vote use `{final_vote_command}`",
                    colour=Colour.green(),
                )
            )

    # @group.command(name="map", description="Vote for a map in a queue")
    # @app_commands.guild_only()
    # @app_commands.describe(queue_name="Existing queue", map_name="Map to vote gor")
    async def votemap(self, interaction: Interaction, queue_name: str, map_name: str):
        """
        Vote for a map in a queue
        TODO: Vote for many maps at once
        TODO: Decide if/how to list voteable maps for each queue/rotation
        """
        if not interaction.channel:
            await interaction.response.send_message(
                embed=Embed(
                    description="Command must be run from a guild text channel",
                    colour=Colour.red(),
                ),
                ephemeral=True,
            )
            return

        session: SQLAlchemySession
        with Session() as session:
            queue: Queue | None = (
                session.query(Queue).filter(Queue.name.ilike(queue_name)).first()
            )
            if not queue:
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"Could not find queue **{queue_name}**",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return

            rotation: Rotation | None = (
                session.query(Rotation)
                .join(Queue, Queue.rotation_id == Rotation.id)
                .filter(Queue.id == queue.id)
                .first()
            )
            if not rotation:
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"Could not find rotation for queue **{queue_name}**",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return

            map: Map | None = (
                session.query(Map).filter(Map.short_name.ilike(map_name)).first()
            )
            if not map:
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"Could not find map **{map_name}**",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return

            rotation_map: RotationMap | None = (
                session.query(RotationMap)
                .filter(RotationMap.map_id == map.id)
                .filter(RotationMap.rotation_id == rotation.id)
                .first()
            )
            if not rotation_map:
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"Could not find map in rotation **{rotation.name}**",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return

            session.add(
                MapVote(interaction.channel.id, interaction.user.id, rotation_map.id)
            )
            try:
                session.commit()
            except IntegrityError:
                session.rollback()

            rotation_map_votes: list[MapVote] = (
                session.query(MapVote)
                .filter(MapVote.rotation_map_id == rotation_map.id)
                .all()
            )
            if len(rotation_map_votes) >= config.MAP_VOTE_THRESHOLD:
                session.query(RotationMap).filter(
                    RotationMap.rotation_id == rotation.id
                ).filter(RotationMap.is_next == True).update({"is_next": False})
                rotation_map.is_next = True

                map_votes = (
                    session.query(MapVote)
                    .join(RotationMap, RotationMap.id == MapVote.rotation_map_id)
                    .filter(RotationMap.rotation_id == rotation.id)
                    .all()
                )
                for map_vote in map_votes:
                    session.delete(map_vote)
                session.query(SkipMapVote).filter(
                    SkipMapVote.rotation_id == rotation.id
                ).delete()

                if interaction.guild and interaction.channel:
                    # TODO: Check if another vote already exists
                    session.add(
                        VotePassedWaitlist(
                            channel_id=interaction.channel.id,
                            guild_id=interaction.guild.id,
                            end_waitlist_at=datetime.now(timezone.utc)
                            + timedelta(seconds=config.RE_ADD_DELAY),
                        )
                    )
                session.commit()

                await interaction.response.send_message(
                    embed=Embed(
                        description=f"Vote for **{map.full_name} ({map.short_name})** passed!\nMap rotated, all votes removed",
                        colour=Colour.green(),
                    )
                )
            else:
                map_votes = (
                    session.query(MapVote)
                    .filter(MapVote.rotation_map_id == rotation_map.id)
                    .count()
                )

                await interaction.response.send_message(
                    embed=Embed(
                        description=f"Added map vote for **{map.short_name}** in **{queue.name}**.\n`!unvotemap` to remove your vote.\nMap vote status: [{map_votes}/{config.MAP_VOTE_THRESHOLD}]",
                        colour=Colour.green(),
                    )
                )

            # old logic for showing current vote status
            # map_votes: list[MapVote] = session.query(MapVote).all()
            # voted_map_ids: list[str] = [map_vote.map_id for map_vote in map_votes]
            # voted_maps: list[Map] = (
            #     session.query(Map).filter(Map.id.in_(voted_map_ids)).all()  # type: ignore
            # )
            # voted_maps_str = ", ".join(
            #     [
            #         f"{voted_map.short_name} [{voted_map_ids.count(voted_map.id)}/{config.MAP_VOTE_THRESHOLD}]"
            #         for voted_map in voted_maps
            #     ]
            # )

    # @group.command(name="skip", description="Vote to skip a map in a queue")
    # @app_commands.guild_only()
    # @app_commands.describe(queue_name="Queue to vote skip")
    async def voteskip(self, interaction: Interaction, queue_name: str):
        """
        Vote to skip a map in a queue
        """
        if not interaction.channel:
            await interaction.response.send_message(
                embed=Embed(
                    description="Command must be run from a guild text channel",
                    colour=Colour.red(),
                ),
                ephemeral=True,
            )
            return

        session: SQLAlchemySession
        with Session() as session:
            queue: Queue | None = (
                session.query(Queue).filter(Queue.name.ilike(queue_name)).first()
            )
            if not queue:
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"Could not find queue **{queue_name}**",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return

            rotation: Rotation | None = (
                session.query(Rotation)
                .join(Queue, Queue.rotation_id == Rotation.id)
                .filter(Queue.id == queue.id)
                .first()
            )
            if not rotation:
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"Could not find rotation for queue **{queue_name}**",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return

            session.add(
                SkipMapVote(interaction.channel.id, interaction.user.id, rotation.id)
            )
            try:
                session.commit()
            except IntegrityError:
                session.rollback()

            skip_map_votes_count = (
                session.query(SkipMapVote)
                .filter(SkipMapVote.rotation_id == rotation.id)
                .count()
            )
            if skip_map_votes_count >= config.MAP_VOTE_THRESHOLD:
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"Vote to skip the current map passed!  All votes removed.",
                        colour=Colour.green(),
                    )
                )
                await update_next_map_to_map_after_next(rotation.id, True)

                if interaction.guild:
                    # TODO: Might be bugs if two votes pass one after the other
                    vpw: VotePassedWaitlist | None = session.query(
                        VotePassedWaitlist
                    ).first()
                    if not vpw:
                        session.add(
                            VotePassedWaitlist(
                                channel_id=interaction.channel.id,
                                guild_id=interaction.guild.id,
                                end_waitlist_at=datetime.now(timezone.utc)
                                + timedelta(seconds=config.RE_ADD_DELAY),
                            )
                        )

                session.commit()
            else:
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"Added vote to skip the current map.\n!unvoteskip to remove vote.\nVotes to skip: [{skip_map_votes_count}/{config.MAP_VOTE_THRESHOLD}]",
                        colour=Colour.green(),
                    )
                )

    @unvotemap.autocomplete("map_name")
    # @votemap.autocomplete("map_name")
    async def map_autocomplete(self, interaction: Interaction, current: str):
        result = []
        session: SQLAlchemySession
        with Session() as session:
            maps: list[Map] | None = (
                session.query(Map).order_by(Map.full_name).limit(25).all()
            )
            if maps:
                for map in maps:
                    if current in map.short_name:
                        result.append(
                            app_commands.Choice(
                                name=map.full_name, value=map.short_name
                            )
                        )
        return result

    # @voteskip.autocomplete("queue_name")
    async def queue_autocomplete(self, interaction: Interaction, current: str):
        result = []
        session: SQLAlchemySession
        with Session() as session:
            queues: list[Queue] | None = (
                session.query(Queue).order_by(Queue.name).limit(25).all()
            )
            if queues:
                for queue in queues:
                    if current in queue.name:
                        result.append(
                            app_commands.Choice(name=queue.name, value=queue.name)
                        )
        return result
