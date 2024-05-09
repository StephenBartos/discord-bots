import logging
import os
import sys
from datetime import datetime, timezone
from shutil import copyfile
from sqlalchemy.orm.session import Session as SQLAlchemySession
from typing import List, Literal

from discord import (
    app_commands,
    Colour,
    Embed,
    Interaction,
    Member,
    Role,
)
from discord.ext.commands import Bot
from discord.utils import escape_markdown

import discord_bots.config as config
from discord_bots.bot import bot
from discord_bots.checks import is_admin_app_command, is_command_channel
from discord_bots.cogs.base import BaseCog
from discord_bots.models import (
    AdminRole,
    CustomCommand,
    DiscordGuild,
    FinishedGame,
    FinishedGamePlayer,
    Player,
    Queue,
    QueuePlayer,
    QueueWaitlist,
    QueueWaitlistPlayer,
    Session,
)
from discord_bots.utils import finished_game_str

_log = logging.getLogger(__name__)


class AdminCommands(BaseCog):
    def __init__(self, bot: Bot):
        super().__init__(bot)

    group = app_commands.Group(name="admin", description="Admin commands")

    @group.command(name="add", description="Add an admin")
    @app_commands.check(is_admin_app_command)
    @app_commands.check(is_command_channel)
    @app_commands.describe(member="Player to be made admin")
    async def addadmin(self, interaction: Interaction, member: Member):
        session: SQLAlchemySession
        with Session() as session:
            player: Player | None = (
                session.query(Player).filter(Player.id == member.id).first()
            )
            if not player:
                session.add(
                    Player(
                        id=member.id,
                        name=member.name,
                        is_admin=True,
                    )
                )
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"{escape_markdown(member.name)} added to admins",
                        colour=Colour.green(),
                    )
                )
                session.commit()
            else:
                if player.is_admin:
                    await interaction.response.send_message(
                        embed=Embed(
                            description=f"{escape_markdown(player.name)} is already an admin",
                            colour=Colour.red(),
                        )
                    )
                else:
                    player.is_admin = True
                    session.commit()
                    await interaction.response.send_message(
                        embed=Embed(
                            description=f"{escape_markdown(player.name)} added to admins",
                            colour=Colour.green(),
                        )
                    )

    @group.command(name="addrole", description="Add an admin role")
    @app_commands.check(is_admin_app_command)
    @app_commands.check(is_command_channel)
    @app_commands.describe(role="Role to be made admin")
    async def addadminrole(self, interaction: Interaction, role: Role):
        if interaction.guild:
            if role not in interaction.guild.roles:
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"Could not find role: {role.name}",
                        colour=Colour.blue(),
                    ),
                    ephemeral=True,
                )
                return

            session: SQLAlchemySession
            with Session() as session:
                admin_role: AdminRole | None = (
                    session.query(AdminRole)
                    .filter(AdminRole.role_id == role.id)
                    .first()
                )
                if admin_role:
                    await interaction.response.send_message(
                        embed=Embed(
                            description=f"**{role.name}** is already an admin", colour=Colour.yellow()
                        ),
                        ephemeral=True,
                    )
                else:
                    session.add(AdminRole(role.id))
                    session.commit()
                    await interaction.response.send_message(
                        embed=Embed(
                            description=f"Added admin role: {role.name}",
                            colour=Colour.green(),
                        )
                    )

    @group.command(name="ban", description="Bans player from queues")
    @app_commands.check(is_admin_app_command)
    @app_commands.check(is_command_channel)
    @app_commands.describe(member="Player to be banned")
    async def ban(self, interaction: Interaction, member: Member):
        """TODO: remove player from queues"""
        session: SQLAlchemySession
        with Session() as session:
            player: Player | None = session.query(Player).filter(Player.id == member.id).first()
            if not player:
                session.add(
                    Player(
                        id=member.id,
                        name=member.name,
                        is_banned=True,
                    )
                )
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"{escape_markdown(member.name)} banned",
                        colour=Colour.green(),
                    )
                )
                session.commit()
            else:
                if player.is_banned:
                    await interaction.response.send_message(
                        embed=Embed(
                            description=f"{escape_markdown(player.name)} is already banned",
                            colour=Colour.red(),
                        )
                    )
                else:
                    player.is_banned = True
                    session.commit()
                    await interaction.response.send_message(
                        embed=Embed(
                            description=f"{escape_markdown(player.name)} banned",
                            colour=Colour.green(),
                        )
                    )

    @group.command(
        name="configure", description="Initially configure the bot for this server"
    )
    @app_commands.check(is_admin_app_command)
    @app_commands.check(is_command_channel)
    @app_commands.guild_only()
    async def configure(self, interaction: Interaction):
        assert interaction.guild
        
        session: SQLAlchemySession
        with Session() as session:
            guild = (
                session.query(DiscordGuild)
                .filter(DiscordGuild.discord_id == interaction.guild.id)
                .first()
            )
            if guild:
                await interaction.response.send_message(
                    embed=Embed(
                        description="Server already configured",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
            else:
                guild = DiscordGuild(interaction.guild.id, interaction.guild.name)
                session.add(guild)
                session.commit()
                await interaction.response.send_message(
                    embed=Embed(
                        description="Server configured successfully!",
                        colour=Colour.green(),
                    ),
                    ephemeral=True,
                )

    @group.command(name="createcommand", description="Create a custom command")
    @app_commands.check(is_admin_app_command)
    @app_commands.check(is_command_channel)
    @app_commands.describe(name="Name of new command", output="Command output")
    async def createcommand(self, interaction: Interaction, name: str, *, output: str):
        session: SQLAlchemySession
        with Session() as session:
            exists = (
                session.query(CustomCommand).filter(CustomCommand.name == name).first()
            )
            if exists is not None:
                await interaction.response.send_message(
                    embed=Embed(
                        description="A command with that name already exists",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return

            session.add(CustomCommand(name, output))
            session.commit()

        await interaction.response.send_message(
            embed=Embed(description=f"Command `{name}` added", colour=Colour.green())
        )

    @group.command(
        name="createdbbackup", description="Creates a backup of the database"
    )
    @app_commands.check(is_admin_app_command)
    @app_commands.check(is_command_channel)
    async def createdbbackup(self, interaction: Interaction):
        # Only functions for SQLite
        # TODO: Covert to work for Postgres

        # Check for Postgres URI
        if config.DATABASE_URI:
            await interaction.response.send_message(
                embed=Embed(
                    description="This command does not support Postgres databases",
                    colour=Colour.yellow(),
                ),
                ephemeral=True
            )
            return
        
        date_string = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        copyfile(f"{config.DB_NAME}.db", f"{config.DB_NAME}_{date_string}.db")
        await interaction.response.send_message(
            embed=Embed(
                description=f"Backup made to {config.DB_NAME}_{date_string}.db",
                colour=Colour.green(),
            ),
            ephemeral=True,
        )

    @group.command(name="deletegame", description="Deletes a finished game")
    @app_commands.check(is_admin_app_command)
    @app_commands.check(is_command_channel)
    @app_commands.describe(game_id="Finished game id")
    async def deletegame(self, interaction: Interaction, game_id: str):
        session: SQLAlchemySession
        with Session() as session:
            finished_game: FinishedGame | None = (
                session.query(FinishedGame)
                .filter(FinishedGame.game_id.startswith(game_id))
                .first()
            )
            if not finished_game:
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"Could not find game: {game_id}",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return
            session.query(FinishedGamePlayer).filter(
                FinishedGamePlayer.finished_game_id == finished_game.id
            ).delete()
            session.delete(finished_game)
            session.commit()
            await interaction.response.send_message(
                embed=Embed(
                    description=f"Game: **{finished_game.game_id}** deleted",
                    colour=Colour.green(),
                )
            )

    @group.command(
        name="delplayer", description="Admin command to delete player from all queues"
    )
    @app_commands.check(is_admin_app_command)
    @app_commands.check(is_command_channel)
    @app_commands.describe(member="Player to be removed from queues")
    async def delplayer(
        self,
        interaction: Interaction,
        member: Member,
    ):
        """
        Admin command to delete player from all queues
        """
        session: SQLAlchemySession
        with Session() as session:
            queues: List[Queue] = (
                session.query(Queue)
                .join(QueuePlayer)
                .filter(QueuePlayer.player_id == member.id)
                .order_by(Queue.created_at.asc())
                .all()
            )  # type: ignore
            for queue in queues:
                session.query(QueuePlayer).filter(
                    QueuePlayer.queue_id == queue.id, QueuePlayer.player_id == member.id
                ).delete()
                # TODO: Test this part
                queue_waitlist: QueueWaitlist | None = (
                    session.query(QueueWaitlist)
                    .filter(
                        QueueWaitlist.queue_id == queue.id,
                    )
                    .first()
                )
                if queue_waitlist:
                    session.query(QueueWaitlistPlayer).filter(
                        QueueWaitlistPlayer.player_id == member.id,
                        QueueWaitlistPlayer.queue_waitlist_id == queue_waitlist.id,
                    ).delete()

            queue_statuses = []
            queue: Queue
            for queue in session.query(Queue).order_by(Queue.created_at.asc()).all():  # type: ignore
                queue_players = (
                    session.query(QueuePlayer)
                    .filter(QueuePlayer.queue_id == queue.id)
                    .all()
                )
                queue_statuses.append(
                    f"{queue.name} [{len(queue_players)}/{queue.size}]"
                )
            session.commit()

        await interaction.response.send_message(
            embed=Embed(
                title=f"{escape_markdown(member.name)} removed from: {', '.join([queue.name for queue in queues])}",
                description=" ".join(queue_statuses),
                colour=Colour.green(),
            )
        )

    @group.command(name="editcommand", description="Edit a custom command")
    @app_commands.check(is_admin_app_command)
    @app_commands.check(is_command_channel)
    @app_commands.describe(
        name="Name of existing custom command", output="New command output"
    )
    async def editcommand(self, interaction: Interaction, name: str, *, output: str):
        session: SQLAlchemySession
        with Session() as session:
            exists = (
                session.query(CustomCommand).filter(CustomCommand.name == name).first()
            )
            if exists is None:
                await interaction.response.send_message(
                    embed=Embed(
                        description="Could not find a command with that name",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return

            exists.output = output
            session.commit()

        await interaction.response.send_message(
            embed=Embed(
                description=f"Command `{name}` updated",
                colour=Colour.green(),
            )
        )

    @group.command(
        name="editgamewinner", description="Edit the winner of a finished game"
    )
    @app_commands.check(is_admin_app_command)
    @app_commands.check(is_command_channel)
    @app_commands.describe(game_id="Finished game id", outcome="Tie, BE, DS")
    async def editgamewinner(
        self,
        interaction: Interaction,
        game_id: str,
        outcome: Literal["Tie", "BE", "DS"],
    ):
        # TODO: Move away from BE/DS to Team0/Team1
        session: SQLAlchemySession
        with Session() as session:
            game: FinishedGame | None = (
                session.query(FinishedGame)
                .filter(FinishedGame.game_id.startswith(game_id))
                .first()
            )
            if not game:
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"Could not find game: {game_id}",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return
            outcome_lower = outcome.lower()
            if outcome_lower == "tie":
                game.winning_team = -1
            elif outcome_lower == "be":
                game.winning_team = 0
            elif outcome_lower == "ds":
                game.winning_team = 1
            else:
                await interaction.response.send_message(
                    embed=Embed(
                        description="Outcome must be tie, be, or ds",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return

            session.add(game)
            session.commit()
        await interaction.response.send_message(
            embed=Embed(
                description=f"Game {game_id} outcome changed:\n\n"
                + finished_game_str(game),
                colour=Colour.green(),
            )
        )

    @group.command(name="remove", description="Remove an admin")
    @app_commands.check(is_admin_app_command)
    @app_commands.check(is_command_channel)
    @app_commands.describe(member="Player to be removed as admin")
    async def removeadmin(self, interaction: Interaction, member: Member):
        session: SQLAlchemySession
        with Session() as session:
            player: Player | None = session.query(Player).filter(Player.id == member.id).first()
            if not player or not player.is_admin:
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"{escape_markdown(member.name)} is not an admin",
                        colour=Colour.red(),
                    )
                )
                return

            player.is_admin = False
            session.commit()
        await interaction.response.send_message(
            embed=Embed(
                description=f"{escape_markdown(member.name)} removed from admins",
                colour=Colour.green(),
            )
        )

    @group.command(name="removerole", description="Remove an admin role")
    @app_commands.check(is_admin_app_command)
    @app_commands.check(is_command_channel)
    @app_commands.guild_only()
    @app_commands.describe(role="Role to be removed as admin")
    async def removeadminrole(self, interaction: Interaction, role: Role):
        assert interaction.guild
        
        if role not in interaction.guild.roles:
            await interaction.response.send_message(
                embed=Embed(
                    description=f"Could not find role: {role.name}",
                    colour=Colour.blue(),
                ),
                ephemeral=True,
            )
            return

        session: SQLAlchemySession
        with Session() as session:
            admin_role = (
                session.query(AdminRole)
                .filter(AdminRole.role_id == role.id)
                .first()
            )
            if admin_role:
                session.delete(admin_role)
                session.commit()
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"Removed admin role: {role.name}",
                        colour=Colour.green(),
                    )
                )
            else:
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"Could not find admin role: {role.name}",
                        colour=Colour.blue(),
                    ),
                    ephemeral=True,
                )

    @group.command(name="removecommand", description="Remove a custom command")
    @app_commands.check(is_admin_app_command)
    @app_commands.check(is_command_channel)
    @app_commands.describe(name="Name of existing custom command")
    async def removecommand(self, interaction: Interaction, name: str):
        session: SQLAlchemySession
        with Session() as session:
            exists = (
                session.query(CustomCommand).filter(CustomCommand.name == name).first()
            )
            if not exists:
                await interaction.response.send_message(
                    embed=Embed(
                        description="Could not find command with that name",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return

            session.delete(exists)
            session.commit()

            await interaction.response.send_message(
                embed=Embed(
                    description=f"Command `{name}` removed",
                    colour=Colour.green(),
                )
            )

    @group.command(name="removedbbackup", description="Remove a database backup")
    @app_commands.check(is_admin_app_command)
    @app_commands.check(is_command_channel)
    @app_commands.describe(db_filename="Name of backup file")
    async def removedbbackup(self, interaction: Interaction, db_filename: str):
        # Only functions for SQLite
        # TODO: Covert to work for Postgres

        # Check for Postgres URI
        if config.DATABASE_URI:
            await interaction.response.send_message(
                embed=Embed(
                    description="This command does not support Postgres databases",
                    colour=Colour.yellow(),
                ),
                ephemeral=True
            )
            return

        
        if not db_filename.startswith(config.DB_NAME) or not db_filename.endswith(
            ".db"
        ):
            await interaction.response.send_message(
                embed=Embed(
                    description=f"Filename must be of the format {config.DB_NAME}_{{date}}.db",
                    colour=Colour.red(),
                ),
                ephemeral=True,
            )
            return

        try:
            os.remove(db_filename)
        except Exception as e:
            _log.exception(f"Caught Exception in removedbbackup: {e}")
            await interaction.response.send_message(
                embed=Embed(
                    description=f"Failed to remove DB Backup {db_filename}",
                    colour=Colour.red(),
                ),
                ephemeral=True,
            )
        else:
            await interaction.response.send_message(
                embed=Embed(
                    description=f"DB backup {db_filename} removed",
                    colour=Colour.green(),
                )
            )

    @group.command(name="restart", description="Restart the bot")
    @app_commands.check(is_admin_app_command)
    @app_commands.check(is_command_channel)
    async def restart(self, interaction: Interaction):
        await interaction.response.send_message(
            embed=Embed(
                description="Restarting bot...",
                colour=Colour.blue(),
            )
        )
        os.execv(sys.executable, ["python", "-m", "discord_bots.main"])

    @group.command(name="setbias", description="Set team bias")
    @app_commands.check(is_admin_app_command)
    @app_commands.check(is_command_channel)
    @app_commands.describe(member="Member to bias", amount="Bias value")
    async def setbias(self, interaction: Interaction, member: Member, amount: float):
        if amount < -100 or amount > 100:
            await interaction.response.send_message(
                embed=Embed(
                    description=f"Amount must be between -100 and 100",
                    colour=Colour.red(),
                ),
                ephemeral=True,
            )
            return
        await interaction.response.send_message(
            embed=Embed(
                description=f"Team bias for {member.name} set to `{amount}%`",
                colour=Colour.green(),
            )
        )

    @group.command(name="setcaptainbias", description="Set captain bias")
    @app_commands.check(is_admin_app_command)
    @app_commands.check(is_command_channel)
    @app_commands.describe(member="Member to bias", amount="Bias value")
    async def setcaptainbias(
        self, interaction: Interaction, member: Member, amount: float
    ):
        if amount < -100 or amount > 100:
            await interaction.response.send_message(
                embed=Embed(
                    description=f"Amount must be between -100 and 100",
                    colour=Colour.red(),
                ),
                ephemeral=True,
            )
            return
        await interaction.response.send_message(
            embed=Embed(
                description=f"Captain bias for {member.name} set to `{amount}%`",
                colour=Colour.green(),
            )
        )

    @group.command(
        name="setcommandprefix", description="Sets the prefix for context commands"
    )
    @app_commands.check(is_admin_app_command)
    @app_commands.check(is_command_channel)
    @app_commands.describe(prefix="New command prefix")
    async def setcommandprefix(self, interaction: Interaction, prefix: str):
        # TODO move to db-config
        global COMMAND_PREFIX
        COMMAND_PREFIX = prefix
        await interaction.response.send_message(
            embed=Embed(
                description=f"Command prefix set to {COMMAND_PREFIX}",
                colour=Colour.green(),
            )
        )

    @group.command(name="unban", description="Unban player")
    @app_commands.check(is_admin_app_command)
    @app_commands.check(is_command_channel)
    @app_commands.describe(member="Player to be unbanned")
    async def unban(self, interaction: Interaction, member: Member):
        session: SQLAlchemySession
        with Session() as session:
            player: Player | None = session.query(Player).filter(Player.id == member.id).first()
            if not player or not player.is_banned:
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"{escape_markdown(member.name)} is not banned",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return

            player.is_banned = False
            session.commit()
            await interaction.response.send_message(
                embed=Embed(
                    description=f"{escape_markdown(member.name)} unbanned",
                    colour=Colour.green(),
                )
            )

    @editcommand.autocomplete("name")
    @removecommand.autocomplete("name")
    async def command_autocomplete(self, interaction: Interaction, current: str):
        result = []
        session: SQLAlchemySession
        with Session() as session:
            commands: list[CustomCommand] | None = (
                session.query(CustomCommand)
                .order_by(CustomCommand.name)
                .limit(25)
                .all()
            )  # discord only supports up to 25 choices
            if commands:
                for command in commands:
                    if current in command.name:
                        result.append(
                            app_commands.Choice(name=command.name, value=command.name)
                        )
        return result
