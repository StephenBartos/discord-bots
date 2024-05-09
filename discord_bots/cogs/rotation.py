import logging
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.session import Session as SQLAlchemySession

from discord import (
    app_commands,
    Colour,
    Embed,
    Interaction,
)
from discord.ext.commands import Bot, Context, check, command

from discord_bots.checks import is_admin_app_command, is_command_channel
from discord_bots.cogs.base import BaseCog
from discord_bots.models import Map, Queue, Rotation, RotationMap, Session
from discord_bots.utils import update_next_map_to_map_after_next

_log = logging.getLogger(__name__)


class RotationCommands(BaseCog):
    def __init__(self, bot: Bot):
        super().__init__(bot)

    group = app_commands.Group(name="rotation", description="Rotation commands")

    @group.command(name="add", description="Add a rotation to the rotation pool")
    @app_commands.check(is_admin_app_command)
    @app_commands.check(is_command_channel)
    @app_commands.describe(rotation_name="Existing rotation")
    async def addrotation(self, interaction: Interaction, rotation_name: str):
        """
        Add a rotation to the rotation pool
        """
        session: SQLAlchemySession
        with Session() as session:
            try:
                session.add(Rotation(name=rotation_name))
                session.commit()
            except IntegrityError:
                session.rollback()
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"Error adding rotation {rotation_name}). Does it already exist?",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
            else:
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"Rotation **{rotation_name}** added",
                        colour=Colour.green(),
                    )
                )

    @group.command(
        name="addmap",
        description="Add a map to a rotation at a specific ordinal (position)",
    )
    @app_commands.check(is_admin_app_command)
    @app_commands.check(is_command_channel)
    @app_commands.describe(
        rotation_name="Existing rotation",
        map_short_name="Existing map",
        ordinal="Map ordinal",
    )
    async def addrotationmap(
        self,
        interaction: Interaction,
        rotation_name: str,
        map_short_name: str,
        ordinal: int,
    ):
        """
        Add a map to a rotation at a specific ordinal (position)
        """
        if ordinal < 1:
            await interaction.response.send_message(
                embed=Embed(
                    description="Ordinal must be a positive number",
                    colour=Colour.red(),
                ),
                ephemeral=True,
            )
            return

        session: SQLAlchemySession
        with Session() as session:
            try:
                rotation = (
                    session.query(Rotation)
                    .filter(Rotation.name.ilike(rotation_name))
                    .one()
                )
            except NoResultFound:
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"Could not find rotation **{rotation_name}**",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return

            try:
                map = (
                    session.query(Map)
                    .filter(Map.short_name.ilike(map_short_name))
                    .one()
                )
            except NoResultFound:
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"Could not find map **{map_short_name}**",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return

            # logic for organizing ordinals.  ordinals are kept unique and consecutive.
            # we insert a map directly at an ordinal and increment every one after that.
            rotation_maps = (
                session.query(RotationMap)
                .join(Rotation, RotationMap.rotation_id == rotation.id)
                .order_by(RotationMap.ordinal.asc())
                .all()
            )

            if ordinal > len(rotation_maps):
                ordinal = len(rotation_maps) + 1
            else:
                for rotation_map in rotation_maps[ordinal - 1 :]:
                    rotation_map.ordinal += 1

            is_next = True if not rotation_maps else False

            try:
                session.add(
                    RotationMap(
                        rotation_id=rotation.id,
                        map_id=map.id,
                        ordinal=ordinal,
                        is_next=is_next,
                    )
                )
                session.commit()
            except IntegrityError:
                session.rollback()
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"Error adding {map_short_name} to {rotation_name} at ordinal {ordinal}",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
            else:
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"{map.short_name} added to {rotation.name} at ordinal {ordinal}",
                        colour=Colour.green(),
                    )
                )

    @group.command(
        name="remove", description="Remove a rotation from the rotation pool"
    )
    @app_commands.check(is_admin_app_command)
    @app_commands.check(is_command_channel)
    @app_commands.describe(rotation_name="Existing rotation")
    async def removerotation(self, interaction: Interaction, rotation_name: str):
        """
        Remove a rotation from the rotation pool
        TODO: Add confirmation for deleting rotation still associated with a queue
        """
        session: SQLAlchemySession
        with Session() as session:
            try:
                rotation = (
                    session.query(Rotation)
                    .filter(Rotation.name.ilike(rotation_name))
                    .one()
                )
            except NoResultFound:
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"Could not find rotation **{rotation_name}**",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return

            session.delete(rotation)
            session.commit()
            await interaction.response.send_message(
                embed=Embed(
                    description=f"Rotation **{rotation.name}** removed",
                    colour=Colour.green(),
                )
            )

    @group.command(name="removemap", description="Remove a map from a rotation")
    @app_commands.check(is_admin_app_command)
    @app_commands.check(is_command_channel)
    @app_commands.describe(
        rotation_name="Existing rotation", map_short_name="Existing map"
    )
    async def removerotationmap(
        self, interaction: Interaction, rotation_name: str, map_short_name: str
    ):
        """
        Remove a map from a rotation
        """
        session: SQLAlchemySession
        with Session() as session:
            try:
                rotation = (
                    session.query(Rotation)
                    .filter(Rotation.name.ilike(rotation_name))
                    .one()
                )
            except NoResultFound:
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"Could not find rotation **{rotation_name}**",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return

            try:
                map = (
                    session.query(Map)
                    .filter(Map.short_name.ilike(map_short_name))
                    .one()
                )
            except NoResultFound:
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"Could not find map **{map_short_name}**",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return

            rotation_map = (
                session.query(RotationMap)
                .filter(
                    rotation.id == RotationMap.rotation_id, map.id == RotationMap.map_id
                )
                .first()
            )
            if not rotation_map:
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"Could not find map **{map.short_name}** in rotation **{rotation.name}**",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return

            if rotation_map.is_next:
                await update_next_map_to_map_after_next(rotation.id, True)

            # adjust the rest of the ordinals in the rotation
            rotation_maps = (
                session.query(RotationMap)
                .filter(RotationMap.rotation_id == rotation.id)
                .order_by(RotationMap.ordinal.asc())
                .all()
            )
            for entry in rotation_maps[rotation_map.ordinal :]:
                entry.ordinal -= 1

            session.delete(rotation_map)
            session.commit()
            await interaction.response.send_message(
                embed=Embed(
                    description=f"**{map.short_name}** removed from rotation **{rotation.name}**",
                    colour=Colour.green(),
                )
            )

    @group.command(
        name="setmapordinal",
        description="Set the ordinal (position) for a map in a rotation",
    )
    @app_commands.check(is_admin_app_command)
    @app_commands.check(is_command_channel)
    @app_commands.describe(
        rotation_name="Existing rotation",
        map_short_name="Existing map",
        new_ordinal="New map ordinal",
    )
    async def setrotationmapordinal(
        self,
        interaction: Interaction,
        rotation_name: str,
        map_short_name: str,
        new_ordinal: int,
    ):
        """
        Set the ordinal (position) for a map in a rotation
        """
        session: SQLAlchemySession
        with Session() as session:
            if new_ordinal < 1:
                await interaction.response.send_message(
                    embed=Embed(
                        description="Ordinal must be a positive number",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return

            try:
                rotation = (
                    session.query(Rotation)
                    .filter(Rotation.name.ilike(rotation_name))
                    .one()
                )
            except NoResultFound:
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"Could not find rotation **{rotation_name}**",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return

            try:
                map = (
                    session.query(Map)
                    .filter(Map.short_name.ilike(map_short_name))
                    .one()
                )
            except NoResultFound:
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"Could not find map **{map_short_name}**",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return

            try:
                rotation_map_to_set = (
                    session.query(RotationMap)
                    .filter(RotationMap.rotation_id == rotation.id)
                    .filter(RotationMap.map_id == map.id)
                    .one()
                )
            except NoResultFound:
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"Map **{map.short_name}** is not in rotation **{rotation.name}**\nPlease add it with `!addrotationmap`.",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return

            # logic for organizing ordinals.  ordinals are kept unique and consecutive.
            current_ordinal = rotation_map_to_set.ordinal

            if new_ordinal == current_ordinal:
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"Map **{map.short_name}** in rotation **{rotation.name}** is already set to ordinal **{new_ordinal}**.",
                        colour=Colour.blue(),
                    )
                )
                return

            rotation_maps = (
                session.query(RotationMap)
                .join(Rotation, RotationMap.rotation_id == rotation.id)
                .order_by(RotationMap.ordinal.asc())
                .all()
            )

            if new_ordinal > len(rotation_maps):
                new_ordinal = len(rotation_maps)

            if new_ordinal > current_ordinal:
                for rotation_map in rotation_maps[current_ordinal:new_ordinal]:
                    rotation_map.ordinal -= 1
            else:
                for rotation_map in rotation_maps[
                    new_ordinal - 1 : current_ordinal - 1
                ]:
                    rotation_map.ordinal += 1

            rotation_map_to_set.ordinal = new_ordinal

            session.commit()

            await interaction.response.send_message(
                embed=Embed(
                    description=f"Map **{map.short_name}** in rotation **{rotation.name}** set to ordinal **{rotation_map_to_set.ordinal}**.",
                    colour=Colour.green(),
                )
            )

    @group.command(name="setname", description="Set rotation name")
    @app_commands.check(is_admin_app_command)
    @app_commands.check(is_command_channel)
    @app_commands.describe(
        old_rotation_name="Existing rotation", new_rotation_name="New rotation name"
    )
    async def setrotationname(
        self, interaction: Interaction, old_rotation_name: str, new_rotation_name: str
    ):
        """
        Set rotation name
        """
        await self.setname(interaction, Rotation, old_rotation_name, new_rotation_name)

    @group.command(name="setrandom", description="Chooses rotation's maps at random")
    @app_commands.check(is_admin_app_command)
    @app_commands.check(is_command_channel)
    @app_commands.describe(rotation_name="Existing rotation")
    async def setrotationrandom(self, interaction: Interaction, rotation_name: str):
        """
        Chooses rotation's maps at random
        """
        session: SQLAlchemySession
        with Session() as session:
            rotation: Rotation | None = (
                session.query(Rotation)
                .filter(Rotation.name.ilike(rotation_name))
                .first()
            )
            if not rotation:
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"Could not find rotation **{rotation_name}**",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return

            rotation.is_random = True
            session.commit()
            await interaction.response.send_message(
                embed=Embed(
                    description=f"**{rotation.name}** rotation set to random",
                    colour=Colour.green(),
                )
            )

    @group.command(name="unsetrandom", description="Chooses rotation's maps in order")
    @app_commands.check(is_admin_app_command)
    @app_commands.check(is_command_channel)
    @app_commands.describe(rotation_name="Existing rotation")
    async def unsetrotationrandom(self, interaction: Interaction, rotation_name: str):
        """
        Chooses rotation's maps in order
        """
        session: SQLAlchemySession
        with Session() as session:
            rotation: Rotation | None = (
                session.query(Rotation)
                .filter(Rotation.name.ilike(rotation_name))
                .first()
            )
            if not rotation:
                await interaction.response.send_message(
                    embed=Embed(
                        description=f"Could not find rotation **{rotation_name}**",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return

            rotation.is_random = False
            session.commit()
            await interaction.response.send_message(
                embed=Embed(
                    description=f"**{rotation.name}** rotation unset from random",
                    colour=Colour.green(),
                )
            )

    @addrotationmap.autocomplete("map_short_name")
    @removerotationmap.autocomplete("map_short_name")
    @setrotationmapordinal.autocomplete("map_short_name")
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

    @addrotation.autocomplete("rotation_name")
    @addrotationmap.autocomplete("rotation_name")
    @removerotation.autocomplete("rotation_name")
    @removerotationmap.autocomplete("rotation_name")
    @setrotationmapordinal.autocomplete("rotation_name")
    @setrotationname.autocomplete("old_rotation_name")
    @setrotationrandom.autocomplete("rotation_name")
    @unsetrotationrandom.autocomplete("rotation_name")
    async def rotation_autocomplete(self, interaction: Interaction, current: str):
        result = []
        session: SQLAlchemySession
        with Session() as session:
            rotations: list[Rotation] | None = (
                session.query(Rotation).order_by(Rotation.name).limit(25).all()
            )
            if rotations:
                for rotation in rotations:
                    if current in rotation.name:
                        result.append(
                            app_commands.Choice(name=rotation.name, value=rotation.name)
                        )
        return result
