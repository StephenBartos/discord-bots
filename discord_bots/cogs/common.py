import asyncio
import logging
from bisect import bisect
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from discord import Colour, Embed, Interaction, Message, TextChannel, app_commands
from discord.ext.commands import Bot
from sqlalchemy.orm.session import Session as SQLAlchemySession
from table2ascii import Alignment, PresetStyle, table2ascii
from trueskill import Rating

from discord_bots.bot import bot
from discord_bots.checks import is_command_channel
from discord_bots.cogs.base import BaseCog
from discord_bots.config import (
    SHOW_TRUESKILL,
)
from discord_bots.models import (
    Category,
    FinishedGame,
    FinishedGamePlayer,
    InProgressGame,
    InProgressGamePlayer,
    Player,
    PlayerCategoryTrueskill,
    Position,
    Session,
)
from discord_bots.utils import (
    MU_LOWER_UNICODE,
    SIGMA_LOWER_UNICODE,
    add_empty_field,
    category_autocomplete_with_user_id,
    code_block,
    get_guild_partial_message,
    send_in_guild_message,
    short_uuid,
)

_log = logging.getLogger(__name__)


class CommonCommands(BaseCog):
    def __init__(self, bot: Bot):
        super().__init__(bot)


    @app_commands.command(
        name="setgamecode", description="Sets lobby code for your current game"
    )
    @app_commands.check(is_command_channel)
    @app_commands.guild_only()
    @app_commands.describe(code="Game lobby code")
    async def setgamecode(self, interaction: Interaction, code: str):
        assert interaction.guild
        session: SQLAlchemySession
        with Session() as session:
            ipgp = (
                session.query(InProgressGamePlayer)
                .filter(InProgressGamePlayer.player_id == interaction.user.id)
                .first()
            )
            if not ipgp:
                await interaction.response.send_message(
                    embed=Embed(
                        description="You must be in game to set the game code!",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return

            ipg = (
                session.query(InProgressGame)
                .filter(InProgressGame.id == ipgp.in_progress_game_id)
                .first()
            )
            if not ipg:
                await interaction.response.send_message(
                    embed=Embed(
                        description="You must be in game to set the game code!",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return

            if ipg.code == code:
                await interaction.response.send_message(
                    embed=Embed(
                        description="This is already the current game code!",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
                return
            ipg.code = code
            await interaction.response.defer(ephemeral=True)
            title: str = f"Lobby code for ({short_uuid(ipg.id)})"
            if ipg.channel_id and ipg.message_id:
                # Update the match channel's in_progress_game embed with the game code
                partial_message = get_guild_partial_message(
                    interaction.guild, ipg.channel_id, ipg.message_id
                )
                channel = interaction.guild.get_channel(ipg.channel_id)
                if isinstance(channel, TextChannel):
                    try:
                        message: Message = await channel.fetch_message(ipg.message_id)
                        if len(message.embeds) > 0:
                            embed: Embed = message.embeds[0]
                            replaced_code = False
                            for i, field in enumerate(embed.fields):
                                if field.name == "🔢 Game Code":
                                    field.value = code_block(code, language="yaml")
                                    embed.set_field_at(
                                        i,
                                        name="🔢 Game Code",
                                        value=code_block(code, language="yaml"),
                                        inline=True,
                                    )
                                    replaced_code = True
                                    break
                            if not replaced_code:
                                last = embed.fields[-1]
                                if (
                                    last.name == ""
                                    and last.value == ""
                                    and last.inline == True
                                ):
                                    embed.remove_field(-1)
                                embed.add_field(
                                    name="🔢 Game Code",
                                    value=code_block(code, language="yaml"),
                                    inline=True,
                                )
                                add_empty_field(embed, offset=3)
                            await message.edit(embed=embed)
                    except:
                        _log.exception(
                            f"[setgamecode] Failed to get message with guild_id={interaction.guild_id}, channel_id={ipg.channel_id}, message_id={ipg.message_id}:"
                        )
                if partial_message:
                    title = f"Lobby code for {partial_message.jump_url}"

            embed = Embed(
                title=title,
                description=code_block(code, language="yaml"),
                colour=Colour.green(),
            )
            embed.set_footer(
                text=f"set by {interaction.user.display_name} ({interaction.user.name})"
            )
            coroutines = []
            result = (
                session.query(InProgressGamePlayer.player_id)
                .filter(
                    InProgressGamePlayer.in_progress_game_id == ipg.id,
                    InProgressGamePlayer.player_id
                    != interaction.user.id,  # don't send the code to the one who wants to send it out
                )
                .all()
            )
            ipg_player_ids: list[int] = (
                [player_id[0] for player_id in result if player_id] if result else []
            )
            for player_id in ipg_player_ids:
                coroutines.append(
                    send_in_guild_message(interaction.guild, player_id, embed=embed)
                )
            if ipg_player_ids:
                try:
                    await asyncio.gather(*coroutines)
                except:
                    _log.exception(
                        "[setgamecode] Ignoring exception in asyncio.gather:"
                    )
                else:
                    await interaction.followup.send(
                        embed=Embed(
                            description="Lobby code sent to each player",
                            colour=Colour.blue(),
                        ),
                        ephemeral=True,
                    )
            else:
                _log.warn("No in_progress_game_players to send a lobby code to")
                await interaction.followup.send(
                    embed=Embed(
                        description="There are no in-game players to send this lobby code to!",
                        colour=Colour.red(),
                    ),
                    ephemeral=True,
                )
            session.commit()

    @app_commands.command(
        name="stats", description="Privately displays your TrueSkill statistics"
    )
    @app_commands.check(is_command_channel)
    @app_commands.describe(category_name="Category to show stats for")
    @app_commands.autocomplete(category_name=category_autocomplete_with_user_id)
    @app_commands.rename(category_name="category")
    async def stats(
        self, interaction: Interaction, category_name: Optional[str] | None
    ):
        """
        Replies to the user with their TrueSkill statistics. Can be used both inside and out of a Guild
        """
        session: SQLAlchemySession
        with Session() as session:
            config = session.query(Config).first()
            player: Player | None = (
                session.query(Player).filter(Player.id == interaction.user.id).first()
            )
            if not player:
                # Edge case where user has no record in the Players table
                await interaction.response.send_message(
                    embed=Embed(
                        description="You have not played any games",
                        colour=Colour.blue(),
                    ),
                    ephemeral=True,
                )
                return
            if not player.stats_enabled:
                await interaction.response.send_message(
                    embed=Embed(
                        description="You have disabled `/stats`",
                        colour=Colour.blue(),
                    ),
                    ephemeral=True,
                )
                return

            fgps: List[FinishedGamePlayer] | None = (
                session.query(FinishedGamePlayer)
                .filter(FinishedGamePlayer.player_id == player.id)
                .all()
            )
            if not fgps:
                await interaction.response.send_message(
                    embed=Embed(
                        description="You have not played any games",
                        colour=Colour.blue(),
                    ),
                    ephemeral=True,
                )
                return

            finished_game_ids: List[str] | None = [fgp.finished_game_id for fgp in fgps]
            fgs: List[FinishedGame] | None = (
                session.query(FinishedGame)
                .filter(FinishedGame.id.in_(finished_game_ids))
                .all()
            )
            if not fgs:
                await interaction.response.send_message(
                    embed=Embed(
                        description="You have not played any games",
                        colour=Colour.blue(),
                    ),
                    ephemeral=True,
                )
                session.close()
                return

            fgps_by_finished_game_id: dict[str, FinishedGamePlayer] = {
                fgp.finished_game_id: fgp for fgp in fgps
            }

            players: list[Player] = session.query(Player).all()

            default_rating = Rating()
            # Filter players that haven't played a game
            players = list(
                filter(
                    lambda x: (
                        x.rated_trueskill_mu != default_rating.mu
                        and x.rated_trueskill_sigma != default_rating.sigma
                    )
                    and (
                        x.rated_trueskill_mu != config.default_trueskill_mu
                        and x.rated_trueskill_sigma != config.default_trueskill_sigma
                    ),
                    players,
                )
            )
            trueskills = list(
                sorted(
                    [
                        round(p.rated_trueskill_mu - 3 * p.rated_trueskill_sigma, 2)
                        for p in players
                    ]
                )
            )
            trueskill_index = bisect(
                trueskills,
                round(player.rated_trueskill_mu - 3 * player.rated_trueskill_sigma, 2),
            )
            trueskill_ratio = (len(trueskills) - trueskill_index) / (
                len(trueskills) or 1
            )
            if trueskill_ratio <= 0.05:
                trueskill_pct = "Top 5%"
            elif trueskill_ratio <= 0.10:
                trueskill_pct = "Top 10%"
            elif trueskill_ratio <= 0.25:
                trueskill_pct = "Top 25%"
            elif trueskill_ratio <= 0.50:
                trueskill_pct = "Top 50%"
            elif trueskill_ratio <= 0.75:
                trueskill_pct = "Top 75%"
            else:
                trueskill_pct = "Top 100%"

            # all of this below can probably be done more gracefull with a pandas dataframe
            def wins_losses_ties_last_ndays(
                finished_games: List[FinishedGame], n: int = -1
            ) -> tuple[list[FinishedGame], list[FinishedGame], list[FinishedGame]]:
                if n == -1:
                    # all finished games
                    last_nfgs = finished_games
                else:
                    # last n
                    last_nfgs = [
                        fg
                        for fg in finished_games
                        if fg.finished_at.replace(tzinfo=timezone.utc)
                        > datetime.now(timezone.utc) - timedelta(days=n)
                    ]
                wins = [
                    fg
                    for fg in last_nfgs
                    if fg.winning_team == fgps_by_finished_game_id[fg.id].team
                ]
                losses = [
                    fg
                    for fg in last_nfgs
                    if fg.winning_team != fgps_by_finished_game_id[fg.id].team
                    and fg.winning_team != -1
                ]
                ties = [fg for fg in last_nfgs if fg.winning_team == -1]
                return wins, losses, ties

            def win_rate(wins, losses, ties):
                denominator = max(wins + losses + ties, 1)
                return round(100 * (wins + 0.5 * ties) / denominator, 1)

            def get_table_col(games: List[FinishedGame]):
                cols = []
                for num_days in [7, 30, 90, 365, -1]:
                    wins, losses, ties = wins_losses_ties_last_ndays(games, num_days)
                    num_wins, num_losses, num_ties = len(wins), len(losses), len(ties)
                    winrate = round(win_rate(num_wins, num_losses, num_ties))
                    col = [
                        "Total" if num_days == -1 else f"{num_days}D",
                        len(wins),
                        len(losses),
                        len(ties),
                        num_wins + num_losses + num_ties,
                        f"{winrate}%",
                    ]
                    cols.append(col)
                return cols

            message_content = ""  # TODO: temp fix
            footer_text = f"-# Rating = {MU_LOWER_UNICODE} - 3*{SIGMA_LOWER_UNICODE}"
            cols = []
            conditions = []
            conditions.append(PlayerCategoryTrueskill.player_id == player.id)
            if category_name:
                conditions.append(Category.name == category_name)
            player_category_trueskills: list[PlayerCategoryTrueskill] | None = (
                session.query(PlayerCategoryTrueskill)
                .join(Category)
                .filter(*conditions)
                .order_by(Category.name)
                .all()
            )
            player_category_trueskills_by_category = defaultdict(list)
            for pct in player_category_trueskills:
                player_category_trueskills_by_category[pct.category_id].append(pct)

            # assume that if a guild uses categories, they will use them exclusively, i.e., no mixing categorized and uncategorized queues
            if player_category_trueskills:
                for i_pct, (category_id, pcts) in enumerate(
                    player_category_trueskills_by_category.items()
                ):
                    non_position_pcts = filter(
                        lambda x: x.position_id is None,
                        pcts,
                    )
                    position_pcts = filter(
                        lambda x: x.position_id is not None,
                        pcts,
                    )
                    # Order the non-position trueskill first
                    pcts = list(non_position_pcts) + list(position_pcts)
                    category: Category | None = (
                        session.query(Category)
                        .filter(Category.id == category_id)
                        .first()
                    )
                    if not category:
                        # should never happen
                        _log.error(
                            f"No Category found for player_category_trueskill with id {pct.id}"
                        )
                        await interaction.response.send_message(
                            embed=Embed(description="Could not find your stats")
                        )
                        return

                    # Don't show stats for unrated categories
                    if not category.is_rated:
                        continue

                    for pct in pcts:
                        if pct.position_id:
                            position = (
                                session.query(Position)
                                .filter(Position.id == pct.position_id)
                                .first()
                            )
                            title = (
                                f"TrueSkill for {category.name} ({position.short_name})"
                            )
                            category_games = (
                                session.query(FinishedGame)
                                .join(FinishedGamePlayer)
                                .filter(
                                    FinishedGame.category_name == category.name,
                                    FinishedGamePlayer.position_name
                                    == position.short_name,
                                    FinishedGamePlayer.player_id == player.id,
                                )
                                .all()
                            )
                        else:
                            title = f"TrueSkill for {category.name}"
                            category_games = (
                                session.query(FinishedGame)
                                .join(FinishedGamePlayer)
                                .filter(
                                    FinishedGame.category_name == category.name,
                                    FinishedGamePlayer.player_id == player.id,
                                    FinishedGamePlayer.position_name == None,
                                )
                                .all()
                            )
                        if category.is_rated and SHOW_TRUESKILL:
                            description = (
                                f"Rating: **{round(pct.rank, 1)}**"
                                f" `{MU_LOWER_UNICODE}: {round(pct.mu, 1)}`, "
                                f"`{SIGMA_LOWER_UNICODE}: {round(pct.sigma, 1)}` "
                            )
                        else:
                            description = f"Rating: {trueskill_pct}"

                        message_content += f"\n{title}\n{description}"  # TODO: temp fix

                        cols = get_table_col(category_games)
                        table = table2ascii(
                            header=["Last", "W", "L", "T", "Total", "WR"],
                            body=cols,
                            first_col_heading=True,
                            style=PresetStyle.plain,
                            alignments=[
                                Alignment.RIGHT,
                                Alignment.DECIMAL,
                                Alignment.DECIMAL,
                                Alignment.DECIMAL,
                                Alignment.DECIMAL,
                                Alignment.RIGHT,
                            ],
                        )
                        description = code_block(table)
                        message_content += f"\n{description}"  # TODO: temp fix
                    if i_pct < (len(player_category_trueskills_by_category) - 1):
                        message_content += f"\n{footer_text}"

            else:
                # no categories defined, display their global trueskill stats
                description = ""
                if SHOW_TRUESKILL:
                    rank = player.rated_trueskill_mu - 3 * player.rated_trueskill_sigma
                    description = (
                        f"Rating: **{round(rank, 1)}**"
                        f" `{MU_LOWER_UNICODE}: {round(player.rated_trueskill_mu, 1)}`, "
                        f"`{SIGMA_LOWER_UNICODE}: {round(player.rated_trueskill_sigma, 1)}` "
                    )
                else:
                    description = f"Rating: {trueskill_pct}"
                cols = get_table_col(fgs)
                table = table2ascii(
                    header=["Period", "Wins", "Losses", "Ties", "Total", "Win %"],
                    body=cols,
                    first_col_heading=True,
                    style=PresetStyle.plain,
                    alignments=[
                        Alignment.LEFT,
                        Alignment.DECIMAL,
                        Alignment.DECIMAL,
                        Alignment.DECIMAL,
                        Alignment.DECIMAL,
                        Alignment.DECIMAL,
                    ],
                )
                description += code_block(table)
                message_content = (
                    f"Overall Stats\n{description}\n{footer_text}"  # TODO: temp fix
                )
            try:
                await interaction.response.send_message(
                    content=message_content, ephemeral=True
                )
            except Exception:
                _log.exception(f"Caught exception trying to send stats message")
