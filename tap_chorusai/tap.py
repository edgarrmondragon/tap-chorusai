"""Chorus.ai tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
from tap_chorusai.streams import (
    ChorusaiStream,
    EngagementsStream,
    ScorecardsStream,
    EmailsStream,
)

STREAM_TYPES = [
    EngagementsStream,
    ScorecardsStream,
    EmailsStream,
]


class TapChorusai(Tap):
    """Chorus.ai tap class."""
    name = "tap-chorusai"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_token",
            th.StringType,
            required=True,
            description="The token to authenticate against the API service"
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=True,
            description="Start date if no bookmark available."
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
