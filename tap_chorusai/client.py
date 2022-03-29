"""Custom client handling, including ChorusaiStream base class."""

import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk.streams import Stream
from singer_sdk.tap_base import Tap

from pychorusai import chorusai


class ChorusaiStream(Stream):
    """Stream class for Chorus.ai streams."""
    
    def __init__(self, tap: Tap):
        super().__init__(tap)
        self.api = chorusai(self.config.get("auth_token"))

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.
        """
        # TODO: Write logic to extract data from the upstream source.
        # rows = mysource.getall()
        # for row in rows:
        #     yield row.to_dict()
        raise NotImplementedError("The method is not yet implemented (TODO)")
