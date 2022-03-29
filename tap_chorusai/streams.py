"""Stream type classes for tap-chorusai."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th

from tap_chorusai.client import ChorusaiStream

from pychorusai import chorusai
from datetime import datetime, timezone


class EngagementsStream(ChorusaiStream):
    name = "engagements"
    primary_keys = ["engagement_id"]
    replication_key = "date_time"
    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property(
            "account_id",
            th.StringType,
            description="Account ID of engagementparticipant"
        ),
        th.Property(
            "account_name",
            th.StringType,
            description="Account Name of engagement participant"
        ),
        th.Property(
            "compliance",
            th.StringType,
            description="Compliance details"
        ),
        th.Property("date_time", th.DateTimeType),
        th.Property("disposition_connected", th.StringType),
        th.Property("disposition_gatekeeper", th.StringType),
        th.Property("disposition_tree", th.StringType),
        th.Property("disposition_voicemail", th.StringType),
        th.Property("duration", th.NumberType),
        th.Property("engagement_id", th.StringType),
        th.Property("engagement_type", th.StringType),
        th.Property("initiator", th.StringType),
        th.Property("language", th.StringType),
        th.Property("no_show", th.BooleanType),
        th.Property("num_cust_questions", th.IntegerType),
        th.Property("num_engaging_questions", th.IntegerType),
        th.Property("opportunity_id", th.StringType),
        th.Property("opportunity_name", th.StringType),
        th.Property(
            "participants",
            th.ArrayType(
                th.ObjectType(
                    th.Property("company_name", th.StringType),
                    th.Property("email", th.StringType),
                    th.Property("name", th.StringType),
                    th.Property("person_id", th.StringType),
                    th.Property("title", th.StringType),
                    th.Property("type", th.StringType),
                    th.Property("user_id", th.StringType),
                )
            )
        ),
        th.Property("processing_state", th.StringType),
        th.Property("subject", th.StringType),
        th.Property(
            "tracker_matches",
            th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType),
                    th.Property("num_matches", th.IntegerType),
                    th.Property("type", th.StringType),
                )
            )
        ),
        th.Property("url", th.StringType),
        th.Property("user_email", th.StringType),
        th.Property("user_id", th.StringType),
        th.Property("user_name", th.StringType),
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        starting_timestamp = self.get_starting_timestamp(context)
        for resp in self.api.getEngagements(min_date=starting_timestamp, with_trackers=True):
            for row in resp:
                row['date_time'] = datetime.fromtimestamp(
                    row['date_time'], tz=timezone.utc).isoformat()
                yield row


class ScorecardsStream(ChorusaiStream):
    name = "scorecards"
    primary_keys = ["id"]
    replication_key = "submitted"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("type", th.StringType),
        th.Property("submitted", th.DateTimeType),
        th.Property("url", th.StringType),
        th.Property("seen", th.BooleanType),
        th.Property("engagement", th.StringType),
        th.Property(
            "initiative",
            th.ObjectType(
                th.Property("description", th.StringType),
                th.Property("id", th.StringType),
                th.Property("name", th.StringType),
                th.Property("url", th.StringType),
            )
        ),
        th.Property(
            "recipient",
            th.ObjectType(
                th.Property("name", th.StringType),
                th.Property("person_id", th.StringType),
                th.Property("user_id", th.StringType),
            )
        ),
        th.Property(
            "reviewer",
            th.ObjectType(
                th.Property("name", th.StringType),
                th.Property("person_id", th.StringType),
                th.Property("user_id", th.StringType),
            )
        ),
        th.Property(
            "scores",
            th.ArrayType(
                th.ObjectType(
                    th.Property(
                        "comment",
                        th.ObjectType(
                            th.Property("text", th.StringType),
                        )
                    ),
                    th.Property(
                        "question",
                        th.ObjectType(
                            th.Property("id", th.StringType),
                            th.Property("text", th.StringType),
                        )
                    ),
                    th.Property("score", th.IntegerType),
                ),
            )
        ),
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        starting_timestamp = self.get_starting_timestamp(context)
        for resp in self.api.getScorecards(min_date=starting_timestamp):
            for row in resp:
                yield row
                

class EmailsStream(ChorusaiStream):
    name = "emails"
    primary_keys = ["id"]
    replication_key = "sent_time"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("type", th.StringType),
        th.Property("status", th.StringType),
        th.Property("sent_time", th.DateTimeType),
        th.Property("name", th.StringType),
        th.Property("private", th.BooleanType),
        th.Property("company_name", th.StringType),
        th.Property(
            "account",
            th.ObjectType(
                th.Property("ext_id", th.StringType),
                th.Property("id", th.StringType),
                th.Property("name", th.StringType),
                th.Property("type", th.StringType),
            )
        ),
        th.Property(
            "email",
            th.ObjectType(
                th.Property("body", th.StringType),
                th.Property(
                    "initiator",
                    th.ObjectType(
                        th.Property('email', th.StringType),
                        th.Property("name", th.StringType),
                    )
                ),
                th.Property("thread", th.StringType),
            )
        ),
        th.Property(
            "deal",
            th.ObjectType(
                th.Property("close_date", th.DateTimeType),
                th.Property("current_stage", th.StringType),
                th.Property("id", th.StringType),
                th.Property("initial_stage", th.StringType),
                th.Property("name", th.StringType),
                th.Property("on_stage_since", th.DateTimeType),
                th.Property("size", th.StringType),
            )
        ),
        th.Property(
            "owner",
            th.ObjectType(
                th.Property('email', th.StringType),
                th.Property("name", th.StringType),
                th.Property("person_id", th.StringType),
                th.Property("user_id", th.StringType),
            )
        ),
        th.Property(
            "participants",
            th.ArrayType(
                th.ObjectType(
                    th.Property("company_name", th.StringType),
                    th.Property("email", th.StringType),
                    th.Property("name", th.StringType),
                    th.Property("person_id", th.StringType),
                    th.Property("title", th.StringType),
                    th.Property("type", th.StringType),
                    th.Property("user_id", th.StringType),
                )
            )
        ),
    ).to_dict()
    
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        starting_timestamp = self.get_starting_timestamp(context)
        for resp in self.api.getEmails(min_date=starting_timestamp):
            for row in resp:
                row['sent_time'] = row.get('email').get('sent_time')
                yield row
