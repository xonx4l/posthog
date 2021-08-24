from uuid import uuid4

from ee.clickhouse.client import sync_execute
from ee.clickhouse.models.event import create_event
from ee.clickhouse.util import ClickhouseTestMixin
from posthog.api.test.test_person import factory_test_person
from posthog.models import Event, Person


def _create_event(**kwargs):
    kwargs.update({"event_uuid": uuid4()})
    return Event(pk=create_event(**kwargs))


def _get_events(team_id):
    return sync_execute("select FINAL * from events where team_id = %(team_id)s", {"team_id": team_id})


def _create_person(**kwargs):
    return Person.objects.create(**kwargs)


class ClickhouseTestPersonApi(
    ClickhouseTestMixin, factory_test_person(_create_event, _create_person, _get_events)  # type: ignore
):
    pass
