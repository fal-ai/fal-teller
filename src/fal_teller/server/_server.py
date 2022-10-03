from __future__ import annotations

import json
import secrets
import shelve
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, ClassVar, Dict, Iterator, Optional, Tuple

from pyarrow import flight

from fal_teller.server._providers import ArrowProvider, PathT, get_provider

TOKEN_DB_PATH = "/tmp/tokens.db"


def parse_criteria(raw_criteria: bytes) -> Optional[Dict[str, Any]]:
    """Each criteria must be either empty or a json-encoded data
    from the client."""
    if not raw_criteria:
        return None

    return json.loads(raw_criteria)


class AuthenticationMiddlewareFactory(flight.ServerMiddlewareFactory):
    def start_call(
        self, info: flight.CallInfo, headers: Dict[str, str]
    ) -> AuthenticationMiddleware:
        auth_header = self.parse_header(headers, "Authorization")
        target_profile_name = self.parse_header(headers, "Target-Profile")

        # Initially try to parse the auth header's value
        authentication_type, _, authentication_token = auth_header.partition(" ")

        if authentication_type != "Bearer" or not authentication_token:
            raise flight.FlightUnauthenticatedError("Invalid credentials type!")

        with shelve.open(TOKEN_DB_PATH) as db:
            token_db = db["tokens"]
            token_profile = token_db.get(authentication_token)
            if token_profile is None:
                raise flight.FlightUnauthenticatedError("Unregistered user!")

            trusted_profiles = token_profile["profiles"]

            # Once we have the list of trusted profiles, check whether if the
            # target is one of them.
            if target_profile_name not in trusted_profiles:
                raise flight.FlightUnauthenticatedError(
                    f"Can't access '{target_profile_name}' with token '{authentication_token}'!"
                )

            target_profile = db["profiles"][target_profile_name]

        return AuthenticationMiddleware(target_profile_name, target_profile)

    def parse_header(self, headers: Dict[str, str], header_name: str) -> str:
        for header_key, header_values in headers.items():
            if header_key.casefold() == header_name.casefold():
                assert len(header_values) == 1
                return header_values[0]
        else:
            raise flight.FlightUnauthenticatedError(
                f"Expected header '{header_name}' but couldn't find it!"
            )


@dataclass(init=False)
class AuthenticationMiddleware(flight.ServerMiddleware):
    """A ServerMiddleware that transports incoming username and password."""

    profile_name: str
    provider: ArrowProvider

    def __init__(self, profile_name: str, raw_provider_args: Dict[str, Any]) -> None:
        self.profile_name = profile_name
        self.provider = get_provider(
            raw_provider_args["type"], **raw_provider_args["params"]
        )

    def sending_headers(self):
        return {}


@dataclass
class Ticket:
    MAX_VALIDITY: ClassVar[timedelta] = timedelta(minutes=120)

    path: Any
    provider: ArrowProvider[Any]
    token: str = field(default_factory=secrets.token_urlsafe)
    generated_time: datetime = field(default_factory=datetime.now)

    def is_valid(self) -> bool:
        return self.generated_time + self.MAX_VALIDITY > datetime.now()

    def serialize(self) -> bytes:
        return self.token.encode("utf-8")


@dataclass
class TicketStore:
    """Tickets generated from the get_flight_info calls. They are single-use, have
    limited time validity (120 minutes) and can be used without authentication."""

    tickets: Dict[str, Ticket] = field(default_factory=dict)

    def add_ticket(self, normalized_path: Any, provider: ArrowProvider[Any]) -> bytes:
        ticket = Ticket(normalized_path, provider)
        self.tickets[ticket.token] = ticket
        return ticket.serialize()

    def use_ticket(self, token: str) -> Ticket:
        ticket = self.tickets.pop(token, None)
        if ticket is None:
            raise flight.FlightUnauthenticatedError("Invalid ticket token!")

        if not ticket.is_valid():
            raise flight.FlightUnauthenticatedError(
                f"Ticket expired at {ticket.generated_time + ticket.MAX_VALIDITY}!"
            )

        return ticket


class TellerServer(flight.FlightServerBase):
    def __init__(self, location: str, *args, **kwargs) -> None:
        self._endpoint = location
        self._ticket_store = TicketStore()
        super().__init__(
            location=location, middleware={"auth": AuthenticationMiddlewareFactory()}
        )

    def _profile_from(self, context: flight.ServerCallContext) -> ArrowProvider:
        auth_middleware: AuthenticationMiddleware = context.get_middleware("auth")
        if auth_middleware is None:
            raise flight.FlightUnauthenticatedError("Client must use authentication!")
        return auth_middleware.provider

    def _unpack_descriptor(
        self,
        provider: ArrowProvider[PathT],
        descriptor: flight.FlightDescriptor,
    ) -> PathT:
        # Descriptors store paths as bytes, so we need to decode it before
        # actually normalizing it.
        parts = [part.decode() for part in descriptor.path]
        return provider.pack_path(*parts)

    def list_flights(
        self, context: flight.ServerCallContext, criteria: bytes
    ) -> Iterator[flight.FlightInfo]:
        # Flight accepts a list of criteria for the search (like
        # query only the tables starting with prefix F_, etc). We
        # currently do not support it.
        assert parse_criteria(criteria) is None

        provider = self._profile_from(context)
        for table_info in provider.list():
            flight_descriptor = flight.FlightDescriptor.for_path(
                # This is going to be the same descriptor that the
                # do_get is going to parse, so we need to serialize
                # the path in the native Arrow form (and load it back
                # on do_get with the provider's custom representation).
                *provider.unpack_path(table_info.path)
            )
            ticket_token = self._ticket_store.add_ticket(table_info.path, provider)
            flight_endpoint = flight.FlightEndpoint(ticket_token, [self._endpoint])
            yield table_info.to_flight(flight_descriptor, endpoints=[flight_endpoint])

    def get_flight_info(
        self, context: flight.ServerCallContext, descriptor: flight.FlightDescriptor
    ) -> flight.FlightInfo:
        provider = self._profile_from(context)
        requested_path = self._unpack_descriptor(provider, descriptor)
        table_info = provider.info(requested_path)

        # We don't need to create a new descriptor, since we can simply forward what
        # we received.
        ticket_token = self._ticket_store.add_ticket(table_info.path, provider)
        flight_endpoint = flight.FlightEndpoint(ticket_token, [self._endpoint])
        return table_info.to_flight(descriptor, endpoints=[flight_endpoint])

    def do_get(
        self, context: flight.ServerCallContext, ticket: flight.Ticket
    ) -> flight.FlightDataStream:
        token = ticket.ticket.decode("utf-8")
        ticket = self._ticket_store.use_ticket(token)
        reader = ticket.provider.read_from(ticket.path, query=None)
        return flight.RecordBatchStream(reader)

    def do_put(
        self,
        context: flight.ServerCallContext,
        descriptor: flight.FlightDescriptor,
        reader: flight.MetadataRecordBatchReader,
        writer: flight.FlightMetadataWriter,
    ) -> None:
        provider = self._profile_from(context)
        target_path = self._unpack_descriptor(provider, descriptor)
        provider.write_to(target_path, reader.to_reader())

    def list_actions(self, context):
        return []

    def do_action(self, context, action):
        raise NotImplementedError(f"Action '{action.type}' is not implemented!")
