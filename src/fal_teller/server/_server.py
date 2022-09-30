from __future__ import annotations

import json
import shelve
from dataclasses import dataclass
from typing import Any, Dict, Iterator, Optional, Tuple

from pyarrow import flight

from fal_teller.server._providers import ArrowProvider, get_provider

TOKEN_DB_PATH = "/tmp/tokens.db"


def parse_criteria(raw_criteria: str) -> Optional[Dict[str, Any]]:
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


class TellerServer(flight.FlightServerBase):
    def __init__(self, location: str, *args, **kwargs) -> None:
        self._endpoint = location
        super().__init__(
            location=location, middleware={"auth": AuthenticationMiddlewareFactory()}
        )

    def _profile_from(
        self, context: flight.ServerCallContext
    ) -> Tuple[str, ArrowProvider]:
        auth_middleware = context.get_middleware("auth")
        if auth_middleware is None:
            raise flight.FlightUnauthenticatedError("Client must use authentication!")
        return auth_middleware.profile_name, auth_middleware.provider

    def _unpack_descriptor(
        self, descriptor: flight.FlightDescriptor
    ) -> Tuple[str, str]:
        if len(descriptor.path) != 2:
            raise flight.FlightError(
                f"Flight descriptors must always have a path of len 2, not '{len(descriptor.path)}'"
            )

        profile_name, descriptor_path = descriptor.path
        return profile_name.decode(), descriptor_path.decode()

    def list_flights(
        self, context: flight.ServerCallContext, criteria: bytes
    ) -> Iterator[flight.FlightInfo]:
        assert parse_criteria(criteria) is None
        profile_name, provider = self._profile_from(context)
        for table_info in provider.list():
            flight_descriptor = flight.FlightDescriptor.for_path(
                profile_name, table_info.path
            )
            flight_endpoint = flight.FlightEndpoint(table_info.path, [self._endpoint])
            yield table_info.to_flight(flight_descriptor, endpoints=[flight_endpoint])

    def get_flight_info(
        self, context: flight.ServerCallContext, descriptor: flight.FlightDescriptor
    ) -> flight.FlightInfo:
        profile_name, provider = self._profile_from(context)
        requested_profile_name, requested_path = self._unpack_descriptor(descriptor)
        if requested_profile_name != profile_name:
            raise flight.FlightUnauthenticatedError(
                f"Can't request profile '{requested_profile_name}' while being authenticated to {profile_name}!"
            )

        table_info = provider.info(requested_path)
        flight_endpoint = flight.FlightEndpoint(requested_path, [self._endpoint])
        return table_info.to_flight(descriptor, endpoints=[flight_endpoint])

    def do_get(
        self, context: flight.ServerCallContext, ticket: flight.Ticket
    ) -> flight.FlightDataStream:
        profile_name, provider = self._profile_from(context)
        ticket_path = ticket.ticket.decode("utf-8")

        reader = provider.read_from(ticket_path, query=None)
        return flight.RecordBatchStream(reader)

    def do_put(
        self,
        context: flight.ServerCallContext,
        descriptor: flight.FlightDescriptor,
        reader: flight.MetadataRecordBatchReader,
        writer: flight.FlightMetadataWriter,
    ) -> None:
        profile_name, provider = self._profile_from(context)
        target_profile_name, target_path = self._unpack_descriptor(descriptor)
        if target_profile_name != profile_name:
            raise flight.FlightUnauthenticatedError(
                f"Can't request profile '{target_profile_name}' while being authenticated to {profile_name}!"
            )

        provider.write_to(target_path, reader.to_reader())

    def list_actions(self, context):
        return []

    def do_action(self, context, action):
        raise NotImplementedError(f"Action '{action.type}' is not implemented!")
