from dataclasses import dataclass
from functools import cached_property, lru_cache, singledispatchmethod
from typing import Any, List, Literal, Optional, Tuple, overload

import pandas
import pyarrow as arrow
from pyarrow import flight


@dataclass
class TellerProfile:
    name: str

    def to_headers(self, token: str) -> List[Tuple[bytes, bytes]]:
        return [
            (b"authorization", f"Bearer {token}".encode()),
            (b"target-profile", self.name.encode()),
        ]


@dataclass(frozen=True)
class TellerClient:
    server: str
    token: str
    default_profile: str

    @lru_cache(None)
    def get_profile(self, name: Optional[str] = None) -> TellerProfile:
        return TellerProfile(name or self.default_profile)

    def write(self, data, to: str, *, profile_name: Optional[str] = None) -> None:
        """Write the given `data` to the given `table` on the
        given profile (or the default, if it is not supplied)."""
        return self._write(data, to=to, profile=self.get_profile(profile_name))

    @singledispatchmethod
    def _write(
        self,
        data: Any,
        to: str,
        *,
        profile: TellerProfile,
    ) -> None:
        # TODO: show a list of supported formats.
        raise ValueError(
            f"Given 'data' format '{type(data).__name__}' is not supported."
        )

    @_write.register
    def _write_as_pandas(
        self,
        data: pandas.DataFrame,
        to: str,
        *,
        profile: TellerProfile,
    ) -> None:
        arrow_table = arrow.Table.from_pandas(data)
        self._write_as_arrow(arrow_table, to=to, profile=profile)

    @_write.register
    def _write_as_arrow(
        self,
        data: arrow.Table,
        to: str,
        *,
        profile: TellerProfile,
    ) -> None:
        flight_options = flight.FlightCallOptions(
            headers=profile.to_headers(self.token)
        )
        flight_path = self._flight_path_for(to, profile=profile)
        schema = data.schema

        writer, _ = self._client.do_put(flight_path, schema, options=flight_options)
        writer.write_table(data)
        writer.close()

    @overload
    def read(
        self,
        from_: str,
        format: Literal["pandas"] = "pandas",
        *,
        profile: Optional[str] = None,
    ) -> pandas.DataFrame:
        ...

    @overload
    def read(
        self, from_: str, format: Literal["arrow"], *, profile: Optional[str] = None
    ) -> arrow.Table:
        ...

    def read(
        self,
        from_: str,
        format: Literal["pandas", "arrow"] = "pandas",
        *,
        profile_name: Optional[str] = None,
    ) -> Any:
        profile = self.get_profile(profile_name)
        if format == "pandas":
            return self._read_as_arrow(from_, profile=profile).to_pandas()
        elif format == "arrow":
            return self._read_as_arrow(from_, profile=profile)
        else:
            raise ValueError("Unsupported format: " + format)

    def _read_as_arrow(self, from_: str, *, profile: TellerProfile) -> arrow.Table:
        flight_options = flight.FlightCallOptions(
            headers=profile.to_headers(self.token)
        )
        flight_path = self._flight_path_for(from_, profile=profile)
        flight_info = self._client.get_flight_info(flight_path, options=flight_options)
        ticket = flight_info.endpoints[0].ticket

        stream_reader = self._client.do_get(ticket, options=flight_options)
        return stream_reader.read_all()

    def _flight_path_for(
        self,
        table_name: str,
        *,
        profile: TellerProfile,
    ) -> flight.FlightDescriptor:
        return flight.FlightDescriptor.for_path(table_name)

    @cached_property
    def _client(self) -> flight.FlightClient:
        return flight.connect(self.server)
