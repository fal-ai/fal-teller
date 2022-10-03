# Fal Teller

An [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) based
data broker for dynamically providing storage proxies to different query engines.

Features:
- Arrow-native, for best performance.
- Built-in support for authentication and profile management.
- A simple (read/write orianted) client to operate on Pandas level without
  bearing the complexity of Flight.
- Ease of extensibility through plugins for different providers.
