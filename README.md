# Fal Teller

![image](https://user-images.githubusercontent.com/47358913/193630260-556469b7-163a-4035-bcd4-4c77cd7f5a0f.png)

An [Apache Arrow](https://arrow.apache.org) native dynamic storage proxy layer for query engines.

Features:
- [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) based server, for maximized performance.
- Unbuferred data streams for reading and writing.
- An inngestion layer through teleport.
- Built-in support for authentication and profile management.
- A simple (read/write orianted) client to operate on Pandas level without
  bearing the complexity of Flight.
- Ease of extensibility through plugins for different providers.
