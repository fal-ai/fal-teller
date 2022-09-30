from fal_teller.server import TellerServer

if __name__ == "__main__":
    server = TellerServer("grpc://0.0.0.0:1997")
    server.serve()
