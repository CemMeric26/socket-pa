import socket

def receive_object(conn):
    # Read the size of the object first
    object_size = int.from_bytes(conn.recv(4), 'big') # Receives the first 4 bytes, which represent the size of the incoming object. This is expected to be sent by the client as a 4-byte big-endian integer.
    # Read the object data
    object_data = conn.recv(object_size)  # Receives the actual object data based on the previously determined size.
    print(f"Received object of size: {len(object_data)}")
    # Send acknowledgment
    conn.sendall(b"Ack")  #Sends an acknowledgment back to the client, indicating that the object has been received successfully.

def start_server():
    host = '127.0.0.1'
    port = 65432

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, port)) # Binds the socket to the specified host and port.
        s.listen()
        print(f"Server listening on {host}:{port}")

        conn, addr = s.accept() # Waits for a client to connect. When a client connects, it returns a new socket object representing the connection and a tuple holding the address of the client.
        with conn:
            print(f"Connected by {addr}")
            for _ in range(20):  # Expecting 20 objects (10 large, 10 small)
                receive_object(conn)

if __name__ == "__main__":
    start_server()