import socket

def send_object(sock, object_data):
    # Send the size of the object first
    sock.sendall(len(object_data).to_bytes(4, 'big'))
    # Send the object data
    sock.sendall(object_data) # Sends data through the socket. sendall ensures that all data is sent.
    # Wait for acknowledgment
    ack = sock.recv(1024) # Waits to receive an acknowledgment from the server, up to 1024 bytes.
    print(f"Acknowledged: {ack}")

def start_client():
    host = '127.0.0.1'
    port = 65432

    large_object = b'A' * 100000  # Example large object (100,000 bytes)
    small_object = b'B' * 100     # Example small object (100 bytes)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port)) # Establishes a TCP connection to the server at the specified host and port.
        
        for _ in range(10):
            send_object(s, large_object)  # Send large object
            send_object(s, small_object)  # Send small object

if __name__ == "__main__":
    start_client()