import socket
import os

def receive_object(conn, filename):
    try:
        # Read the size of the object first
        object_size_bytes = conn.recv(4)
        if not object_size_bytes:
            return False
        object_size = int.from_bytes(object_size_bytes, 'big')

        # Read the object data
        object_data = b''
        while len(object_data) < object_size:
            more_data = conn.recv(object_size - len(object_data))
            if not more_data:
                raise Exception("Connection lost while receiving file data.")
            object_data += more_data

        # Write the received data to a file
        with open(filename, 'wb') as file:
            file.write(object_data)

        print(f"Received object of size: {len(object_data)} and saved to {filename}")
        # Send acknowledgment
        conn.sendall(b"Ack")
    except Exception as e:
        print(f"Error receiving object: {e}")
        return False
    return True

def start_server():
    HOST = "127.0.0.1"
    PORT = 8000

    received_dir = "./received"
    if not os.path.exists(received_dir):
        os.makedirs(received_dir)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        print(f"Server listening on {HOST}:{PORT}")

        conn, addr = s.accept()
        with conn:
            print(f"Connected by {addr}")
            for i in range(10):  # Expecting 10 small and 10 large objects
                if not receive_object(conn, f"{received_dir}/received_small-{i}.obj"):
                    break
                if not receive_object(conn, f"{received_dir}/received_large-{i}.obj"):
                    break
            print("Server has finished receiving files.")

if __name__ == "__main__":
    start_server()
