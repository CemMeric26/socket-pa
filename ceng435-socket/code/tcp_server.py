import socket
import os

# Function to receive a file object from the connection
def receive_object(conn, filename):
    """
        this is the part we receive the objects from the client with tcp
        In here like the sender part we just receive the size of the object first and then we receive the object itself
        """
    try:
        # Receive the first 4 bytes, which indicate the size of the incoming file
        object_size_bytes = conn.recv(4)
        if not object_size_bytes:
            return False # Return False if no data is received
        object_size = int.from_bytes(object_size_bytes, 'big') # Convert bytes to integer

        # Read the object data
        object_data = b'' # Initialize an empty byte string
        while len(object_data) < object_size:
            # Keep receiving data until the full file is received
            more_data = conn.recv(object_size - len(object_data))
            if not more_data:
                raise Exception("Connection lost while receiving file data.")
            object_data += more_data # Append received data

        # Write the received data to a file
        with open(filename, 'wb') as file:
            file.write(object_data)

        print(f"Received object of size: {len(object_data)} and saved to {filename}")
        # Send acknowledgment to the client
        conn.sendall(b"Ack")
    except Exception as e:
        print(f"Error receiving object: {e}")
        return False # Return False if an error occurs
    return True   # Return True if the file is received successfully

def start_server():
    # HOST = "server"
    HOST = "127.0.0.1"
    PORT = 8000

    # Create a directory to save received files
    received_dir = "./received"
    if not os.path.exists(received_dir):
        os.makedirs(received_dir)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT)) # Bind the socket to the address and port
        s.listen() # Listen for incoming connections
        print(f"Server listening on {HOST}:{PORT}")

        conn, addr = s.accept() # Accept a new connection
        with conn:
            print(f"Connected by {addr}")
            for i in range(10):  # Expecting 10 small and 10 large objects
                # Receive and save small objects
                if not receive_object(conn, f"{received_dir}/received_small-{i}.obj"):
                    break # Break if an error occurs
                # Receive and save large objects
                if not receive_object(conn, f"{received_dir}/received_large-{i}.obj"):
                    break # Break if an error occurs
            print("Server has finished receiving files.")


if __name__ == "__main__":
    start_server()
