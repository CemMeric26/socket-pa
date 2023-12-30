import socket
import os

def receive_object(conn, filename):
    try:
        # Read the size of the object first
        object_size = int.from_bytes(conn.recv(4), 'big')

        # Read the object data
        object_data = conn.recv(object_size)

        # Write the received data to a file
        with open(filename, 'wb') as file:
            file.write(object_data)

        print(f"Received object of size: {len(object_data)} and saved to {filename}")
        # Send acknowledgment
        conn.sendall(b"Ack")
    except Exception as e:
        print(f"Error receiving object: {e}")
        return False  # Indicates an error occurred
    return True  # Indicates success

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
                    print("Error in receiving small object. Ending transmission.")
                    break
                if not receive_object(conn, f"{received_dir}/received_large-{i}.obj"):
                    print("Error in receiving large object. Ending transmission.")
                    break
            print("Server has finished receiving files.")

if __name__ == "__main__":
    start_server()
