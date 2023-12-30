import socket
import os

def send_object(sock, filepath):

    if not os.path.exists(filepath): # Check if the file exists
        print(f"File not found: {filepath}")
        return False

    try:
        file_size = os.path.getsize(filepath) # Get the size of the file
        # Send the size of the object first
        sock.sendall(file_size.to_bytes(4, 'big')) # Convert int to bytes and send

        # Send the object data
        with open(filepath, 'rb') as file: # Open the file as binary
            while True: # Keep sending until all is sent
                data = file.read(1024)  # Read in chunks
                if not data: # EOF
                    break
                sock.sendall(data) # Send the chunk
        
        # Wait for acknowledgment
        ack = sock.recv(1024) # Receive acknowledgment
        print(f"Acknowledged: {ack.decode()}") # Print acknowledgment
    except Exception as e:
        print(f"Error sending object: {e}") # Print error message
        return False
    return True

def start_client():
    HOST = "127.0.0.1"
    PORT = 8000

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT)) # Connect to the server
        
        for i in range(10):
            # Send small and large objects
            if not send_object(s, f"../objects/small-{i}.obj"): # Send small object
                print(f"Error in sending small-{i}.obj. Ending transmission.")
                break # Break if an error occurs
            if not send_object(s, f"../objects/large-{i}.obj"): # Send large object
                print(f"Error in sending large-{i}.obj. Ending transmission.")
                break # Break if an error occurs

        # Send an end-of-transmission message
        s.sendall(b"END") 
        print("End of transmission message sent.")

if __name__ == "__main__":
    start_client()
