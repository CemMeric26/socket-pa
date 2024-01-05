import socket
import os
import time

def send_object(sock, filepath):
    """
        this is the part we send the objects to the server with tcp
        Its really basic we just send the size of the object first and then we send the object itself
    """

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
    # HOST = socket.gethostbyname("server")  # Use this if you are using docker compose
    PORT = 8000
    
    elapsed_time = 0

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT)) # Connect to the server
        start_time = time.time() # Start time
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
        end_time = time.time()  # End time
        elapsed_time = end_time - start_time
        print(f"Total time taken for file transfer: {elapsed_time} seconds")

    # Open a file in write mode. If the file doesn't exist, it will be created.
    with open('elapsed_time_TCP.txt', 'a') as file:
        file.write(str(elapsed_time) + '\n')
    
       

if __name__ == "__main__":
    
    start_client()