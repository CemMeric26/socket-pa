import socket
import os

def send_object(sock, filepath):
    try:
        with open(filepath, 'rb') as file:
            # Send the file size first
            file_size = os.path.getsize(filepath)
            sock.sendall(file_size.to_bytes(4, 'big'))

            # Now send the file data in chunks
            while True:
                data = file.read(1024)  # Read in chunks of 1024 bytes
                if not data:
                    break
                sock.sendall(data)
        
        # Wait for acknowledgment
        ack = sock.recv(1024)
        print(f"Acknowledged: {ack.decode()}")
    except FileNotFoundError:
        print(f"File not found: {filepath}")
        return False
    except Exception as e:
        print(f"Error sending object: {e}")
        return False
    return True

def start_client():
    HOST = "127.0.0.1"
    PORT = 8000

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        
        for i in range(10):
            if not send_object(s, f"../objects/small-{i}.obj"):
                print(f"Error in sending small-{i}.obj. Ending transmission.")
                break
            if not send_object(s, f"../objects/large-{i}.obj"):
                print(f"Error in sending large-{i}.obj. Ending transmission.")
                break

        # Send an end-of-transmission message
        s.sendall(b"END")
        print("End of transmission message sent.")

if __name__ == "__main__":
    start_client()
