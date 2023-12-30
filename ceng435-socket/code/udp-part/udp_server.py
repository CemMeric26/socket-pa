import socket
import json

def process_segment(segment):
    # Process received segment
    pass

def send_ack(udp_socket, client_address, sequence_number):
    # Send acknowledgment for received segment
    pass

def start_server():
    local_ip = "127.0.0.1"
    local_port = 8000
    udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    udp_socket.bind((local_ip, local_port))

    print("UDP server up and listening")

    received_segments = set()

    while True:
        bytes_address_pair = udp_socket.recvfrom(1024)
        segment = json.loads(bytes_address_pair[0])
        address = bytes_address_pair[1]

        if segment["sequence_number"] not in received_segments:
            process_segment(segment)
            send_ack(udp_socket, address, segment["sequence_number"])
            received_segments.add(segment["sequence_number"])

        # Implement logic to check for the last segment
        # If the last segment is received, break the loop
            break

    udp_socket.close()

if __name__ == "__main__":
    start_server()
