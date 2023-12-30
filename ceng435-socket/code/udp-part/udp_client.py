import socket
import os
import json
import time

def create_segments(file_path, segment_size):
    # This function should read the file and create segments
    # You can use the following snippet to create segments
    segments = []
    sequence_number = 0

    with open(file_path, 'rb') as file: # The function opens the file in binary read mode ('rb')
        while True:
            data = file.read(segment_size) # It reads the file in chunks, with each chunk being segment_size bytes.
            if not data:
                break
            # For each chunk, it creates a dictionary (segment) with three keys:
                #'sequence_number': A unique identifier for each segment.
                # 'data': The binary data of the segment.
                # 'is_last_segment': A boolean flag indicating whether this is the last segment of the file.
            segment = {
                'sequence_number': sequence_number,
                'data': data,
                'is_last_segment': False  # Will be set later
            }
            segments.append(segment) # append to the segments list
            sequence_number += 1  # increment the sequence number

    # Mark the last segment
    if segments:
        segments[-1]['is_last_segment'] = True

    return segments


def send_segment(udp_socket, segment, server_address):
    # Serialize and send a segment
    try:
        #  The segment dictionary is serialized into a JSON string using json.dumps 
        # then encoded into bytes. This is necessary 
        # because UDP sendto requires data to be in bytes.
        serialized_segment = json.dumps(segment).encode('utf-8')         # Serialize the segment into JSON
     
        # The sendto method of the socket object is used to 
        # send the serialized segment to the specified server_address.
        udp_socket.sendto(serialized_segment, server_address)           # Send the serialized segment to the server
    except Exception as e:
        print(f"Error sending segment: {e}")

def receive_ack(udp_socket, expected_seq_num, timeout=0.8):
    # Wait for an acknowledgment
    try:
        # This timeout prevents the client from waiting indefinitely for an acknowledgment.
        # If the timeout is reached, a socket.timeout exception is raised
        udp_socket.settimeout(timeout)        # Set a timeout for the socket to wait for an acknowledgment

        while True:
            # Wait for a response from the server
            ack_data, _ = udp_socket.recvfrom(1024) # The recvfrom method of the socket object is used to receive data from the server.
            ack = json.loads(ack_data.decode('utf-8'))      # it deserializes the JSON data to get the acknowledgment details.

            # Check if the acknowledgment is for the expected segment
            if ack.get('sequence_number') == expected_seq_num:
                return ack  # Return the acknowledgment

    except socket.timeout:
        print(f"No acknowledgment received for segment {expected_seq_num}.")
        return None
    except Exception as e:  # if no acknowledgment is received, the client prints None
        # but we should send the segment again i think for the data integrity
        print(f"Error in receiving acknowledgment: {e}")
        return None

def start_client(server_ip, server_port):
    server_address = (server_ip, server_port)
    udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

    file_path = "path_to_your_file"
    segment_size = 1024  # Define your segment size

    window_size = 10  # Define your window size, must be smaller than segment size

    segments = create_segments(file_path, segment_size)
    sent_segments = set()

    for segment in segments:
        send_segment(udp_socket, segment, server_address)
        sent_segments.add(segment["sequence_number"])
        ack = receive_ack(udp_socket, segment['sequence_number'])
        if ack is None:
            # Handle missing acknowledgment (e.g., resend the segment)
            pass
        if len(sent_segments) >= window_size:
            # Implement logic to wait for acknowledgments or timeouts
            pass

    udp_socket.close()

if __name__ == "__main__":
    start_client("127.0.0.1", 8000)
