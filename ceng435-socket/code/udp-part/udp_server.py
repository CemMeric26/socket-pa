import socket
import json
import os

def process_segment(segment, output_directory):
    # Process received segment
    # Extract segment data
    sequence_number = segment['sequence_number'] # get the value from the segment dictionary
    data = segment['data']                       # get the value from the segment dictionary
    is_last_segment = segment['is_last_segment'] # get the value from the segment dictionary

    # Define the output file path 
    output_file_path = os.path.join(output_directory, f"/output_{sequence_number}.obj")

    # Write the segment data to the output file
    with open(output_file_path, 'wb') as file:
        file.write(data)

    print(f"Processed segment {sequence_number}, Last Segment: {is_last_segment}")

    return is_last_segment  # Return if the segment is the last segment, this means all segments are received

def send_ack(udp_socket, client_address, sequence_number):
    # Send acknowledgment for received segment
    try:
        # Create an acknowledgment message
        # The function creates a dictionary ack_message with the sequence number 
        # that was successfully received
        ack_message = {
            'acknowledged_sequence_number': sequence_number
        }

        # Serialize the acknowledgment message
        # seriazlize it to json then encode it to bytes
        serialized_ack = json.dumps(ack_message).encode('utf-8')

        # Send the acknowledgment back to the client
        udp_socket.sendto(serialized_ack, client_address)
    except Exception as e:
        print(f"Error sending acknowledgment: {e}")

def start_server():
    local_ip = "127.0.0.1"
    local_port = 8000
    udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    udp_socket.bind((local_ip, local_port))

    print("UDP server up and listening")

    received_segments = set()
    output_directory = "./received_files"  # Define the output directory

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    while True:
        bytes_address_pair = udp_socket.recvfrom(1024)
        segment = json.loads(bytes_address_pair[0].decode('utf-8'))
        address = bytes_address_pair[1]

        if segment["sequence_number"] not in received_segments:
            is_last_segment = process_segment(segment, output_directory)
            send_ack(udp_socket, address, segment["sequence_number"])
            received_segments.add(segment["sequence_number"])

            if is_last_segment:
                # break the loop if the last segment is received
                print("Last segment received. Server is closing.")
                break

    udp_socket.close()

if __name__ == "__main__":
    start_server()