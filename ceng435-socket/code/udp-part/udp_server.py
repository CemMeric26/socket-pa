import socket
import os
import pickle
import threading
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def process_segment(segment, received_files):
    """
    Processes a received segment. store the segment data in the received_files 
    determine if the segment is the last one for its file.
    """
    file_id = segment['file_id']
    sequence_number = segment['sequence_number']
    data = segment['data']
    is_last_segment = segment['is_last_segment']

    # Store the segment data in the received_files dictionary
    received_files.setdefault(file_id, {})[sequence_number] = data

    logging.info(f"Received segment {segment['sequence_number']} from file {segment['file_id']}")


    return is_last_segment

def send_ack(udp_socket, client_address, sequence_number):
    # Send ack back to the client for a received segment.
    try:
        # Create an acknowledgment message
        # The function creates a dictionary ack_message with the sequence number 
        # that was successfully received
        ack_message = {
            'acknowledged_sequence_number': sequence_number
        }

        # Serialize the acknowledgment message
        # seriazlize it with pickle
        serialized_ack = pickle.dumps(ack_message)

        # Send the acknowledgment back to the client
        udp_socket.sendto(serialized_ack, client_address)
        logging.info(f"Sending ACK for segment {sequence_number}")
    except Exception as e:
        print(f"Error sending acknowledgment: {e}")


def reassemble_file(received_files, output_directory, file_id):
    """
    Reassemble and save the file from its segments after everything is received.
    """
    # Determine file type based on file_id
    file_type = "small" if file_id < 10 else "large"
    
    # Sort the segments by sequence number and concatenate their data
    file_data = b''.join(received_files[file_id][seq_num] for seq_num in sorted(received_files[file_id]))

    output_file_path = os.path.join(output_directory, f"{file_type}-obj{file_id % 10}.obj")
    with open(output_file_path, 'wb') as file:
        file.write(file_data)
    print(f"File {output_file_path} reassembled and saved.")


def start_server():
    local_ip = "127.0.0.1"
    local_port = 8000
    udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    udp_socket.bind((local_ip, local_port))

    print("UDP server up and listening")


    received_files = {} # Dictionary to store received segments
    received_ack = set() # Set to keep track of received acknowledgments
    output_directory = "./received_files"  # Define the output directory
    lock = threading.Lock() # Lock to synchronize access to received_files

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    while True:
        buffer_size = 4096
        bytes_address_pair = udp_socket.recvfrom(buffer_size)
        segment = pickle.loads(bytes_address_pair[0])  # Deserialize the segment using pickle  
        address = bytes_address_pair[1]

        sequence_number = segment['sequence_number']

        # Lock access to received_files as it might be accessed from multiple threads
        with lock:
            # Process the segment and always send an ACK back
            is_last_segment = process_segment(segment, received_files)
            send_ack(udp_socket, address, sequence_number)

            # Add the sequence number to the received acknowledgments
            received_ack.add(sequence_number)

            # If the segment is the last one for its file, attempt to reassemble the file
            if is_last_segment:
                # Check if all previous segments have been received
                all_segments_received = all(seq_num in received_ack for seq_num in range(sequence_number + 1))
                if all_segments_received:
                    reassemble_file(received_files, output_directory, segment['file_id'])
                    print(f"File {segment['file_id']} reassembled and saved.")


    udp_socket.close()

if __name__ == "__main__":
    start_server()
