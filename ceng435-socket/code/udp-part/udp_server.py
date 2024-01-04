import socket
import os
import pickle
import hashlib

def process_segment(segment, received_segments):
    file_id = segment['file_id']
    if file_id not in received_segments:
        received_segments[file_id] = {}
    received_segments[file_id][segment['sequence_number']] = segment

    return segment['is_last_segment']

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
        print(f"Acknowledgment sent for segment {sequence_number}")
    except Exception as e:
        print(f"Error sending acknowledgment: {e}")

def verify_checksum(file_path, file_type, file_number):
    """
    Compute and verify the MD5 checksum of the given file.
    """
    # Read the expected checksum from the corresponding .md5 file
    with open(f"../../objects/{file_type}-{file_number}.obj.md5", 'r') as file:
        expected_checksum = file.read().strip()

    print(f"Verifying checksum of {file_path}... with expected_checksum: {expected_checksum}")

    # Initialize MD5 hasher
    hasher = hashlib.md5()

    # Read and update hasher with file content
    with open(file_path, 'rb') as file:
        buf = file.read()
        hasher.update(buf)  # Update the hasher with the content of the file

    # Compute our checksum
    our_checksum = hasher.hexdigest()

    print(f"Checksum of {file_path} is our checksum: {our_checksum}")

    # Compare and return the result of checksum verification
    return our_checksum == expected_checksum


def reassemble_file(received_files, output_directory, file_id, total_files):
    """
    Reassemble and save the file from its segments after everything is received.
    Verifies the MD5 checksum of the reassembled file.
    """
    # Determine the file type and number based on the file_id
    if file_id % 2 == 0:
        file_type = "small"
    else:
        file_type = "large"
    file_number = file_id // 2

    sorted_segments = sorted(received_files[file_id].values(), key=lambda x: x['sequence_number'])
    file_data = b''.join(segment['data'] for segment in sorted_segments)

    output_file_path = os.path.join(output_directory, f"{file_type}-{file_number}.obj")
    with open(output_file_path, 'wb') as file:
        file.write(file_data)

    # Assuming the function verify_checksum exists
    if verify_checksum(output_file_path, file_type, file_number):
        print(f"File {output_file_path} reassembled, saved, and verified.")
    else:
        print(f"File {output_file_path} reassembled and saved, but failed verification.")


def start_server():
    local_ip = "server"
    local_port = 8000
    udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    udp_socket.bind((local_ip, local_port))

    print("UDP server up and listening")

    received_segments = {} # Dictionary to store received segments
    output_directory = "./received_files"  # Define the output directory

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    while True:
        buffer_size = 4096
        bytes_address_pair = udp_socket.recvfrom(buffer_size)
        segment = pickle.loads(bytes_address_pair[0])  # Deserialize the segment using pickle  
        address = bytes_address_pair[1]

        # process the segment and send ack
        is_last_segment = process_segment(segment, received_segments)
        send_ack(udp_socket, address, segment["sequence_number"])

        # If the segment is the last one for its file, reassemble and save the file
        if is_last_segment:
            # here reassemble the file and save it
            reassemble_file(received_segments, output_directory, segment['file_id'],20)
            print(f"File {segment['file_id']} reassembled and saved.")

    udp_socket.close()

if __name__ == "__main__":
    start_server()
