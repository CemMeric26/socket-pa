import socket
import os
import pickle
import hashlib
from common import WINDOW_SIZE, TIMEOUT_DURATION, TIMEOUT_SLEEP


def process_segment(segment, received_segments):
    file_id = segment['file_id']
    if file_id not in received_segments:
        received_segments[file_id] = {}
    received_segments[file_id][segment['sequence_number']] = segment

    return segment['is_last_segment']

def send_ack(udp_socket, client_address, sequence_number, checksum):
    # Send ack back to the client for a received segment.
    try:
        # Create an acknowledgment message
        ack_message = {
            'acknowledged_sequence_number': sequence_number,
            'checksum': checksum
        }

        # Serialize the acknowledgment message using pickle
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

def calculate_checksum(data):
    # Convert data to bytes if it is not already a bytes-like object
    if isinstance(data, str):
        # If it's a string, encode it to bytes
        data = data.encode('utf-8')
    elif isinstance(data, (bytearray, memoryview)):
        # If it's a bytearray or memoryview (buffer protocol types), convert to bytes
        data = bytes(data)
    elif not isinstance(data, bytes):
        # If it's not a bytes-like object at all, raise an error
        raise TypeError("The data provided is not bytes or cannot be converted to bytes.")
    
    # Create an MD5 hash object
    hash_obj = hashlib.md5()
    
    # Update the hash object with the data
    hash_obj.update(data)
    
    # Return the hex digest of the data
    return hash_obj.hexdigest()

def receive_segment(udp_socket):
    # Receive a segment over UDP and return the deserialized segment.
    try:
        # The recvfrom method of the socket
        # The recvfrom method returns a tuple with the serialized segment and the address of the sender
        serialized_segment, address = udp_socket.recvfrom(1024)  # buffer size

        # Deserialize the segment using pickle
        segment = pickle.loads(serialized_segment)

        # Return the deserialized segment and the address of the sender
        return segment
    except Exception as e:
        print(f"Error receiving segment: {e}")
        return None, None

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

def is_not_corrupt(segment):
    # Placeholder function to check if a segment is not corrupt
    hash_object_data = hashlib.md5()
    # Update the hash object with the data
    hash_object_data.update(segment['data'])
    # Return the hex digest of the data
    # hash_object_data.hexdigest() == segment['checksum']
    return True

def has_sequence_number(segment, expected_seq_num):
    # Check if the segment has the expected sequence number
    return segment['sequence_number'] == expected_seq_num


def GBN_receiver(udp_socket):

    received_segments = {} # Dictionary to store received segments
    output_directory = "./received_files"  # Define the output directory

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    expected_seq_num = 0  # Start with expecting the first sequence number

    while True:
        # segment = receive_segment(udp_socket)
        buffer_size = 1024*10*WINDOW_SIZE
        bytes_address_pair = udp_socket.recvfrom(buffer_size)
        segment = pickle.loads(bytes_address_pair[0])  # Deserialize the segment using pickle  
        address = bytes_address_pair[1]
        

        if segment and is_not_corrupt(segment):
            # checksum = calculate_checksum(segment['sequence_number'])  # Replace with actual method to compute checksum
            checksum = "" 
            if  has_sequence_number(segment, expected_seq_num):
                print(f"Segment {segment['sequence_number']} received.")
                send_ack(udp_socket, address,expected_seq_num, checksum)
                expected_seq_num += 1  # Increment the expected sequence number
            else:
                print(f"Out-of-order segment received. Expected: {expected_seq_num}, got: {segment['sequence_number']}")
                send_ack(udp_socket, address, expected_seq_num, checksum)
             # process the segment and send ack
            
            is_last_segment = process_segment(segment, received_segments)
            if is_last_segment:
                # here reassemble the file and save it
                reassemble_file(received_segments, output_directory, segment['file_id'],20)
                print(f"File {segment['file_id']} reassembled and saved.")

        # If the packet is not the one we expect, we do nothing and wait for the next one
        # The FSM diagram shows no action in the case of default (unexpected packet)


def start_server():
    local_ip = "server"
    # local_ip = "127.0.0.1"
    local_port = 8000
    udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    udp_socket.bind((local_ip, local_port))

    print("UDP server up and listening")

    GBN_receiver(udp_socket)

    udp_socket.close()


if __name__ == "__main__":
    start_server()
