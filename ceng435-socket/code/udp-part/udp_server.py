import socket
import os
import pickle

def process_segment(segment, received_files):
    file_id = segment['file_id']
    sequence_number = segment['sequence_number']
    data = segment['data']
    is_last_segment = segment['is_last_segment']

    # Store the segment data in the received_files dictionary
    received_files.setdefault(file_id, {})[sequence_number] = data

    return is_last_segment

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
        # seriazlize it with pickle
        serialized_ack = pickle.dumps(ack_message)

        # Send the acknowledgment back to the client
        udp_socket.sendto(serialized_ack, client_address)
    except Exception as e:
        print(f"Error sending acknowledgment: {e}")


def reassemble_file(received_files, output_directory, file_id):
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
    output_directory = "./received_files"  # Define the output directory

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    while True:
        buffer_size = 4096
        bytes_address_pair = udp_socket.recvfrom(buffer_size)
        segment = pickle.loads(bytes_address_pair[0])  # Deserialize the segment using pickle  
        address = bytes_address_pair[1]

        is_last_segment = process_segment(segment, received_files)
        send_ack(udp_socket, address, segment["sequence_number"])

        if is_last_segment:
            reassemble_file(received_files, output_directory, segment['file_id'])
            print(f"File {segment['file_id']} reassembled and saved.")

    udp_socket.close()

if __name__ == "__main__":
    start_server()
