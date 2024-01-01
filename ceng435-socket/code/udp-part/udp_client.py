import socket
import os
import pickle
import time

def create_segments_for_files(file_paths, segment_size):
    """
    this function creates segments for the files
    each segment is dictonary containts some fields 
    like file_id, sequence_number, data, is_last_segment
    """
    
    all_segments = []
    file_id = 0

    for file_path in file_paths:
        # check if the file exists
        if not os.path.exists(file_path):
            print(f"File {file_path} does not exist.")
            continue
        # reading the file and creating segments
        with open(file_path, 'rb') as file:
            sequence_number = 0
            while True:
                data = file.read(segment_size)
                if not data:
                    break
                segment = {
                    'file_id': file_id, # identify the file
                    'sequence_number': sequence_number, # order of the segment
                    'data': data, # actual data
                    'is_last_segment': False # flag for the last segment
                }
                all_segments.append(segment)
                sequence_number += 1

        # set the last segment flag to true
        all_segments[-1]['is_last_segment'] = True
        file_id += 1

    return all_segments

def send_segment(udp_socket, segment, server_address):
    # Serialize and send a segment over UDP
    try:
        #  The segment dictionary is serialized with pickle
        serialized_segment = pickle.dumps(segment)        # Serialize the segment using pickle
     
        # The sendto method of the socket
        udp_socket.sendto(serialized_segment, server_address)           # Send the serialized segment to the server
    except Exception as e:
        print(f"Error sending segment: {e}")

def receive_ack(udp_socket, expected_seq_num, timeout=2):
    # this function waits for an ack for the segment from the server
    # ack is expected for a specific segment
    try:
        udp_socket.settimeout(timeout)
        while True:
            ack_data, _ = udp_socket.recvfrom(1024)  # buffer size
            ack = pickle.loads(ack_data)
            
            # Check if the acknowledgment is for the expected segment
            if ack.get('acknowledged_sequence_number') == expected_seq_num:
                print(f"Acknowledgment received for segment {expected_seq_num}")
                return ack
    except socket.timeout:
        print(f"No acknowledgment received for segment {expected_seq_num}.")
        return None
    except Exception as e:
        print(f"Error in receiving acknowledgment: {e}")
        return None
    
def send_and_wait_for_ack(udp_socket, segment, server_address, timeout=2):
    # this function sends the segment to the server
    # and waits for the ack, resends the segment if ack is not received
    while True:
        send_segment(udp_socket, segment, server_address)
        ack = receive_ack(udp_socket, segment['sequence_number'], timeout)
        if ack is not None:
            break  # Ack received, break out of the loop
        print(f"Resending segment {segment['sequence_number']}")


def start_client(server_ip, server_port, window_size=10):
    # main function for the client
    # It sends file segments to the server, ensuring that the number of unacknowledged
    # segments does not exceed the window size.
    server_address = (server_ip, server_port)
    udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

    file_paths = [f"../../objects/small-{i}.obj" for i in range(10)] + \
                    [f"../../objects/large-{i}.obj" for i in range(10)] 
                 
    segment_size = 1024  # Defining the segment size

    window_size = 10  # Defining the window size, must be smaller than segment size

    segments = create_segments_for_files(file_paths, segment_size)

    sent_segments = set()
    acked_segments = set()

    for segment in segments:
        # wait if the number of unacknowledged segments is equal to the window size
        while len(sent_segments) - len(acked_segments) >= window_size:
            time.sleep(0.1)  # Wait before sending more segments

        send_and_wait_for_ack(udp_socket, segment, server_address)
        sent_segments.add(segment["sequence_number"])
        acked_segments.add(segment["sequence_number"]) # when ack is received, add it to acked_segments


    udp_socket.close()

if __name__ == "__main__":
    start_client("127.0.0.1", 8000)
