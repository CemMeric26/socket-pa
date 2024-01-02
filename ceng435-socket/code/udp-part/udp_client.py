import socket
import os
import pickle
import time
from threading import Thread, Lock
import logging
import hashlib

# Setup basic logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


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
                checksum = hashlib.md5(data).hexdigest()  # Calculate checksum
                timestamp = time.time()
                segment = {
                    'file_id': file_id, # identify the file
                    'sequence_number': sequence_number, # order of the segment
                    'data': data, # actual data
                    'is_last_segment': False, # flag for the last segment,
                    'checksum': checksum, # checksum of the data
                    'timestamp': timestamp, # timestamp of the segment
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
        logging.info(f"Sending segment {segment['sequence_number']}")
    except Exception as e:
        print(f"Error sending segment: {e}")

def send_segment_with_timeout(udp_socket, segment, server_address, acked_segments, send_lock):
    # Send a segment and start its timeout timer
    with send_lock:
        send_segment(udp_socket, segment, server_address)
    timer_thread = Thread(target=handle_timeout, args=(udp_socket, segment, server_address, acked_segments, send_lock))
    timer_thread.daemon = True
    timer_thread.start()

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

# Thread to listen for ACKs
def receive_acks(udp_socket, acked_segments, base):
    while True:
        ack = receive_ack(udp_socket, -1, timeout=None)  # Listen indefinitely
        if ack:
            with base['lock']:
                ack_seq_num = ack['acknowledged_sequence_number']
                acked_segments.add(ack_seq_num)
                print(f"Acknowledgment received for segment {ack_seq_num}")
                # Slide the window
                while base['value'] in acked_segments:
                    base['value'] += 1
    
def send_and_wait_for_ack(udp_socket, segment, server_address, acked_segments, send_lock):
    
    with send_lock:
        send_segment(udp_socket, segment, server_address)
    
    # Start a thread to handle the timeout
    timeout_thread = Thread(target=handle_timeout, args=(udp_socket, segment, server_address, acked_segments, send_lock))
    timeout_thread.start()


def handle_timeout(udp_socket, segment, server_address, acked_segments, send_lock):
    time.sleep(1)  # Wait for a timeout before resending
    with send_lock:
        if segment['sequence_number'] not in acked_segments:
            logging.debug(f"Timeout occurred for segment {segment['sequence_number']}")
            send_segment(udp_socket, segment, server_address)
            logging.info(f"Resending segment {segment['sequence_number']} due to timeout")



def start_client(server_ip, server_port, window_size=10):
    # main function for the client
    # It sends file segments to the server, ensuring that the number of unacknowledged
    # segments does not exceed the window size.
    server_address = (server_ip, server_port)
    udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

    # defining the file amount
    FILE_COUNT = 3

    file_paths = [f"../../objects/small-{i}.obj" for i in range(FILE_COUNT)] + \
                    [f"../../objects/large-{i}.obj" for i in range(FILE_COUNT)] 
                 
    segment_size = 1024  # Defining the segment size

    window_size = 10  # Defining the window size, must be smaller than segment size

    segments = create_segments_for_files(file_paths, segment_size)

    sent_segments = set()
    acked_segments = set()
    base = { 'value': 0, 'lock': Lock()}

    send_lock = Lock()

    ack_thread = Thread(target=receive_acks, args=(udp_socket, acked_segments, base))
    ack_thread.daemon = True  # Daemonize the thread so it dies when the main thread dies
    ack_thread.start()

    # Loop through all segments and send them according to the Selective Repeat logic
    for segment in segments:
        with base['lock']:
            # Check if the segment is within the sending window
            while segment['sequence_number'] >= base['value'] + window_size:
                # If the window is full, wait for an ack to slide the window
                time.sleep(0.2)

        # Send the segment and start the timeout process
        send_segment_with_timeout(udp_socket, segment, server_address, acked_segments, send_lock)

        # Send the segment and handle timeout if not acknowledged
        with send_lock:
            sent_segments.add(segment['sequence_number'])

        # Wait a short time before attempting to send the next segment
        time.sleep(0.1)

    # Wait for all segments to be acknowledged before closing
    while len(acked_segments) < len(segments):
        time.sleep(0.2)

    udp_socket.close()
    print("All segments have been acknowledged. Transfer complete.")

if __name__ == "__main__":
    start_client("127.0.0.1", 8000)
