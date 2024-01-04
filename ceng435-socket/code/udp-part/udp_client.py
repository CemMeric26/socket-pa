import socket
import os
import pickle
import time
import hashlib
from common import WINDOW_SIZE, TIMEOUT_DURATION, TIMEOUT_SLEEP


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

def create_segment_for_file(file_path, segment_size,file_id):
    """
    this function creates segments for the files
    each segment is dictonary containts some fields 
    like file_id, sequence_number, data, is_last_segment
    """
    all_segments = []
    # check if the file exists
    if not os.path.exists(file_path):
        print(f"File {file_path} does not exist.")
        return None
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
                'is_last_segment': False, # flag for the last segment
                # 'checksum': calculate_checksum(data) # checksum of the segment
            }
            all_segments.append(segment)
            sequence_number += 1

    # set the last segment flag to true
    all_segments[-1]['is_last_segment'] = True

    return all_segments

def interleave_segments(segments):
    # this function interleaves the segments
    interleaved_segments = []

    global_sequence_number = 0
    # Interleave the segments
    while any(segments):  # Continue until all segments are empty
        for i in range(len(segments)):
            if len(segments[i]) > 0:  # Check if the current segment is not empty
                segment = segments[i].pop(0)  # Pop the first element of the non-empty segment
                segment['sequence_number'] = global_sequence_number
                interleaved_segments.append(segment)
                global_sequence_number += 1

    return interleaved_segments

def send_segment(udp_socket, segment, server_address):
    # Serialize and send a segment over UDP
    try:
        #  The segment dictionary is serialized with pickle
        serialized_segment = pickle.dumps(segment)        # Serialize the segment using pickle
     
        # The sendto method of the socket
        udp_socket.sendto(serialized_segment, server_address)           # Send the serialized segment to the server
        print(f"Segment {segment['sequence_number']} sent.")
    except Exception as e:
        print(f"Error sending segment: {e}")

def receive_ack(udp_socket, timeout=TIMEOUT_SLEEP):
    """
    Wait for an ACK from the server for a specific segment with a specified timeout.
    If the expected ACK is received, it returns the ACK.
    If a timeout occurs, it returns None.
    """
    try:
        # Set the socket timeout
        udp_socket.settimeout(timeout)

        # Try to receive an ACK
        ack_data, _ = udp_socket.recvfrom(1024)  # buffer size
        ack = pickle.loads(ack_data)

        # Return the received ACK
        return ack
    except socket.timeout:
        # Handle the timeout case
        print("Timeout in receiving acknowledgment")
        return None
    except Exception as e:
        # Handle other errors
        print(f"Error in receiving acknowledgment: {e}")
        return None

def is_not_corrupt(segment):
    return True

def has_sequence_number(segment, expected_seq_num):
    return segment['sequence_number'] == expected_seq_num

def GBN_sender(udp_socket,server_address, base, next_seq_num, N, interleaved_segments, timer_start_time, timeout_duration):
    
    # 4 states there is 
    # rdt send data
    # timeout
    # rdt receive ack and not corrupted
    # rdt receive ack and corrupted
    timer_start_time = None

    while(base < len(interleaved_segments)):

        if(next_seq_num < base + N):
            # print(next_seq_num)
            send_segment(udp_socket, interleaved_segments[next_seq_num], server_address)
    
            if(base == next_seq_num):
                timer_start_time = time.time()
            else:
                timer_start_time = time.time()
            next_seq_num += 1
        else:
            # refuse data
            pass

        ack = receive_ack(udp_socket)
        if(ack != None and is_not_corrupt(ack)):
            base = ack['acknowledged_sequence_number'] + 1
            if(base == next_seq_num):
                timer_start_time = None
            else:
                timer_start_time = time.time()
        
        if(timer_start_time != None and time.time() - timer_start_time > timeout_duration):
            timer_start_time = time.time()

            for i in range(base-1, next_seq_num):
                print(f"Segment resending segment...")
                send_segment(udp_socket, interleaved_segments[i], server_address)

        
    print("All segments are sent")


def start_client(server_ip, server_port, window_size=100):
    # main function for the client
    # It sends file segments to the server, ensuring that the number of unacknowledged
    # segments does not exceed the window size.
    server_address = (server_ip, server_port)
    udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

    FILE_COUNT = 10

    file_paths = []
    for i in range(FILE_COUNT):
        small_path = f"../../objects/small-{i}.obj"
        large_path = f"../../objects/large-{i}.obj"
        file_paths.append(small_path)
        file_paths.append(large_path)


    # print(file_paths)
               
    segment_size = 10 * 1024  # Defining the segment size

    window_size = WINDOW_SIZE # Defining the window size, must be smaller than segment size

    each_segments = []
    i = 0
    for file_path in file_paths:
        each_segments.append(create_segment_for_file(file_path,segment_size,i))
        i+=1

    
    interleaved_segments = interleave_segments(each_segments)

    print(len(interleaved_segments))
    base = 0
    next_seq_num = 0
    N = window_size  # window size

    # Start timer for the oldest unacknowledged packet
    timer_start_time = None
    timeout_duration = TIMEOUT_DURATION # Duration after which to consider a timeout has occurred

    start_time = time.time()

    GBN_sender(udp_socket,server_address, base, next_seq_num, N, interleaved_segments, timer_start_time, timeout_duration)

    end_time = time.time()  # End time
    elapsed_time = end_time - start_time
    print(f"Total time taken for file transfer: {elapsed_time} seconds")
    print(f"Average throughput: {FILE_COUNT * segment_size * 8 / elapsed_time} bits per second")



    udp_socket.close()


if __name__ == "__main__":
    # IP = "127.0.0.1"
    IP = "server"
    start_client(IP, 8000)


# tc qdisc add dev eth0 root netem delay 100ms 50ms
# tc qdisc add dev eth0 root netem loss 5%
# tc qdisc del dev eth0 root
