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
    # checking if the file exists
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
                # we learned that we dont need checksum because the UDP handles that
            }
            all_segments.append(segment)
            sequence_number += 1 # increment the sequence number its specific for the file now will be changed later with global seq nmber

    # set the last segment flag to true
    all_segments[-1]['is_last_segment'] = True # this is needed to assemble the files correctly

    return all_segments

def interleave_segments(segments):
    """
        this function interleaves the segments
        as stated in the homework definition we wanted to interleave the segments like one small and one large segment from each file
        and we wanted to use the round robin method to interleave the segments
        it would be easier to track the seq numbers and the file ids
    """

    interleaved_segments = []

    global_sequence_number = 0 # reassign the sequence numbers for all the segments
    # Interleave the segments
    while any(segments):  # Continue until all segments are empty
        for i in range(len(segments)):
            if len(segments[i]) > 0:  # Check if the current segment is not empty
                segment = segments[i].pop(0)  # Pop the first element of the non-empty segment
                segment['sequence_number'] = global_sequence_number # reassign the global sequence number
                interleaved_segments.append(segment)
                global_sequence_number += 1

    return interleaved_segments

def send_segment(udp_socket, segment, server_address):
    """
        Serializes and sends a  segment over a UDP socket we are using the pickle to serialize.
    """
    try:
        #  The segment dictionary is serialized with pickle
        serialized_segment = pickle.dumps(segment) 
     
        udp_socket.sendto(serialized_segment, server_address)        # Send the serialized segment to the server
        print(f"Segment {segment['sequence_number']} sent.") # for debugging purposes
    except Exception as e:
        print(f"Error sending segment: {e}")

def receive_ack(udp_socket, timeout=TIMEOUT_SLEEP):
    """
        Wait for an ACK from the server for a specific segment with a specified timeout.
        If the expected ACK is received, it returns the ACK.
        If a timeout occurs, it returns None.
        Note that we set a timeout here as well because we encountered with some stuck situation 
        while packet loss and delay scenarios
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
    """
        We implemented the Go-Back-N protocol for reliable data transfer over an unreliable network. (It end up better than our SR implementation)
        this function handles segment transmission, waiting for ACKs, and retransmission in timeout scenario.
    """
    # 4 states there is 
    # rdt send data
    # timeout
    # rdt receive ack and not corrupted
    # rdt receive ack and corrupted
    timer_start_time = None   # setting up the timer for the timeout event

    #  the code is implemented we got inspired by the FSM diagram in the book
    while(base < len(interleaved_segments)):   # send all the interleaved segments, base used as the window base

        if(next_seq_num < base + N):  # check if the window is not full
            # print(next_seq_num)
            if(next_seq_num == len(interleaved_segments)): # check if we reached the end of the segments
                break
            send_segment(udp_socket, interleaved_segments[next_seq_num], server_address) # send the segment
    
            if(base == next_seq_num):  # check if the window is empty and start the timer
                timer_start_time = time.time()
            else:
                timer_start_time = time.time()
            next_seq_num += 1
        else:
            # refuse data 
            pass

        ack = receive_ack(udp_socket) # receive the ack
        if(ack != None and is_not_corrupt(ack)): # check if the ack is not corrupted not corrupted is not necessary because udp already checks
            base = ack['acknowledged_sequence_number'] + 1              # update the base for the received ack
            if(base == next_seq_num):           # if base is equal to next seq num stop the timer
                timer_start_time = None
            else:
                timer_start_time = time.time()
        
        # this is checking the timeout event if timeout occurs we should resend the segments that are in the window
        if(timer_start_time != None and time.time() - timer_start_time > timeout_duration):
            timer_start_time = time.time()
            
            for i in range(base-1, next_seq_num):
                print(f"Segment resending segment...")
                send_segment(udp_socket, interleaved_segments[i], server_address)

        
    print("All segments are sent")


def start_client(server_ip, server_port, window_size=100):
    """
        main function for the client
        it creates the segments for the files, it interleaves the segments, it calls the GBN_sender function
        and it calculates the time taken to send all the files and the throughput
    """
    server_address = (server_ip, server_port)
    udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

    FILE_COUNT = 10

    # read all the files and append them to the file_paths list
    file_paths = []
    for i in range(FILE_COUNT):
        small_path = f"../../objects/small-{i}.obj"
        large_path = f"../../objects/large-{i}.obj"
        file_paths.append(small_path)
        file_paths.append(large_path)


    # print(file_paths)
               
    segment_size = 10 * 1024  # Defining the segment size, 

    window_size = WINDOW_SIZE # Defining the window size, must be smaller than segment size


    # Create segments for each file
    each_segments = []
    i = 0
    for file_path in file_paths:
        each_segments.append(create_segment_for_file(file_path,segment_size,i))
        i+=1

    
    interleaved_segments = interleave_segments(each_segments)

    # print(len(interleaved_segments))

    # since sequence numbers of the segments starts with 0 below values also starts with 0
    base = 0
    next_seq_num = 0
    N = window_size  # window size

    # Start timer for the oldest unacknowledged packet
    timer_start_time = None
    timeout_duration = TIMEOUT_DURATION # Duration after which to consider a timeout has occurred

    start_time = time.time()

    # main function
    GBN_sender(udp_socket,server_address, base, next_seq_num, N, interleaved_segments, timer_start_time, timeout_duration)

    end_time = time.time()  # End time
    elapsed_time = end_time - start_time

    # Open a file in write mode. If the file doesn't exist, it will be created.
    with open('elapsed_time_UDP.txt', 'a') as file:
        file.write(str(elapsed_time) + '\n')

    print(f"Total time taken for file transfer: {elapsed_time} seconds")
    print(f"Average throughput: {FILE_COUNT * segment_size * 8 / elapsed_time} bits per second")


    udp_socket.close()


if __name__ == "__main__":
    # IP = "127.0.0.1"
    IP = "server"
    start_client(IP, 8000)


# tc qdisc add dev eth0 root netem delay 100ms 10ms
# tc qdisc add dev eth0 root netem delay 100ms 10ms distribution normal
# tc qdisc add dev eth0 root netem loss 15%
# tc qdisc del dev eth0 root
# tc qdisc add dev eth0 root netem duplicate 10%
# tc qdisc add dev eth0 root netem corrupt 10%
