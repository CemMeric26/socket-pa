import socket
import os
import pickle
import time
import hashlib

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

def send_segment(udp_socket, segment, server_address):
    # Serialize and send a segment over UDP
    try:
        #  The segment dictionary is serialized with pickle
        serialized_segment = pickle.dumps(segment)        # Serialize the segment using pickle
     
        # The sendto method of the socket
        udp_socket.sendto(serialized_segment, server_address)           # Send the serialized segment to the server
    except Exception as e:
        print(f"Error sending segment: {e}")

def receive_ack(udp_socket, expected_seq_num):
    # this function waits for an ack for the segment from the server
    # ack is expected for a specific segment
    try:
        while True:
            ack_data, _ = udp_socket.recvfrom(1024)  # buffer size
            ack = pickle.loads(ack_data)         
            # Check if the acknowledgment is for the expected segment
            if ack.get('acknowledged_sequence_number') == expected_seq_num:
                print(f"Acknowledgment received for segment {expected_seq_num}")
                return ack
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

def GBN_sender(udp_socket,server_address, base, next_seq_num, N, interleaved_segments, timer_start_time, timeout_duration):
    
    sent_segments = set()
    acked_segments = set()
    timer_start_time = None  # Initialize the timer to None

    while base < len(interleaved_segments):
        # Send segments that are within the window
        # print(f"Acknowledgment received for segment {acked_seq_num}")
        while next_seq_num < base + N and next_seq_num <= len(interleaved_segments):
            segment = interleaved_segments[next_seq_num ]  # sequence numbers are 1-indexed
            send_segment(udp_socket, segment, server_address)
            sent_segments.add(segment["sequence_number"])

            # print(f"Sending segment {seq_num}, Timer: {time.time() - timer_start_time:.2f}")
            print(f"Current window: [{base}, {base + N - 1}]")
            print(f"Sent segment {segment['sequence_number']}")

            if base == next_seq_num:
                # Start timer upon sending the first segment in the window
                timer_start_time = time.time()
            next_seq_num += 1

        # Check for ACKs
        ack = receive_ack(udp_socket, base)
        if ack is not None:
            acked_seq_num = ack['acknowledged_sequence_number']
            if acked_seq_num in sent_segments:
                print(f"Received ACK for segment {acked_seq_num}")
                acked_segments.add(acked_seq_num)
                if acked_seq_num >= base:
                    # Move the base up to the next unacknowledged segment
                    while base in acked_segments:
                        base += 1
                        acked_segments.remove(base-1)
                        # Restart timer upon receiving an ACK for the base segment
                    
                    timer_start_time = time.time()
        
        # Check for timeout
        print(f"Timer timeout time: {time.time()-timer_start_time}")

        if timer_start_time and ((time.time() - timer_start_time) > timeout_duration):
            print(f"Timeout for segment {base}, resending...")
            next_seq_num = base

            timer_start_time = time.time()  # Restart timer for the oldest unacknowledged packet

            # Resend all segments starting from 'base' up to 'next_seq_num'
            for seq_num in range(base, min(base + N, len(interleaved_segments))):
                segment = interleaved_segments[seq_num]
                send_segment(udp_socket, segment, server_address)
                print(f"Resent segment {segment['sequence_number']}")

            # timer_start_time = time.time()  # Restart timer for the oldest unacknowledged packet
            # Resend all segments starting from 'base' up to 'next_seq_num - 1'
            """
            for seq_num in range(base, next_seq_num):
                segment = interleaved_segments[seq_num]  # sequence numbers are 1-indexed
                send_segment(udp_socket, segment, server_address)
                """


    print("All segments have been acknowledged.")

        

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
               
    segment_size = 1024  # Defining the segment size

    window_size = 10  # Defining the window size, must be smaller than segment size

    sent_segments = set()
    acked_segments = set()

    each_segments = []
    i = 0
    for file_path in file_paths:
        each_segments.append(create_segment_for_file(file_path,segment_size,i))
        i+=1

    
    interleaved_segments = interleave_segments(each_segments)

    # print(len(interleaved_segments))
    base = 0
    next_seq_num = 0
    N = window_size  # window size

    # Start timer for the oldest unacknowledged packet
    timer_start_time = None
    timeout_duration = 0.5  # Duration after which to consider a timeout has occurred

    start_time = time.time()

    GBN_sender(udp_socket,server_address, base, next_seq_num, N, interleaved_segments, timer_start_time, timeout_duration)

    end_time = time.time()  # End time
    elapsed_time = end_time - start_time
    print(f"Total time taken for file transfer: {elapsed_time} seconds")
    print(f"Average throughput: {FILE_COUNT * segment_size * 8 / elapsed_time} bits per second")



    udp_socket.close()
"""
    for segment in interleaved_segments:
        # print(f"Sending segment {segment['file_id']}-{segment['sequence_number']}")
        
        # wait if the number of unacknowledged segments is equal to the window size
        while len(sent_segments) - len(acked_segments) >= window_size:
            time.sleep(0.1)  # Wait before sending more segments

        send_and_wait_for_ack(udp_socket, segment, server_address)
        sent_segments.add(segment["sequence_number"])
        acked_segments.add(segment["sequence_number"]) # when ack is received, add it to acked_segments

"""


if __name__ == "__main__":
    # IP = "127.0.0.1"
    IP = "server"
    start_client(IP, 8000)
