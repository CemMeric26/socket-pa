import socket
import os
import pickle
import time

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
                'is_last_segment': False # flag for the last segment
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


def start_client(server_ip, server_port, window_size=10):
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


    start_time = time.time()  # Start time
    for segment in interleaved_segments:
        # print(f"Sending segment {segment['file_id']}-{segment['sequence_number']}")
        
        # wait if the number of unacknowledged segments is equal to the window size
        while len(sent_segments) - len(acked_segments) >= window_size:
            time.sleep(0.1)  # Wait before sending more segments

        send_and_wait_for_ack(udp_socket, segment, server_address)
        sent_segments.add(segment["sequence_number"])
        acked_segments.add(segment["sequence_number"]) # when ack is received, add it to acked_segments

    end_time = time.time()  # End time
    elapsed_time = end_time - start_time
    print(f"Total time taken for file transfer: {elapsed_time} seconds")
    print(f"Average throughput: {FILE_COUNT * segment_size * 8 / elapsed_time} bits per second")
    
    udp_socket.close()

if __name__ == "__main__":
    start_client("server", 8000)
