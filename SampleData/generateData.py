import os
import time
import uuid

FILE_PATH = "./test_10gb.csv"
TARGET_SIZE_BYTES = 10 * 1024 * 1024 * 1024
CHUNK_SIZE = 50 * 1024 * 1024

def generate_large_file():
    start_time = time.time()
    
    dir_name = os.path.dirname(FILE_PATH)
    if dir_name:
        os.makedirs(dir_name, exist_ok=True)
    
    with open(FILE_PATH, 'w', encoding='utf-8') as f:
        header = "id,uuid,timestamp,padding_data,score\n"
        f.write(header)
        current_size = len(header.encode('utf-8'))
        
        row_template = "{},{},{},{},{}\n"
        padding = "X" * 500
        row_index = 0

        while current_size < TARGET_SIZE_BYTES:
            buffer = []
            buffer_size = 0
            
            while buffer_size < CHUNK_SIZE and current_size + buffer_size < TARGET_SIZE_BYTES:
                row = row_template.format(
                    row_index, 
                    str(uuid.uuid4()), 
                    int(time.time()), 
                    padding, 
                    row_index * 2
                )
                buffer.append(row)
                buffer_size += len(row)
                row_index += 1
            
            chunk = "".join(buffer)
            f.write(chunk)
            current_size += len(chunk.encode('utf-8'))
            
            print(f"Progress: {current_size / (1024**3):.2f} GB / 10.00 GB", end='\r')

    print(f"\nFinished creating 10GB file in {time.time() - start_time:.2f} seconds.")

if __name__ == "__main__":
    generate_large_file()