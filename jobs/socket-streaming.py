import json
import socket
import time
import pandas as pd


def send_data_over_socket(file_path, host="127.0.0.1", port=9999, chunk_size=2):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host, port))
    s.listen(1)
    print(f"Listening for connections on {host}:{port}")

    last_sent_index = 0
    while True:
        conn, addr = s.accept()
        print(f"Connection from {addr}")
        try:
            with open(file_path, "r") as file:
                # Skip lines which are already sent
                for _ in range(last_sent_index):
                    next(file)

                records = []
                for line in file:
                    records.append(json.loads(line))
                    if (len(records)) == chunk_size:
                        chunk = pd.DataFrame(records)
                        print(chunk)
                        for record in chunk.to_dict(orient="records"):
                            serialize_data = json.dumps(records).encode("utf-8")
                            conn.send(serialize_data + b"\n")
                            time.sleep(5)
                            last_sent_index += 1
                        records = []
        except (BrokenPipeError, ConnectionResetError):
            print("Client Disconnected.")
        finally:
            conn.close()
            print("Connection Closed.")


if __name__ == "__main__":
    send_data_over_socket("datasets/review.json")
