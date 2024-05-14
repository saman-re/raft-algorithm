import sys
import grpc
import json
from proto import raft_pb2, raft_pb2_grpc
from concurrent import futures
import time


def serve(node_id, config_path):
    config = load_config(config_path)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    node_config = next(n for n in config['nodes'] if n['id'] == node_id)
    local_address = node_config['address']
    server.add_insecure_port(local_address)
    server.start()
    node_config['state'] = 'ready'
    make_server_ready(config_path, config)
    try:
        while True:
            time.sleep(86400)  # Sleep the main thread for a day
    except KeyboardInterrupt:
        server.stop(0)
        node_config['state'] = 'not_ready'
        make_server_ready(config_path, config)


def load_config(config_path):
    with open(config_path, 'r') as file:
        return json.load(file)


def make_server_ready(config_path, config):
    with open(config_path, 'w') as file:
        json.dump(config, file, indent=4)


if __name__ == '__main__':
    serve(sys.argv[1], "./config.json")
