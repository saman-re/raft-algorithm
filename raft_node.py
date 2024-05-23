import sys
import grpc
import json
from proto import raft_pb2, raft_pb2_grpc
from concurrent import futures
import threading
import random
import time
from pymongo import MongoClient


class RaftNode(raft_pb2_grpc.RaftServiceServicer):
    def __init__(self, node_id, config):
        self.vote_count = None
        self.node_id = node_id
        self.peers = {n['id']: n['address'] for n in config['nodes'] if n['id'] != node_id and n['state'] == 'ready'}
        self.current_term = 0
        self.voted_for = None
        self.role = 'follower'  # follower, candidate, leader

        # MongoDB client setup
        self.client = MongoClient('mongodb://admin:admin@localhost:27017')
        self.db = self.client[f'raft_db_{self.node_id}']
        self.logs_collection = self.db[f'logs_{self.node_id}']
        self.state_collection = self.db[f'state_{self.node_id}']

        self.next_index = {peer_id: self.logs_collection.count_documents({}) for peer_id in self.peers}
        self.match_index = {peer_id: 0 for peer_id in self.peers}
        self.commit_index = 0
        self.last_applied = 0

        # self.update_peers_timeout = threading.Timer(1, self.update_peers, args=(config,))
        # self.update_peers_timeout.start()

        self.election_timeout = random.uniform(1.5, 2.5)  # random timeout between 1.5 and 2.5 seconds
        self.election_timer = threading.Timer(self.election_timeout, self.start_election)
        self.election_timer.start()

        saved_state = self.state_collection.find_one({"node_id": self.node_id})
        if saved_state:
            self.current_term = saved_state['current_term']
            self.voted_for = saved_state.get('voted_for')

    def update_peers(self, config):
        self.peers = {n['id']: n['address'] for n in config['nodes'] if
                      n['id'] != self.node_id and n['state'] == 'ready'}
        self.next_index = {peer_id: self.logs_collection.count() for peer_id in self.peers}
        self.match_index = {peer_id: 0 for peer_id in self.peers}

    def reset_election_timer(self):
        """Resets the election timer with a random timeout."""
        if self.election_timer:
            self.election_timer.cancel()  # Stop the current timer if it is running

        # Set the timeout to a random value (for example, between 150ms to 300ms)
        self.election_timer = threading.Timer(self.election_timeout, self.start_election)
        self.election_timer.start()

    def start_election(self):
        with threading.Lock():
            self.role = 'candidate'
            self.current_term += 1
            self.voted_for = self.node_id
            self.vote_count = 1
            self.state_collection.update_one({"node_id": self.node_id},
                                             {"$set": {"current_term": self.current_term, "voted_for": self.voted_for}},
                                             upsert=True)

            for peer_id in self.peers:
                self.send_vote_request(peer_id)

    def send_vote_request(self, peer_id):
        channel = grpc.insecure_channel(self.peers[peer_id])
        stub = raft_pb2_grpc.RaftServiceStub(channel)
        last_log_list = list(self.logs_collection.find().sort("index", -1).limit(1))
        last_log_index = last_log_list[0]['index'] if last_log_list else 0
        last_log_term = last_log_list[0]['term'] if last_log_list else 0

        request = raft_pb2.VoteRequest(term=self.current_term, candidateId=self.node_id, lastLogIndex=last_log_index,
                                       lastLogTerm=last_log_term)
        response = stub.RequestVote(request)
        if response.voteGranted:
            self.vote_count += 1
            if self.vote_count > len(self.peers) // 2:
                self.become_leader()

    def RequestVote(self, request, context):
        term = request.term
        candidateId = request.candidateId
        lastLogIndex = request.lastLogIndex
        lastLogTerm = request.lastLogTerm

        last_log_list = list(self.logs_collection.find().sort("index", -1).limit(1))
        last_log_index = last_log_list[0]['index'] if last_log_list else 0
        last_log_term = last_log_list[0]['term'] if last_log_list else 0
        if (term > self.current_term or
                (term == self.current_term and
                 (lastLogTerm > last_log_term or
                  (lastLogTerm == last_log_term and lastLogIndex >= last_log_index)))):

            self.current_term = term
            self.voted_for = candidateId

            return raft_pb2.VoteResponse(term=self.current_term, voteGranted=True)
        else:
            return raft_pb2.VoteResponse(term=self.current_term, voteGranted=False)

    def become_leader(self):
        self.role = 'leader'
        self.send_heartbeats_periodically()

    def send_heartbeats_periodically(self):
        while self.role == 'leader':
            for peer_id in self.peers:
                self.send_append_entries(peer_id)
            time.sleep(0.5)

    def send_append_entries(self, peer_id):
        channel = grpc.insecure_channel(self.peers[peer_id])
        stub = raft_pb2_grpc.RaftServiceStub(channel)
        prev_log_index = self.next_index[peer_id] - 1
        prev_log_term = self.logs_collection.find_one({"index": prev_log_index})['term'] if prev_log_index >= 0 else 0
        entries = list(self.logs_collection.find({"index": {"$gte": self.next_index[peer_id]}}))

        request = raft_pb2.AppendEntriesRequest(
            term=self.current_term, leaderId=self.node_id,
            prevLogIndex=prev_log_index, prevLogTerm=prev_log_term,
            entries=entries, leaderCommit=self.commit_index
        )

        try:
            response = stub.AppendEntries(request)
            if response.success:
                # Follower appended the entries successfully
                self.match_index[peer_id] = request.prevLogIndex + len(request.entries)
                self.next_index[peer_id] = self.match_index[peer_id] + 1

                # Update commit index if majority of followers have acknowledged
                for n in sorted(self.match_index.values(), reverse=True):
                    if (n > self.commit_index and
                            self.logs_collection.find_one({"index": n})['term'] == self.current_term):
                        if list(self.match_index.values()).count(n) > len(self.peers) // 2:
                            self.commit_index = n
                            break
            else:
                # Log inconsistency, decrement next index and retry
                self.next_index[peer_id] = max(0, self.next_index[peer_id] - 1)
        except grpc.RpcError as e:
            print(f"Failed to send AppendEntries to {peer_id}: {str(e)}")

    def AppendEntries(self, request, context):
        if request.term < self.current_term:
            return raft_pb2.AppendEntriesResponse(term=self.current_term, success=False)

        self.reset_election_timer()

        last_log_list = list(self.logs_collection.find().sort("index", -1).limit(1))
        last_log_index = last_log_list[0]['index'] if last_log_list else 0
        if request.prevLogIndex >= last_log_index or (
                last_log_list[request.prevLogIndex].term if request.prevLogIndex >= 0 else 0) != request.prevLogTerm:
            return raft_pb2.AppendEntriesResponse(term=self.current_term, success=False)

        entry_index = request.prevLogIndex + 1
        while entry_index < last_log_index and entry_index - request.prevLogIndex - 1 < len(request.entries) and \
                last_log_list[entry_index].term != request.entries[entry_index - request.prevLogIndex - 1].term:
            self.logs_collection.delete_one({'index': entry_index})

        if entry_index - request.prevLogIndex - 1 < len(request.entries):
            for new_entry in request.entries:
                self.logs_collection.insert_one({'index': new_entry.index, 'term': new_entry.term})

        if request.leaderCommit > self.commit_index:
            self.commit_index = min(request.leaderCommit, last_log_index - 1)


        return raft_pb2.AppendEntriesResponse(term=self.current_term, success=True)


def serve(node_id, config_path):
    config = load_config(config_path)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    node = RaftNode(node_id, config)
    raft_pb2_grpc.add_RaftServiceServicer_to_server(node, server)

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
