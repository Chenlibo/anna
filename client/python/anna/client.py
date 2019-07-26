#  Copyright 2019 U.C. Berkeley RISE Lab
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import os
import random
import socket
import zmq

from .lattices import *
from .common import *
from .zmq_util import *

from .anna_pb2 import *

class AnnaClient():
    def __init__(self, elb_addr, ip, elb_ports=list(range(6450, 6454)), offset=0):
        '''
        The AnnaClient allows you to interact with a local copy of Anna or with
        a remote cluster running on AWS.

        elb_addr: Either 127.0.0.1 (local mode) or the address of an AWS ELB
        for the routing tier
        ip: The IP address of the machine being used -- if None is provided, one
        is inferred by using socket.gethostbyname(); WARNING: this does not
        always work
        elb_ports: The ports on which the routing tier will listen; use 6450 if
        running in local mode, otherwise do not change
        offset: A port numbering offset, which is only needed if multiple
        clients are running on the same machine
        '''
        assert type(elb_addr) == str, \
            'ELB IP argument must be a string.'

        self.elb_addr = elb_addr
        self.elb_ports = elb_ports

        if ip:
            self.ut = UserThread(ip, offset)
        else:
            self.ut = UserThread(socket.gethostbyname(socket.gethostname()), offset)

        self.context = zmq.Context(1)

        self.address_cache = {}
        self.pusher_cache = SocketCache(self.context, zmq.PUSH)

        self.response_puller = self.context.socket(zmq.PULL)
        self.response_puller.bind(self.ut.get_request_pull_bind_addr())

        self.key_address_puller = self.context.socket(zmq.PULL)
        self.key_address_puller.bind(self.ut.get_key_address_bind_addr())

        self.rid = 0


    def get(self, key):
        '''
        Retrieves a key from the key value store.

        key: The name of the key being retrieved.

        returns: A Lattice containing the server's response
        '''
        worker_address = self._get_worker_address(key)

        if not worker_address:
            return None

        send_sock = self.pusher_cache.get(worker_address)

        req, _ = self._prepare_data_request(key)
        req.type = GET

        send_request(req, send_sock)
        response = recv_response([req.request_id], self.response_puller,
                KeyResponse)[0]

        # we currently only support single key operations
        tup = response.tuples[0]

        if tup.invalidate:
            self._invalidate_cache(tup.key)

        if tup.error == 0:
            return self._deserialize(tup)
        else:
            return None # key does not exist

    def get_all(self, key):
        '''
        Retrieves all versions of the keys from the KVS; there may be multiple
        versions because the KVS is eventually consistent.

        key: The name of the key being retrieved

        returns: A list of Lattices with all the key versions returned by the
        KVS
        '''
        worker_addresses = self._get_worker_address(key, False)

        if not worker_addresses:
            return None

        req, _ = self._prepare_data_request(key)
        req.type = GET

        req_ids = []
        for address in worker_addresses:
            req.request_id = self._get_request_id()

            send_sock = self.pusher_cache.get(address)
            send_request(req, send_sock)

            req_ids.append(req.request_id)

        responses = recv_response(req_ids, self.response_puller, KeyResponse)

        for resp in responses:
            tup = resp.tuples[0]
            if tup.invalidate:
                # reissue the request
                self._invalidate_cache(tup.key)
                return self.get_all(key)

            if tup.error != 0:
                return None

        return list(map(lambda resp: self._deserialize(resp), responses))


    def put_all(self, key, value):
        '''
        Puts a new key into the key value store and waits for acknowledgement
        from all key replicas.

        key: The name of the key being put
        value: A Lattice with the data corresponding to this key

        returns: True if all replicas acknowledged the request or False if a
        replica returned an error or could not be reached
        '''
        worker_addresses = self._get_worker_address(key, False)

        if not worker_addresses:
            return False

        req, tup = self._prepare_data_request(key)
        req.type = PUT
        tup.payload, tup.lattice_type = self._serialize(value)
        tup.timestamp = 0

        req_ids = []
        for address in worker_addresses:
            req.request_id = self._get_request_id()

            send_sock = self.pusher_cache.get(address)
            send_request(req, send_sock)

            req_ids.append(req.request_id)

        responses = recv_response(req_ids, self.response_puller, KeyResponse)

        for resp in responses:
            tup = resp.tuples[0]
            if tup.invalidate:
                # reissue the request
                self._invalidate_cache(tup.key)
                return self.durable_put(key, value)

            if tup.error != 0:
                return False

        return True


    def put(self, key, value):
        '''
        Puts a new key into the KVS.

        key: The name of the key being put
        value: A lattice with the data corresponding to this key

        returns: True if the server responded without an error, and False
        otherwise or if the server could not be reached
        '''
        worker_address = self._get_worker_address(key)

        if not worker_address:
            return False

        send_sock = self.pusher_cache.get(worker_address)

        req, tup = self._prepare_data_request(key)
        req.type = PUT
        tup.payload, tup.lattice_type = self._serialize(value)

        send_request(req, send_sock)
        response = recv_response([req.request_id], self.response_puller,
                KeyResponse)[0]

        # we currently only support single key operations
        tup = response.tuples[0]

        if tup.invalidate:
            self._invalidate_cache(tup.key)

            # re-issue the request
            return self.put(tup.key)

        return tup.error == 0

    # Takes a KeyTuple (defined in hydro-project/common/proto/anna.proto) as an
    # input and returns either a LWWPairLattice or a SetLattice.
    # TODO: add support for other lattice types stored in the KVS
    def _deserialize(self, tup):
        if tup.lattice_type == LWW:
            val = LWWValue()
            val.ParseFromString(tup.payload)

            return LWWPairLattice(val.timestamp, val.value)
        elif tup.lattice_type == SET:
            s = SetValue()
            s.ParseFromString(tup.payload)

            result = set()
            for k in s.values:
                result.add(k)

            return SetLattice(result)

    # Takes in a Lattice subclass and returns a bytestream representing a
    # serialized Protobuf message.
    # TODO: add support for other lattice types stored in the KVS
    def _serialize(self, val):
        if isinstance(val, LWWPairLattice):
            lww = LWWValue()
            lww.timestamp = val.ts
            lww.value = val.val
            return lww.SerializeToString(), LWW
        elif isinstance(val, SetLattice):
            s = SetValue()
            for o in val.reveal():
                s.values.append(o)

            return s.SerializeToString(), SET

    # Helper function to create a KeyRequest (see
    # hydro-project/common/proto/anna.proto). Takes in a key name and returns a
    # tuple containing a KeyRequest and a KeyTuple contained in that KeyRequest
    # with response_address, request_id, and address_cache_size automatically
    # populated.
    def _prepare_data_request(self, key):
        req = KeyRequest()
        req.request_id = self._get_request_id()
        req.response_address = self.ut.get_request_pull_connect_addr()
        tup = req.tuples.add()

        tup.key = key
        tup.address_cache_size = len(self.address_cache[key])

        return (req, tup)


    # Returns and increments a request ID. Loops back after 10,000 requests.
    def _get_request_id(self):
        response = self.ut.get_ip() + ':' + str(self.rid)
        self.rid = (self.rid + 1) % 10000
        return response


    # Returns the worker address for a particular key. If worker addresses for
    # that key are not cached locally, a query is synchronously issued to the
    # routing tier, and the address cache is updated.
    def _get_worker_address(self, key, pick=True):
        if key not in self.address_cache:
            port = random.choice(self.elb_ports)
            addresses = self._query_routing(key, port)
            self.address_cache[key] = addresses

        if len(self.address_cache[key]) == 0:
            return None

        if pick:
            return random.choice(self.address_cache[key])
        else:
            return self.address_cache[key]


    # Invalidates the address cache for a particular key when the server tells
    # the client that its cache is out of date.
    def _invalidate_cache(self, key):
        del self.address_cache[key]


    # Issues a synchronous query to the routing tier. Takes in a key and a
    # (randomly chosen) routing port to issue the request to. Returns a list of
    # addresses that the routing tier returned that correspond to the input
    # key.
    def _query_routing(self, key, port):
        key_request = KeyAddressRequest()

        key_request.response_address = self.ut.get_key_address_connect_addr()
        key_request.keys.append(key)
        key_request.request_id = self._get_request_id()

        dst_addr = 'tcp://' + self.elb_addr  + ':' + str(port)
        send_sock = self.pusher_cache.get(dst_addr)

        send_request(key_request, send_sock)
        response = recv_response([key_request.request_id],
                self.key_address_puller,  KeyAddressResponse)[0]

        if response.error != 0:
            return []

        result = []
        for t in response.addresses:
            if t.key == key:
                for a in t.ips:
                    result.append(a)

        return result
