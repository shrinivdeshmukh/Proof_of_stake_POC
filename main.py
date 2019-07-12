import json as JSON
import os
import websockets
import random
import requests

from sanic import Sanic
from sanic.response import json
from websockets.exceptions import ConnectionClosed

from blockchain import Block, Blockchain
from utils.logger import logger

import sqlite3 as sq

from Crypto.PublicKey import RSA


new_key = RSA.generate(4096, e=65537)
private_key = new_key.exportKey("PEM")
public_key = new_key.publickey().exportKey("PEM")


peerId = "PEER"+str(random.randint(1, 1000))
stake = random.randint(1, 300)
print("stakes for this peer are:\t", stake)

QUERY_LATEST = 0
QUERY_ALL = 1
RESPONSE_BLOCKCHAIN = 2
sq.connect('/home/shree/.blockchain/{}.sqlite'.format(peerId)).close()
db = sq.connect('/home/shree/.blockchain/{}.sqlite'.format(peerId))
peer_list = []
peer_list_http = []
try:
    port = int(os.environ['PORT'])
    # host = str(os.environ['HOST'])
except KeyError as e:
    # logger.debug("Exception")
    port = 3001
    # host = '127.0.0.1'

try:
    host = os.environ["HOST"]
except Exception as e:
    host = '127.0.0.1'

try:
    initialPeers = os.environ['PEERS'].split(",")
except Exception as e:
    initialPeers = []


class Server(object):

    def __init__(self):

        self.app = Sanic()
        self.blockchain = Blockchain()
        self.sockets = []
        self.app.add_route(self.blocks, '/blocks', methods=['GET'])
        self.app.add_route(self.mine_block, '/mineBlock', methods=['POST'])
        self.app.add_route(self.peers, '/peers', methods=['GET'])
        self.app.add_route(self.add_peer, '/addPeer', methods=['POST'])
        self.app.add_route(self.elect, '/election', methods=['POST'])
        self.app.add_route(self.update_stake, 'updateStake', methods=['POST'] )
        self.app.add_websocket_route(self.p2p_handler, '/')
        self.app.add_route(self.send_message,'/sendMessage',methods=['POST'])
        self.app.add_websocket_route(self.receiveMessage,'/receiveMessage')

    async def blocks(self, request):
        return json(self.blockchain.blocks)

    async def elect(self, request):
        try:
            temp_peer_list=[]
            print("request json:\t",request.json)
            newData = {}
            newData["data"] = request.json["data"]
            newData["peer"] = request.json["peer"]
            temp_peer_list = request.json["temp_peer"]
            
           
            print("Inside elect---------",temp_peer_list)

            stakesResult = sorted(temp_peer_list, key=lambda x: int(x.split(';')[1]), reverse=True)
            leader = stakesResult[0].split(';')[0]
            print("$$$$$$$$$$$LEADER IS:\t{}".format(leader))
            if  "{}:{}".format(host,port) in leader:                
                requestObject = {}
                requestObject["json"] = newData
                print("requestObject mineblock:\t",requestObject)
                mine_block = await self.mine_block(JSON.dumps(newData))
                return json(mine_block)
        except Exception as e:
            print("error:\t{}".format(e))
            return json({"Error in leader election":"{}".format(e)}, status=401)
        

        

    async def send_message(self, request):
            from_peer = str(peer_list[0])
            to = request.json["to"]
            print("to is\t",to)
            message = request.json["message"]
            jsonData = {}
            jsonData["type"] = 0
            jsonData["message"] = format(message)
            cursor = db.cursor()
            query = 'insert into message(data) values ("{}")'.format(
                message)
            print("query is------------{}".format(query))
            cursor.execute(query)
            db.commit()
            ws = await websockets.connect(to)
            data = {"data":"p2p message","peer":"{}-{}".format(from_peer,to)}
            await ws.send(JSON.dumps(jsonData))

            newBlock = self.blockchain.generate_next_block(data)
            self.blockchain.add_block(newBlock)
            await self.broadcast(self.response_latest_msg())

            
            temp_peer_list = peer_list
            for i, peer in enumerate(temp_peer_list):
                if from_peer in temp_peer_list:
                    del temp_peer_list[i]
            
            for i, peer in enumerate(temp_peer_list):
                if str(to) in temp_peer_list[i]:
                    del temp_peer_list[i]



            print("peer inclusion list is:@@@@@@@@@@@@@@@@@@@\t{}".format(temp_peer_list))
            for peer in temp_peer_list:
                peerSplit = peer.split(';')[0]
                peerIp = "http:{}:{}".format(peerSplit.split(":")[1], peerSplit.split(":")[2])                
                data["temp_peer"] = temp_peer_list
                responseObject = requests.post("{}/election".format(peerIp), json=data)          
            
            
            return json(temp_peer_list)

    async def receiveMessage(self,request,ws):
        data = await ws.recv()
        message = JSON.loads(data)
        logger.info('Received p2p message: {}'.format(data))
        return json(message)

    async def send_handshake(self, peer):
        cursor = db.cursor()
        query = 'select public_key from my_keys where peer={}'.format(peer_list[0])
        cursor.execute(query)
        query_response = cursor.fetchall()
        query_result_to_str = ''.join(query_response[0])
        socket = self.sockets[-1]
        await socket.send(JSON.dumps(query_result_to_str))

    async def http_peer(self, peer):
        peer_updated = []
        try:
            peer_info = peer.replace('ws','http').split(';')[0]
            peer_updated.append(peer_info)
            return peer_updated
        except Exception as e:
            return [format(e)]

    async def mine_block(self, data):

        try:
            newData = data
            print("Inside mineblock:\t",newData)
            newData['peer'] = peerId
            print("newData block ----------------------", newData)
            newBlock = self.blockchain.generate_next_block(newData)
            print("new block here is:\t---", newBlock)
        except KeyError as e:
            return json({"status": False, "message": "pass value in data key"})
        
        self.blockchain.add_block(newBlock)
        await self.broadcast(self.response_latest_msg())

        return json(newBlock)

    async def peers(self, request):
        peers = map(lambda x: "{}:{}".format(
            x.remote_address[0], x.remote_address[1]), self.sockets)
        return json(peers)

    async def add_peer(self, request):
        import asyncio
        peerName = request.json["peer"]
        asyncio.ensure_future(self.connect_to_peers([peerName]),
                              loop=asyncio.get_event_loop())
        return json({"status": True})

    async def connect_to_peers(self, newPeers):
        for peer in newPeers:
            logger.info(peer)
            try:
                peerName = str(peer).split(';')[0]
                ws = await websockets.connect(peerName)
                print("ws########################", peerName)
                peer_list.append(peer)
                print("peer_list -----------\t", peer_list)
                await self.init_connection(ws)
            except Exception as e:
                logger.info(str(e))

    async def update_stake(self, request):
        
        logger.info("Updating stake for peer")
        peer = request.json["peer"]
        new_stake = request.json["stake"]
        for i, peer_info in enumerate(peer_list):
            if peer in peer_info:
                peer_name = str(peer_list[i]).split(';')[0]
                peer_info_updated = peer_name +";" + new_stake
                peer_list[i] = peer_info_updated
        return json(peer_list)

    # initP2PServer WebSocket server
    async def p2p_handler(self, request, ws):
        logger.info('listening websocket p2p port on: %d' % port)

        try:
            await self.init_connection(ws)
        except (ConnectionClosed):
            await self.connection_closed(ws)
            

    async def connection_closed(self, ws):

        logger.critical("connection failed to peer")
        self.sockets.remove(ws)

    async def init_connection(self, ws):

        self.sockets.append(ws)
        await ws.send(JSON.dumps(self.query_chain_length_msg()))

        while True:
            await self.init_message_handler(ws)

    async def init_message_handler(self, ws):
        data = await ws.recv()
        message = JSON.loads(data)
        logger.info('Received message: {}'.format(data))

        await {
            QUERY_LATEST: self.send_latest_msg,
            QUERY_ALL: self.send_chain_msg,
            RESPONSE_BLOCKCHAIN: self.handle_blockchain_response
        }[message["type"]](ws, message)

    async def send_latest_msg(self, ws, *args):
        await ws.send(JSON.dumps(self.response_latest_msg()))

    async def send_chain_msg(self, ws, *args):

        await ws.send(JSON.dumps(self.response_chain_msg()))

    def response_chain_msg(self):
        return {
            'type': RESPONSE_BLOCKCHAIN,
            'data': JSON.dumps([block.dict() for block in self.blockchain.blocks])
        }

    def response_latest_msg(self):

        return {
            'type': RESPONSE_BLOCKCHAIN,
            'data': JSON.dumps([self.blockchain.get_latest_block().dict()])
        }

    async def handle_blockchain_response(self, ws, message):

        received_blocks = sorted(JSON.loads(
            message["data"]), key=lambda k: k['index'])
        logger.info(received_blocks)
        latest_block_received = received_blocks[-1]
        latest_block_held = self.blockchain.get_latest_block()
        if latest_block_received["index"] > latest_block_held.index:
            logger.info('blockchain possibly behind. We got: ' + str(latest_block_held.index)
                        + ' Peer got: ' + str(latest_block_received["index"]))
            if latest_block_held.hash == latest_block_received["previous_hash"]:
                logger.info("We can append the received block to our chain")

                self.blockchain.blocks.append(Block(**latest_block_received))
                await self.broadcast(self.response_latest_msg())
            elif len(received_blocks) == 1:
                logger.info("We have to query the chain from our peer")
                await self.broadcast(self.query_all_msg())
            else:
                logger.info(
                    "Received blockchain is longer than current blockchain")
                await self.replace_chain(received_blocks)
        else:
            logger.info(
                'received blockchain is not longer than current blockchain. Do nothing')

    async def replace_chain(self, newBlocks):

        try:

            if self.blockchain.is_valid_chain(newBlocks) and len(newBlocks) > len(self.blockchain.blocks):
                logger.info('Received blockchain is valid. Replacing current blockchain with '
                            'received blockchain')
                self.blockchain.blocks = [
                    Block(**block) for block in newBlocks]
                await self.broadcast(self.response_latest_msg())
            else:
                logger.info('Received blockchain invalid')
        except Exception as e:
            logger.info("Error in replace chain" + str(e))

    def query_chain_length_msg(self):

        return {'type': QUERY_LATEST}

    def query_all_msg(self):

        return {'type': QUERY_ALL}

    async def broadcast(self, message):

        for socket in self.sockets:
            logger.info(socket)
            await socket.send(JSON.dumps(message))


if __name__ == '__main__':

    server = Server()
    cursor = db.cursor()
    print("Id of this peer is peerId", peerId)
    try:
        cursor.execute('create table message(data varchar(100))')
        cursor.execute('create table my_keys(peer varchar(100), public_key varchar(800), private_key varchar(800))')
        cursor.execute('insert into my_keys(peer,public_key, private_key) values("{}","{}","{}")'.format(peer_list[0], public_key, private_key))
        cursor.execute('create table their_keys(peer varchar(100), public_key varchar(800))')
        db.commit()
    except Exception as e:
        print("Database already exists!")
    myPeerInfo = 'ws://{}:{};{}'.format(host, port, stake)
    peer_list.append(myPeerInfo)
    print("Initializing peer list:", peer_list)
    server.app.add_task(server.connect_to_peers(initialPeers))
    server.app.run(host=host, port=port, debug=True)
