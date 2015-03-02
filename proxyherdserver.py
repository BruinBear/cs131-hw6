from twisted.protocols.basic import LineReceiver
from twisted.web.client import getPage
from twisted.internet.protocol import Factory, Protocol
from twisted.application import service, internet
from twisted.internet import reactor, protocol

import time, sys, logging, re, json

placesKey = "AIzaSyATYDSsfguWYSVji3uQWYMNoJy-sQIolHA"
placesUrl = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?"

portsMap = {
    'Alford': 12001,
    'Bolden': 12002,
    'Hamilton': 12003,
    'Parker': 12004,
    'Powell': 12005
    }
# portsMap = {'Alford': 12001}

canTalkTo = {
	"Alford" : ["Parker", "Powell"],
	"Bolden" : ["Parker", "Powell"],
	"Hamilton" : ["Parker"],
	"Parker" : ["Alford", "Bolden", "Hamilton"],
	"Powell" : ["Alford", "Bolden"]
    }


class ProxyHerdProtocol(LineReceiver):
    def __init__(self, factory):
        self.factory = factory

    # wrapper for calling log
    def logAndShow(self, msg):
        line = "({0}): {1}".format(self.factory.name, msg)
        logging.debug(line)
        print line

    def connectionMade(self):
        self.factory.num_clients += 1
        debug = "New client connected. Current Number of clients: {0}".format(self.factory.num_clients)
        self.logAndShow(debug)

    def connectionLost(self, reason):
        self.factory.num_clients -= 1
        debug = "A client disconnected. Current Number of clients: {0}".format(self.factory.num_clients)
        self.logAndShow(debug)


    def lineReceived(self, line):
        debug = "Received" + line
        self.logAndShow(debug)

        params = line.split(" ")
        # use the first parameter to determine handler
        if(params[0] == "IAMAT"):
            self.do_IAMAT(params)
        elif(params[0] == "AT"):
            self.do_AT(params)
        elif(params[0] == "WHATSAT"):
            self.do_WHATSAT(params)
        else:
            self.logAndShow('Bad Request.')

    def do_IAMAT(self, params):
        if len(params) != 4:
            self.logAndShow('Invalid IAMAT Request.')
            self.transport.write("Bad Request\n")
            return

        # parse params
        _, c_id, c_loc, c_time = params

        # Time comparison between sent timestamp and received local timestamp
        local_time = time.time()
        time_diff = local_time - float(c_time)

        # Format response
        if time_diff > 0:
            response = "AT {0} +{1} {2} {3} {4}".format(self.factory.name, time_diff,
                                                        c_id, c_loc, c_time)
        else:
            response = "AT {0} -{1} {2} {3} {4}".format(self.factory.name, time_diff,
                                                        c_id, c_loc, c_time)

        self.transport.write(response+"\n")
        # keep track of the latest IAMAT msg from every client
        if c_id not in self.factory.clients:
            self.logAndShow("New Client [IAMAT]: {0}".format(c_id))
            self.factory.clients[c_id] = {"msg":response, "time":c_time}

            # since we are unsure if the msg is the newest for other servers
            # we flood it
            self.updateLocation(response)
        else:
            # if we see a older client IAMAT msg we disregard
            if(self.factory.clients[c_id]['time'] > c_time):
                self.logAndShow("Existing Client Expired [IAMAT]: {0}".format(c_id))
                return
            else:
                self.logAndShow("Existing Client Updated [IAMAT]: {0}".format(c_id))
                self.factory.clients[c_id]["msg"] =response
                self.factory.clients[c_id]["time"] =c_time
                self.updateLocation(response)

        return

    def updateLocation(self, response):
        if len(canTalkTo[self.factory.name]) == 0:
            self.logAndShow("No neighbours for flooding.")
        else:
            for name in canTalkTo[self.factory.name]:
                self.logAndShow("Flooding to neighbour ({0})".format(name))
                reactor.connectTCP('localhost', portsMap[name], DumbClientFactory(response))

    def do_AT(self, params):
        if len(params) != 6:
            self.logAndShow('Invalid AT Request.')
            return
        # parse params
        _, s_id, time_diff, c_id, c_loc, c_time = params

        response = " ".join(params)
        self.logAndShow("Flood Received:" + response)

        # update the response server id
        params[4] = self.factory.name
        response = " ".join(params)

        # keep track of the latest IAMAT msg from every client
        if c_id not in self.factory.clients:
            self.logAndShow("New Client [IAMAT]: {0}".format(c_id))
            self.factory.clients[c_id] = {"msg":response, "time":c_time}
            # since we are unsure if the msg is the newest for other servers
            # we flood it
            self.updateLocation(response)
        else:
            # if we see a older client IAMAT msg we disregard
            if(self.factory.clients[c_id]['time'] >= c_time):
                self.logAndShow("Existing Client Expired [IAMAT]: {0}".format(c_id))
                return
            else:
                self.logAndShow("Existing Client Updated [IAMAT]: {0}".format(c_id))
                self.factory.clients[c_id] = {"msg":response, "time":c_time}
                self.updateLocation(response)
        return

    def do_WHATSAT(self, params):
        if len(params) != 4:
            self.logAndShow('Bad Requests.')
            self.transport.write("Bad Requess\n")
            return
        _, c_id, radius, num_entries = params
        if(radius > 50):
            radius = 50
        if(num_entries > 20):
            num_entries = 20

        if c_id not in self.factory.clients:
            self.logAndShow('Client location unknown.')
            self.transport.write("Client location unknown.\n")
            return

        _, _, _, _, c_loc, _ = self.factory.clients[c_id]['msg'].split(" ")

        c_loc = re.sub(r'[-]', ' -', c_loc)
        c_loc = re.sub(r'[+]', ' +', c_loc).split()
        pos_str = ",".join(c_loc)

        request_url = "{0}location={1}&radius={2}&sensor=false&key={3}".format(
			placesUrl, pos_str, radius, placesKey)
        self.logAndShow("making places api request to Google at url: {0}."
                        .format(request_url))
        places_response = getPage(request_url)
        places_response.addCallback(
            lambda res:
            self.processPlaces(res, int(num_entries), c_id))
        return

    def processPlaces(self, response, limit, c_id):
        json_object = json.loads(response)
        json_object["results"] = json_object["results"][0:int(limit)]
        self.logAndShow("Api returns: {0}".format(json.dumps(json_object, indent=4)))
        msg = self.factory.clients[c_id]["msg"]
        full_response = "{0}\n{1}\n\n".format(msg, json.dumps(json_object, indent=4))
        self.transport.write(full_response)

class ProxyHerdServerFactory(Factory):
    def __init__(self, name):
        self.name = name
        self.port_number = portsMap[self.name]
        self.clients = {}
        self.num_clients = 0
        logging.basicConfig(filename=name+'.log', level=logging.DEBUG)
        self.debug("Server ({0}) finished initializing on port {1}.".format(name, self.port_number))

    # Set the protocol
    def buildProtocol(self, addr):
		return ProxyHerdProtocol(self)

    def stopFactory(self):
        logging.debug("Server ({0}) is stopped. Port {1} is free.".format(self.name, self.port_number))
        return self


# We need a dumb client that only sends a IAMAT to server
# for the sake of flooding location updates
class DumbClientProtocol(LineReceiver):
    def __init__(self, factory):
        self.factory = factory
        return

    def connectionMade(self):
        self.sendLine(self.factory.msg)
        # we don't want to keep the connection between servers
        self.transport.loseConnection()

class DumbClientFactory(protocol.ClientFactory):
    def __init__(self, msg):
        self.msg = msg

    def buildProtocol(self, addr):
        return DumbClientProtocol(self)



def main():
    if len(sys.argv) != 2:
        print "Please provide the server name."
        return -1
    name = sys.argv[1]
    if(portsMap.has_key(name)):
        reactor.listenTCP(portsMap[name], ProxyHerdServerFactory(name))
        reactor.run()
    else:
        print "Please enter a valid server name from {0}".format(portsMap.keys())
        return -1

if __name__ == '__main__':
    main()