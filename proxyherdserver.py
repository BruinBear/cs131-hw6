from twisted.protocols.basic import LineReceiver
from twisted.web.client import getPage
from twisted.internet.protocol import Factory, Protocol
from twisted.application import service, internet
from twisted.internet import reactor

import time, sys, logging

placesKey = "AIzaSyATYDSsfguWYSVji3uQWYMNoJy-sQIolHA"
placesUrl = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?"

portsMap = {'Alford': 12001,
         'Bolden': 12002,
         'Hamilton': 12003,
         'Parker': 12004,
         'Powell': 12005
         }
# portsMap = {'Alford': 12001}

CanTalkTo = {
    'Alford': ['Bolden', 'Hamilton'],
    'Bolden': ['Parker', 'Powell'],
    'Hamilton': ['Parker']
    }


class ProxyHerdProtocol(LineReceiver):
    def __init__(self, factory):
        self.factory = factory

    def logAndShow(self, debug):
        logging.debug(debug)
        print debug

    def connectionMade(self):
        self.factory.num_clinets += 1
        debug = "New client connected.\n" \
                      "Current Number of clients: "+self.factory.num_clinets
        logAndShow(debug)

    def connectionLost(self, reason):
        self.factory.num_clinets += 1
        debug = "A client disconnected.\n"\
                      "Current Number of clients: "+self.factory.num_clinets
        logAndShow(debug)


    def lineReceived(self, line):
        debug = "Received" + line
        logAndShow(debug)

        params = line.split(" ")
        # use the first parameter to determine handler
        if(params[0] == "IAMAT"):
            self.do_IAMAT(params)
        elif(params[0] == "AT"):
            self.do_AT(params)
        elif(params[0] == "WHATSAT"):
            self.do_WHATSAT(params)
        else:
            logAndShow('Bad Request.')

    def do_IAMAT(self, params):
        if len(params) != 4:
            logAndShow('Invalid IAMAT Request.')
            self.transport.write("Bad Request\n")
            return
        _, c_id, c_loc, c_time = params

        # Time comparison between sent timestamp and received local timestamp
        local_time = time.time()
        time_diff = local_time - float(c_time)

        # Format response
        if time_diff > 0:
            response = "AT {0} +{1} {2} {3} {4}".format("Danny", time_diff,
                                                        c_id, c_loc, c_time)
        else:
            response = "AT {0} -{1} {2} {3} {4}".format("Danny", time_diff,
                                                        c_id, c_loc, c_time)

        if c_id not in self.factory.clients:
            logAndShow("New Client")

        logAndShow("Location update from client [{0}]".format(c_id))
        self.transport.write(response+"\n")
        self.updateLocation(response)
        return

    def updateLocation(self, response):
        
        return

    def do_AT(self, params):
        return

    def do_WHATSAT(self, params):
        if len(params) != 4:
            print('Bad Requests.')
            self.transport.write("Bad Requess\n")
            return
        _, c_id, radius, num_entries = params
        if(radius > 50):
            radius = 50
        if(num_entries > 20):
            num_entries = 20

        pos_str = "+34.068930,-118.445127"
        request_url = "{0}location={1}&radius={2}&sensor=false&key={3}".format(
			placesUrl, pos_str, radius, placesKey)
        places_response = getPage(request_url)
        places_response.addCallback(lambda res: self.processPlaces(res))
        return

    def message(self, message):
        self.transport.write(message + '\n')
        return

    def processPlaces(self, response):
        print response
        return




class ProxyHerdServerFactory(Factory):

    def __init__(self, server_name):
        self.server_name = server_name
        self.port_number = portsMap[self.server_name]
        self.clients = {}
        self.num_clinets = 0
        logging.basicConfig(filename=server_name+'.log', level=logging.DEBUG)
        logging.debug("Server ({0}) finished initializing on port {1}.".format(server_name, self.port_number))

    # Set the protocol
    def buildProtocol(self, addr):
		return ProxyHerdProtocol(self)

    def stopFactory(self):
        logging.debug("Server ({0}) is stopped. Port {1} is free.".format(self.server_name, self.port_number))
        return self

def main():
    if len(sys.argv) != 2:
        print "Please provide the server name."
        return -1
    name = sys.argv[1]
    if(portsMap.has_key(name)):
        reactor.listenTCP(portsMap[name], ProxyHerdServerFactory(name))
        reactor.run()
    else:
        print "Please enter a valid server name from"+portsMap.keys()
        return -1

if __name__ == '__main__':
    main()