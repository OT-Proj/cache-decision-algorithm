from flask import Flask, request, jsonify, session
from MachineCountLogic import *
from Consts import *
from datetime import timedelta

class FlaskFramework:
    # in order to demonstrate the login, we wrap our code inside a web framework,
    # to implement a Rest API.
    # there are usage instructions at the "/" URL.

    def __init__(self):
        # default time delta is 30 seconds
        self.mcl = MachineCountLogic(0, 0, 30)
        self.app = Flask("Machine Count Logic Framework")
        self.initAppRoutes(self.app)
        self.app.run(threaded=True, debug=True)  # debug mode displays JSON results prettier.


    def initAppRoutes(self, app):
        # initiates the routing URLs for the framework

        # main page/help page
        @self.app.route("/", methods=['GET', 'POST'])
        def helpPage():
            return HELP_MESSAGE

        # simulates a machine request by ID
        @self.app.route("/appendRequest", methods=['GET'])
        def appendRequest():
            try:
                machine_id = request.args.get('machineID')
                if machine_id:
                    self.mcl.handleRequest(machine_id)
                    result = {"status_code": 200}
                    return jsonify(result)  # return result as json string
                result = {"status_code": 400, "error": "No machineID provided"}
                return jsonify(result)
            except:  # broad exception handling
                return "something went wrong"

        # get the caching decision
        @self.app.route("/getMostRequested", methods=['GET'])
        def getMostRequested():
            try:
                n = request.args.get('N')
                if n:
                    result = {}
                    result["status_code"] = 200
                    result["time delta"] = str(self.mcl.getTimeDelta())
                    result["machines"] = []
                    total_requests = 0
                    for machine_tuple in self.mcl.getMostRequestedMachines(int(n)):
                        machine_dictionary = {"name": machine_tuple[0], "requests": machine_tuple[1]}
                        result["machines"].append(machine_dictionary)
                        total_requests += machine_tuple[1]
                    result["total requests"] = total_requests
                    return jsonify(result)  # return result as json string
                result = {"status_code": 400, "error": "number of machines not stated"}
                return jsonify(result)
            except:  # broad exception handling
                return "something went wrong"

        # modify the current time delta
        @self.app.route("/setTimeDelta", methods=['GET'])
        def setTimeDelta():
            try:
                # in python 3.8, it is possible to assign variables in 'if' statement.
                # this project however was written in python 3.7
                h = request.args.get('hours')
                if not h:
                    h = 0
                h = int(h)
                m = request.args.get('minutes')
                if not m:
                    m = 0
                m = int(m)
                s = request.args.get('seconds')
                if not s:
                    s = 0
                s = int(s)
                if h == 0 and m == 0 and s == 0:
                    result = {}
                    result["status_code"] = 400
                    result["error"] = "time delta has to be at least 1 second"
                    return jsonify(result)
                self.mcl.setTimeDelta(h, m, s)
                result = {}
                result["status_code"] = 200
                return jsonify(result)
            except:  # broad exception handling
                return "something went wrong"


if __name__ == '__main__':
    f = FlaskFramework()


