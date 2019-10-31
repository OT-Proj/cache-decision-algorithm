import threading
from datetime import datetime, timedelta
import time
import gc
import sys
import math

class MachineCountLogic:
    def __init__(self, h, m, s):
        # initiates the request counting logic
        # usage: MachineCountLogic(datetime.timedelta(hours=1))

        self.time_delta = timedelta(hours=h, minutes=m, seconds=s)  # time backwards is a dateTime object
        self.count_ref_hash = {}  # will hold references to requests list

        # init garbage cleaner thread
        self.stop_cleaning = False
        self.cleaner_thread = threading.Thread(target=self.cleanIrrelevantMachines, args=())
        self.cleaner_thread.start()
        self.is_cleaner_thread_active = True


    def cleanIrrelevantMachines(self):
        while not self.stop_cleaning:
            # the keys are prone to changes as new machines are handled
            # each round we iterate over the known keys, leaving new keys to the next round
            known_keys = list(self.count_ref_hash.keys())

            # for performance optimization, if we currently don't store any requests,
            # its safe to rest for 1 second before checking again
            # we won't have anything to delete anyway
            if len(known_keys) == 0:
                time.sleep(1)

            # however, if we do have stored requests, we delete them one at a time,
            # one machine at a time.
            # example: del <machine1 sample0>, del <machine2 sample0>, del <machine3 sample0>, ...
            oldest_request = datetime.now()  # we are looking for the oldest request recorded
            found_relevant_request = False  # will indicate if we found a sample to be deleted later
            for machine in known_keys:
                if not self.count_ref_hash[machine][1]:
                    # if the list was emptied, we have no reason to keep it's reference
                    del self.count_ref_hash[machine]
                else:
                    dt_timestamp = datetime.fromtimestamp(self.count_ref_hash[machine][1][0])
                    if datetime.now() - dt_timestamp > self.time_delta:
                        del self.count_ref_hash[machine][1][0]

                    # we are looking the the oldest recorded request in our updated hash.
                    # this is for performance optimization. this way,
                    # we can suspend the loop until the oldest request is to be deleted
                    if len(self.count_ref_hash[machine][1]) > 0:
                        dt_timestamp = datetime.fromtimestamp(self.count_ref_hash[machine][1][0])
                        if dt_timestamp < oldest_request:
                            oldest_request = dt_timestamp

                # we give the loop a chance to end prematurely
                if self.stop_cleaning:
                    break

            # we can sleep until the oldest sample is to be deleted
            # calculate how many seconds are between our time delta and oldest sample:
            time_to_sleep = (self.time_delta - (datetime.now() - oldest_request)).total_seconds()
            current_time_delta = self.time_delta

            # we will sleep 10% of the known amount of seconds
            seconds_slept = 0
            while seconds_slept < time_to_sleep:
                time.sleep(0.1)  # for every second we can sleep, we actually sleep 0.1 seconds
                seconds_slept += 1
                if current_time_delta is not self.time_delta:
                    # if the time delta changed during runtime,
                    # the current time_to_sleep calculation becomes deprecated
                    break

        self.is_cleaner_thread_active = False


    def handleRequest(self, machine_id):
        # handles request for a machine
        # if the machine has no recorded request timestamps, we create new list for it
        # else, we append current timestamp to it's list

        if machine_id not in self.count_ref_hash:  # handle new or previously irrelevant machine
            # create new hash entry with a designated lock and timestamps list
            timestamp = datetime.timestamp(datetime.now())
            if len(self.count_ref_hash) < sys.maxsize:  # do not hash more than a python dict can handle
                self.count_ref_hash[machine_id] = (threading.Lock(), [timestamp])

        else:  # handle existing machine
            # append the current timestamp to the existing list

            try:
                self.count_ref_hash[machine_id][0].acquire()
                timestamp = datetime.timestamp(datetime.now())
                # do not store more requests than a list can hold:
                if len(self.count_ref_hash[machine_id][1]) < sys.maxsize:
                    self.count_ref_hash[machine_id][1].append(timestamp)
                self.count_ref_hash[machine_id][0].release()

            except KeyError:  # machine got deleted while adding a request
                # in this case we re initialize the machine requests list
                timestamp = datetime.timestamp(datetime.now())
                if len(self.count_ref_hash) < sys.maxsize:  # do not hash more than a python dict can handle
                    self.count_ref_hash[machine_id] = (threading.Lock(), [timestamp])

    def setTimeDelta(self, h, m, s):
        self.time_delta = timedelta(hours=h, minutes=m, seconds=s)
        self.x = 2

    def getMostRequestedMachines(self, N):
        # this function is not thread-safe for performance reasons.
        # this means it will return and approximation of how many times was a machine requested.
        # the results are yielded to allow processing machine before getting the next.
        # if N > number of existing machines, only the current number of machines will be yielded.
        # if no machines were requested, None will be returned
        # usage: getMostRequestedMachines(100)

        yielded_machines = []  # keep track of which machines were already yielded

        for i in range(0, N):
            current_maximum = 0
            current_most_requested_machine = None

            # find the most requested machine which was not already yielded
            for machine in self.count_ref_hash:

                try:
                    number_of_requests = len(self.count_ref_hash[machine][1])
                except KeyError: # what if the entry was deleted during search?
                    number_of_requests = 0

                if number_of_requests > current_maximum:
                    if machine not in yielded_machines:
                        current_maximum = number_of_requests
                        current_most_requested_machine = machine

            # yield the result.
            if current_most_requested_machine:
                yielded_machines.append(current_most_requested_machine)
                yield (current_most_requested_machine, current_maximum)
            else:  # no more machines could be found
                break  # if no more machines could be found we break out

    def getTimeDelta(self):
        return self.time_delta

    def __del__(self):
        self.stop_cleaning = True
        while self.is_cleaner_thread_active:
            pass
        del self.count_ref_hash
        gc.collect()
        # usually calling gc.collect() is unrecommended,
        # but since we are getting rid of lots of timestamps,
        # this may prove useful.
