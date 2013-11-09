import heapq
import collections
import time
import random
import math

import greenlet
import faststat


class Simulation(object):
    def __init__(self):
        self.time = Timeline()
        self.event = EventDispatcher(self.time)
        self.net = Network(self.time)
        self.stats = collections.defaultdict(faststat.Stats)

    def register(self, name, target):
        self.net.directory[name] = target

    def run(self, duration):
        self.time.run(duration)


NEVER = object()  # sentinel value for scheduling

class Timeline(object):
    'the simulated timeline / sequence of events'
    def __init__(self):
        # simulation state
        self.heap = []  # actions that should execute in the future
        self.now = 0  # current simulation time
        self.loop = greenlet.greenlet(self._loop)
        self.return_to = None
        # meta-data
        self.time_steps = 0
        self.realtime = 0

    def wait(self, duration):
        'simulate a pause (e.g. work being done)'
        heapq.heappush( self.heap, (self.now + duration, greenlet.getcurrent()) )
        self.next()

    def schedule(self, deferred, delay=0):
        'schedule a deferred action (represented as a greenlet) to execute later'
        if type(deferred) is not greenlet.greenlet:
        	deferred = greenlet.greenlet(deferred, self.loop)
        if delay is not NEVER:
            heapq.heappush( self.heap, (self.now + delay, deferred))
        return deferred

    def next(self):
        'advance the timeline to the next event'
        self.time_steps += 1
        self.now, next = heapq.heappop(self.heap)
        next.switch()

    def _loop(self):
    	while 1:
    		while self.heap:
    			self.next()
    		self.return_to.switch()

    def run(self, duration):
    	self.return_to = greenlet.getcurrent()
    	self.schedule(greenlet.getcurrent(), duration)
    	#schedule self, which will bounce back to this function and return
    	start = time.time()
    	self.loop.switch()
    	#alternatively, if there are no events the loop will switch back
    	self.realtime += time.time() - start

    def __repr__(self):
        return "<Timeline simtime={0} realtime={1} steps={2} pending={3}>".format(
            self.now, self.realtime, self.time_steps, len(self.heap))


TIMED_OUT = object()

class EventDispatcher(object):
    def __init__(self, timeline):
        self.listeners = {}
        self.timeline = timeline

    def wait(self, event, timeout=NEVER):
        do_once = self.timeline.schedule(greenlet.getcurrent().switch, timeout)
        listeners = self.listeners.setdefault(event, {})
        listeners[do_once] = TIMED_OUT
        self.timeline.next()  # will resume execution when either event has triggered, or timeout occurred
        result = self.listeners[event][do_once]
        del self.listeners[event][do_once]
        if not self.listeners[event]:
            del self.listeners[event]
        if result is TIMED_OUT:
            raise TimeoutError(timeout)  # more info here
        return result

    def trigger(self, event, data=None):
        listeners = self.listeners.get(event, ())
        for glet in listeners:
            listeners[glet] = data
            self.timeline.schedule(glet)


class Network(object):
    'the simulated network fabric'
    def __init__(self, timeline, directory=None, connect_delay=None):
        self.timeline = timeline
        self.directory = directory or {}
        self.connect_delay = connect_delay
        self.num_connects = 0

    def connect(self, name):
        self.num_connects += 1
        if self.connect_delay:
            self.timeline.wait(self.connect_delay)
        return self.directory[name]


class Request(object):
    '''
    Represents a network request.
    To wait on the request, simply call sim.event.wait(request).
    '''
    __slots__ = ('response', 'start', 'finish', 'sim', 'target')

    def __init__(self, sim, target):
        self.sim = sim
        self.target = target
        self.start = sim.time.now
        sim.net.directory[target](self)

    def finish(self, response):
        self.response = response
        self.finish = self.sim.now
        self.sim.event.trigger(self)


class ConnectionRefused(Exception):
    pass

class TimeoutError(Exception):
    pass


class Router(object):
    def __init__(self, name, servers):
        self.name = name
        self.servers = servers
        self.num_requests = 0

    def __call__(self, req):
        self.num_requests += 1
        return self.servers[self.num_requests % len(self.servers)](req)

    def __repr__(self):
        return "<Router name={0} num_requests={1} servers={2}>".format(
            self.name, self.num_requests, self.servers)


_QUEUE_HAS_ITEM = object()


class Server(object):
    'a simulated physical server instance'
    def __init__(self, simulation, num_workers, queue_size):
        self.queue_size = queue_size
        self.num_workers = num_workers
        self.sim = simulation
        self.queue = collections.deque()
        self.workers = [self.sim.time.schedule(self._worker_run) for i in range(num_workers)]
        self.stats = collections.defaultdict(faststat.Stats)
        self.requests_accepted = 0
        self.requests_rejected = 0

    def enqueue(self, req):
        self.stats['queue_depth'].add(len(self.queue))
        if len(self.queue) > self.queue_size:
            self.requests_rejected += 1
            raise ConnectionRefused()
        self.requests_accepted += 1
        self.queue.append(req)
        self.sim.event.trigger((self, _QUEUE_HAS_ITEM))

    def _worker_run(self):
        while 1:
            while self.queue:
                req = self.queue.popleft()
                self.stats['queue_time'].add(self.sim.time.now - req.start)
                work_start = self.sim.time.now
                self.do_work(req)
                self.stats['worked_time'].add(self.sim.time.now - work_start)
                self.stats['total_time'].add(self.sim.time.now - req.start)
                self.sim.event.trigger(req)
            self.sim.event.wait((self, _QUEUE_HAS_ITEM))

    def do_work(self, req):
        self.sim.time.wait(0.1)
        req.response = "fooo"

    def __call__(self, req):
        self.enqueue(req)

    def __repr__(self):
        return "<Server avg_queue_time={0} avg_total_time={1} num_requests={2},{3}>".format(
            _sigfigs(self.stats['queue_time'].mean), _sigfigs(self.stats['total_time'].mean), 
            self.requests_accepted, self.requests_rejected)


class PoissonProcess(object):
    'represents events occurring independently'
    def __init__(self, simulation, event, requests_per_second):
        self.rps = requests_per_second
        self.sim = simulation
        self.event = event
        simulation.time.schedule(self._run)

    def _run(self):
        while 1:
            delay = random.expovariate(self.rps)
            self.sim.time.wait(delay)
            self.sim.time.schedule(self.event)


class TrafficSource(PoissonProcess):
    def __init__(self, simulation, target, requests_per_second):
        self.sim = simulation
        self.target = target
        self.success = 0
        self.failure = 0
        super(TrafficSource, self).__init__(
            simulation, self.event, requests_per_second)

    def event(self):
        try:
            req = Request(self.sim, self.target)
            self.sim.event.wait(req)
            self.success += 1
        except:
            self.failure += 1


def _sigfigs(n, sigfigs=3):
    'helper function to round a number to significant figures'
    if n == 0 or math.isnan(n):  # avoid math domain errors
        return n
    return round(float(n), -int(math.floor(math.log10(abs(n))) - sigfigs + 1))


SWITCHES = 0

def _trace(event, src_dst):
    global SWITCHES
    SWITCHES += 1
    src, dst = src_dst
    print event, _glet_repr(src), "==>", _glet_repr(dst)


def _glet_repr(glet):
    stack = []
    cur = glet.gr_frame
    while cur:
        stack.append(cur.f_code)
        cur = cur.f_back
    stack = [e.co_name for e in stack[::-1]]
    if stack:
        stack = "()->".join(stack)
    else:
        stack = "(none)"
    return "<greenlet id={0} stack={1}>".format(id(glet) % 1024, stack)


def test():
    sim = Simulation()
    timeline = sim.time
    router = Router("vip1", [Server(sim, 3, 10) for i in range(3)])
    sim.register("vip1", router)
    traffic = TrafficSource(sim, "vip1", 75)

    #greenlet.settrace(_trace)

    print timeline.heap
    timeline.run(1000)
    print timeline
    print router
    print "successes", traffic.success, "failures", traffic.failure
    print "SWITCHES", SWITCHES


if __name__ == "__main__":
    test()
