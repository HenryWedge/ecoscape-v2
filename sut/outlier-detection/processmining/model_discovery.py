from pm4py.algo.discovery.heuristics import algorithm as heuristics_miner
from pm4py.objects.log.obj import Event, EventLog, Trace
from datetime import datetime

class ModelDiscovery:

    def discover_heuristics_miner(self, events):
        traces_by_case = {}
        for event in events:
            traces_by_case.setdefault(event["case"], []).append(event)

        log = EventLog()
        for case, trace_events in traces_by_case.items():
            trace = Trace(attributes={"concept:name": case})
            for event in trace_events:
                trace.append(
                    Event(
                        {
                            "concept:name": event["activity"],
                            "time:timestamp": datetime.fromisoformat(event["timestamp"]),
                        }
                    )
                )
            log.append(trace)

        return heuristics_miner.apply(log)
