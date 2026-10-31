import time
import uuid
import traci
from simulation.event_generator.schemas import TrafficEvent
from .vehicle_observer import observe_vehicle
from .signal_observer import observe_signals


class TraCIClient:
    def __init__(self, config, sumocfg):
        self.config = config
        self.sumocfg = sumocfg

    def events(self):
        config = self.config
        label = str(uuid.uuid4())
        traci.start(['sumo', '-c', str(self.sumocfg), '--seed', str(config.seed), '--no-step-log', 'true', '--duration-log.disable', 'true'], label=label, doSwitch=False)
        connection = traci.getConnection(label)
        sequence = {}
        owners = {}
        tick = 0
        count = 0

        def event(entity, entity_type, event_type, now, owner, variables):
            nonlocal count
            key = (entity_type, entity)
            sequence[key] = sequence.get(key, 0) + 1
            count += 1
            return TrafficEvent(event_id=f'{config.run_id}-evt-{count:08d}', run_id=config.run_id, entity_id=entity, entity_type=entity_type, event_type=event_type, simulation_time_ms=now, source_timestamp_ms=now, sequence_number=sequence[key], source_id=owner, process_variables=variables, emitted_at_unix_ms=time.time_ns()//1000000)

        try:
            while connection.simulation.getTime() < config.duration_s - 1e-8:
                connection.simulationStep()
                tick += 1
                now = round(connection.simulation.getTime()*1000)
                inserted = set(connection.simulation.getDepartedIDList())
                for vehicle in sorted(connection.vehicle.getIDList()):
                    if vehicle in inserted or tick % round(config.observation_interval_s/config.step_s) == 0:
                        variables = observe_vehicle(connection, vehicle)
                        owner = config.owner(variables['position_x'])
                        owners[vehicle] = owner
                        yield event(vehicle, 'vehicle', 'insertion' if vehicle in inserted else 'observation', now, owner, variables)
                for vehicle in sorted(connection.simulation.getArrivedIDList()):
                    yield event(vehicle, 'vehicle', 'departure', now, owners.pop(vehicle, config.west_owner), {})
                if tick % round(config.observation_interval_s/config.step_s) == 0:
                    for signal, variables in observe_signals(connection):
                        x, _ = connection.junction.getPosition(signal)
                        yield event(signal, 'traffic_signal', 'observation', now, config.owner(x), variables)
                    for road in sorted(e for e in connection.edge.getIDList() if not e.startswith(':')):
                        shape = connection.lane.getShape(road+'_0')
                        x = sum(p[0] for p in shape)/len(shape)
                        variables = dict(vehicle_count=connection.edge.getLastStepVehicleNumber(road), mean_speed=connection.edge.getLastStepMeanSpeed(road), occupancy=connection.edge.getLastStepOccupancy(road))
                        yield event(road, 'road_segment', 'observation', now, config.owner(x), variables)
        finally:
            connection.close()
