def observe_vehicle(connection, vehicle_id):
    vehicle = connection.vehicle
    x, y = vehicle.getPosition(vehicle_id)
    return dict(position_x=x, position_y=y, speed=vehicle.getSpeed(vehicle_id), lane_id=vehicle.getLaneID(vehicle_id), road_segment=vehicle.getRoadID(vehicle_id))
