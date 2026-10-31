def observe_signals(connection):
    return [(signal, {'signal_state': connection.trafficlight.getRedYellowGreenState(signal), 'phase': connection.trafficlight.getPhase(signal)}) for signal in sorted(connection.trafficlight.getIDList())]
