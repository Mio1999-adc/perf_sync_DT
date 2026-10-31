"""Generate a two-lane, four-arm signalized intersection without map downloads."""
import json
import random
import subprocess
from pathlib import Path
from xml.etree.ElementTree import Element, SubElement, ElementTree
from pydantic import BaseModel, Field, model_validator


class Scenario(BaseModel):
    seed: int = 27
    duration_s: float = Field(default=90, gt=0)
    step_s: float = Field(default=0.2, gt=0)
    observation_interval_s: float = Field(default=1, gt=0)
    arrival_rate_per_s: float = Field(default=0.3, gt=0)
    run_id: str = 'intersection-seed27'
    rsu_boundary_x: float = 300
    west_owner: str = 'rsu-west'
    east_owner: str = 'rsu-east'

    @model_validator(mode='after')
    def intervals(self):
        ratio = self.observation_interval_s / self.step_s
        if ratio < 1 or abs(ratio - round(ratio)) > 1e-8:
            raise ValueError('observation interval must be an integer multiple of step')
        if self.west_owner == self.east_owner:
            raise ValueError('scenario requires two distinct RSU owners')
        return self

    def owner(self, x):
        return self.west_owner if x < self.rsu_boundary_x else self.east_owner


def write_xml(path, root):
    ElementTree(root).write(path, encoding='utf-8', xml_declaration=True)


def generate(directory, config):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    nodes = Element('nodes')
    for name, x, y in [('W', -300, 0), ('E', 300, 0), ('N', 0, 300), ('S', 0, -300), ('C', 0, 0)]:
        SubElement(nodes, 'node', id=name, x=str(x), y=str(y), type='traffic_light' if name == 'C' else 'priority')
    write_xml(directory / 'nodes.nod.xml', nodes)
    edges = Element('edges')
    for name in 'WENS':
        SubElement(edges, 'edge', id=name+'C', **{'from': name, 'to': 'C', 'numLanes': '2', 'speed': '13.9'})
        SubElement(edges, 'edge', id='C'+name, **{'from': 'C', 'to': name, 'numLanes': '2', 'speed': '13.9'})
    write_xml(directory / 'edges.edg.xml', edges)
    subprocess.run(['netconvert', '--node-files', str(directory/'nodes.nod.xml'), '--edge-files', str(directory/'edges.edg.xml'), '--output-file', str(directory/'intersection.net.xml'), '--no-turnarounds', 'true'], check=True, capture_output=True)
    routes = Element('routes')
    SubElement(routes, 'vType', id='car', accel='2.6', decel='4.5', sigma='0.3', length='5', maxSpeed='13.9')
    for name, edges_value in [('we','WC CE'), ('ew','EC CW'), ('ns','NC CS'), ('sn','SC CN')]:
        SubElement(routes, 'route', id=name, edges=edges_value)
    rng = random.Random(config.seed)
    time_s = 0.0
    index = 0
    while time_s < config.duration_s * 0.65:
        SubElement(routes, 'vehicle', id=f'vehicle-{index:04d}', type='car', route=['we','ew','ns','sn'][index % 4], depart=f'{time_s:.3f}', departLane='best')
        time_s += rng.expovariate(config.arrival_rate_per_s)
        index += 1
    write_xml(directory/'traffic.rou.xml', routes)
    root = Element('configuration')
    inputs = SubElement(root, 'input')
    SubElement(inputs, 'net-file', value='intersection.net.xml')
    SubElement(inputs, 'route-files', value='traffic.rou.xml')
    timing = SubElement(root, 'time')
    SubElement(timing, 'step-length', value=str(config.step_s))
    SubElement(timing, 'end', value=str(config.duration_s))
    random_settings = SubElement(root, 'random_number')
    SubElement(random_settings, 'seed', value=str(config.seed))
    write_xml(directory/'intersection.sumocfg', root)
    (directory/'scenario.json').write_text(config.model_dump_json(indent=2))
    return directory/'intersection.sumocfg'
