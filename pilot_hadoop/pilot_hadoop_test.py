__author__ = 'luckow'

import os
from pilot_hadoop import PilotComputeService

os.environ["SAGA_VERBOSE"]="100"
pilot_compute_description = {
    "service_url":"fork://localhost",
    "number_cores": 1,
    "cores_per_node":1,
    "type":"spark"
}
pilot = PilotComputeService.create_pilot(pilot_compute_description)

print str(pilot.get_details())
#spark_master = pilot.get_details()