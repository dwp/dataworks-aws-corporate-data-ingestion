import os
import logging

logger = logging.getLogger(__name__)

step_to_start_from_file = "/opt/emr/step_to_start_from.txt"


def should_skip_step(current_step_name):
    if os.path.isfile(step_to_start_from_file):
        logger.info(f"Previous step file found at {step_to_start_from_file}")

        with open(step_to_start_from_file, "r") as file_to_open:
            step = file_to_open.read().strip()

        logger.info(f"Current step name is {current_step_name}")

        if step != current_step_name:
            logger.info(f"Current step name is {current_step_name}, which doesn't match previously failed step {step}, "
                        f"so will exit")
            return True
        else:
            logger.info(f"Current step name is {current_step_name}, which matches previously failed step {step}, "
                        f"so deleting local file")
            os.remove(step_to_start_from_file)
    else:
        logger.info(f"No previous step file found at {step_to_start_from_file}")

    return False
