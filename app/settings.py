from os import environ

from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = environ["DATABASE_URL"]

IPC_SMR_G4_PUBSUB = "ipc:///tmp/smr_g4_pubsub"
SMR_G4_REAL_PATH = environ["SMR_G4_REAL_PATH"]
SMR_G4_DOCKER_PATH = environ["SMR_G4_DOCKER_PATH"]
DOCKER_GEANT4_CONTAINER_ID = environ["DOCKER_GEANT4_CONTAINER_ID"]

OUTPUT_PATH = environ.get("OUTPUT_PATH", "output/")
