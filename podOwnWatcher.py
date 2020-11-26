import psycopg2
import os
import logging
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException
from urllib3.exceptions import ReadTimeoutError
import time


def create_tables():
    psqlURL = os.getenv("PSQL_URL")
    if not psqlURL:
        logging.warning("PSQL_URL env is not defined")
        return
    commands = (
        """
        CREATE TABLE IF NOT EXISTS pod_owner (
            pod_name varchar(64) PRIMARY KEY,
            namespace varchar(64),
            pod_owner VARCHAR(140) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_pod_owner ON pod_owner USING hash (pod_name)
        """,
    )
    conn = None
    try:
        conn = psycopg2.connect(psqlURL)
        cur = conn.cursor()
        for command in commands:
            cur.execute(command)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.warning(error)
    finally:
        if conn is not None:
            conn.close()


def put_pod(pod, owner, namespace):
    psqlURL = os.getenv("PSQL_URL")
    conn = None
    try:
        conn = psycopg2.connect(psqlURL)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO pod_owner (pod_name,pod_owner,namespace) values(%s,%s, %s);", (pod, owner, namespace))
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.warning(error)
    finally:
        if conn is not None:
            conn.close()


def getReplicaSet(name, namespace):
    global apis_api
    replica_set = apis_api.read_namespaced_replica_set(name=name, namespace=namespace)
    owner_references2 = replica_set.metadata.owner_references
    if isinstance(owner_references2, list):
        return "dp:" + owner_references2[0].name
    else:
        return "dp:" + owner


def getGrantOwner(name, namespace, otype):
    global apis_api
    global batch_api
    if otype == "ReplicaSet":
        res = apis_api.read_namespaced_replica_set(name=name, namespace=namespace)
    elif otype == "Job":
        res = batch_api.read_namespaced_job(name=name, namespace=namespace)
    owner_references = res.metadata.owner_references
    if isinstance(owner_references, list):
        return owner_references[0].kind + ":" + owner_references[0].name
    else:
        return ""


if __name__ == "__main__":  # noqa
    create_tables()
    try:
        config.load_incluster_config()
    except config.ConfigException:
        try:
            config.load_kube_config()
        except config.ConfigException:
            raise Exception("Could not configure kubernetes python client")

    core_api = client.CoreV1Api()
    apis_api = client.AppsV1Api()
    batch_api = client.BatchV1Api()
    w = watch.Watch()
    while True:
        try:
            for event in w.stream(core_api.list_pod_for_all_namespaces, _request_timeout=60):
                pod_name = event['object'].metadata.name
                namespace = event['object'].metadata.namespace
                if event['type'] == "ADDED":
                    owner = ""
                    if isinstance(event['object'].metadata.owner_references, list):
                        try:
                            if event['object'].metadata.owner_references[0].kind in ["ReplicaSet", "Job"]:
                                owner = getGrantOwner(
                                    event['object'].metadata.owner_references[0].name,
                                    event['object'].metadata.namespace,
                                    event['object'].metadata.owner_references[0].kind)
                            elif event['object'].metadata.owner_references[0].kind:
                                owner = event['object'].metadata.owner_references[0].kind + ":" + event['object'].metadata.owner_references[0].name
                        except ApiException as e:
                            logging.warning("Get pod owner failed: {}".format(e))
                    if owner == "" and event['object'].metadata.name.startswith("kube-proxy"):
                        owner = "System:kube-proxy"
                    put_pod(event['object'].metadata.name, owner, namespace)
                    if owner == "" and namespace != "kube-system":
                        logging.warning("No owner: %s" % (event))
                logging.info("Event: %s %s" % (event['type'], event['object'].metadata.name))
        except (ReadTimeoutError, IOError):
            time.sleep(5)
