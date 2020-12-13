import psycopg2
import os
import logging
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException
from urllib3.exceptions import ReadTimeoutError
import time
import tornado.ioloop
import tornado.web
import sys
import threading
import requests


def create_tables():
    psqlURL = os.getenv("PSQL_URL")
    if not psqlURL:
        logging.warning("PSQL_URL env is not defined")
        sys.exit()
    commands = (
        """
        CREATE TABLE IF NOT EXISTS pod_owner (
            pod_name varchar(64) PRIMARY KEY,
            namespace varchar(64),
            release varchar(64),
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


def put_pod(pod, owner, namespace, release):
    psqlURL = os.getenv("PSQL_URL")
    conn = None
    try:
        conn = psycopg2.connect(psqlURL)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO pod_owner (pod_name,pod_owner,namespace,release) values(%s, %s, %s, %s);", (pod, owner, namespace, release))
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
        return "dp:" + owner_references2.name


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


def watch_grab_owner():
    global podOwners
    global apis_api
    global batch_api
    global core_api
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
                    if event['object'].metadata.name not in podOwners:
                        release=""
                        if "release" in event['object'].metadata.labels:
                            release= event['object'].metadata.labels['release']
                        put_pod(event['object'].metadata.name, owner, namespace,release)
                        podOwners[event['object'].metadata.name] = owner
                    # if owner == "" and namespace != "kube-system":
                    #    logging.warning("No owner: %s" % (event))
                logging.info("Event: %s %s" % (event['type'], event['object'].metadata.name))
        except (ReadTimeoutError, IOError):
            time.sleep(5)


def getPodFromDB():
    global podOwners
    psqlURL = os.getenv("PSQL_URL")
    conn = None
    try:
        conn = psycopg2.connect(psqlURL)
        cur = conn.cursor()
        cur.execute("select pod_name,pod_owner from pod_owner;")
        allrows = cur.fetchall()
        for row in allrows:
            podOwners[row[0]] = row[1]
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.warning(error)
    finally:
        if conn is not None:
            conn.close()
            logging.warning(f"Loadet from DB {len(podOwners)} oblects\n")


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        global podOwners
        promURL = os.getenv("PROMETHEUS_URL")
        nl = '\n'
        responce = """
# HELP cost_cpu_seconds_total Total number of second used by pods.
# TYPE cost_cpu_seconds_total counter
        """
        try:
            r = requests.get(
                promURL +
                '/api/v1/query?query=sum%20by%20(pod,namespace)%20(container_cpu_usage_seconds_total{cpu="total"})',
                timeout=5)
            jout = r.json()
            if "status" in jout:
                if jout["status"] == "success":
                    for line in jout["data"]["result"]:
                        if "pod" not in line["metric"]:
                            continue
                        p = line["metric"]["pod"]
                        n = line["metric"]["namespace"]
                        if p in podOwners:
                            responce += rf"""cost_cpu_seconds_total{{pod="{p}",namespace="{n}" , pod_owner="{podOwners[p]}"}} {line["value"][1]} {nl}"""
            responce += """
# HELP cost_memory_usage_bytes Total number of second used by pods.
# TYPE cost_memory_usage_bytes gauge
                   """ + nl
            r = requests.get(
                promURL +
                '/api/v1/query?query=sum%20by%20(pod%2Cnamespace)%20(container_memory_usage_bytes%7Bcontainer%3D""%2Cpod%3D~".%2B"%7D)',
                timeout=5)
            jout = r.json()
            if "status" in jout:
                if jout["status"] == "success":
                    for line in jout["data"]["result"]:
                        if "pod" not in line["metric"]:
                            continue
                        p = line["metric"]["pod"]
                        n = line["metric"]["namespace"]
                        if p in podOwners:
                            responce += rf"""cost_memory_usage_bytes{{pod="{p}",namespace="{n}" , pod_owner="{podOwners[p]}"}} {line["value"][1]} {nl}"""

        except Exception as e:
            logging.warning(f"Prometheus access error {e}")
        self.write(responce)


def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
        (r"/metrics", MainHandler),
    ])


if __name__ == "__main__":  # noqa
    create_tables()
    try:
        config.load_kube_config()
    except BaseException:
        # load_kube_config throws if there is no config, but does not document what it throws, so I can't rely on any particular type here
        config.load_incluster_config()
    core_api = client.CoreV1Api()
    apis_api = client.AppsV1Api()
    batch_api = client.BatchV1Api()
    podOwners = dict()
    lastPodAccess = dict()
    getPodFromDB()
    t1 = threading.Thread(target=watch_grab_owner)
    t1.start()
    app = make_app()
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()
