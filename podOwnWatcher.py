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
import datetime


def create_tables():
    psqlURL = os.getenv("PSQL_URL")
    if not psqlURL:
        logging.warning("PSQL_URL env is not defined")
        sys.exit()
    commands = (
        """
        CREATE TABLE IF NOT EXISTS pod(
            pod_name varchar(64) PRIMARY KEY,
            namespace varchar(64),
            release varchar(64),
            pod_owner VARCHAR(140) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_pod ON pod USING hash (pod_name)
        """,
        """
        CREATE TABLE IF NOT EXISTS pvc (
            pvc_name varchar(64) PRIMARY KEY,
            pod varchar(64),
            capacity varchar(64),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR(64),
            deleted_at TIMESTAMP
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_pvc ON pvc USING hash (pvc_name)
        """,
        """
        CREATE TABLE IF NOT EXISTS metrics_hour(
            id SERIAL PRIMARY KEY,
            metric varchar(64),
            pod varchar(64),
            value real,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
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


def put_pvc(pvc):
    psqlURL = os.getenv("PSQL_URL")
    conn = None
    try:
        conn = psycopg2.connect(psqlURL)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO pvc (pvc_name,pod,capacity,status) values(%s, %s, %s, 'bound');", (pvc['name'], pvc['pod'], pvc['capacity']))
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.warning(error)
    finally:
        if conn is not None:
            conn.close()

def put_metric(pod, metric, value):
    psqlURL = os.getenv("PSQL_URL")
    conn = None
    try:
        conn = psycopg2.connect(psqlURL)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO metrics_hour (pod,metric,value) values(%s, %s, %s );", (pod, metric, value))
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
            "INSERT INTO pod (pod_name,pod_owner,namespace,release) values(%s, %s, %s, %s);", (pod, owner, namespace, release))
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
    global podRelease
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
                        for opt in ["release", "app.kubernetes.io/name", "k8s-app", "app", "component"]:
                            if not event['object'].metadata.labels:
                                break
                            if opt in event['object'].metadata.labels:
                                release = event['object'].metadata.labels[opt]
                            if release:
                                break
                        put_pod(event['object'].metadata.name, owner, namespace,release)
                        podOwners[event['object'].metadata.name] = owner
                        podRelease[event['object'].metadata.name] = release
                    if owner == "" and event['object'].metadata.name.startswith("kube-proxy"):
                        owner = "System:kube-proxy"
                        release = event['object'].metadata.name
                    # if owner == "" and namespace != "kube-system":
                    #    logging.warning("No owner: %s" % (event))
                    if len(event['object'].spec.volumes):
                        for v in event['object'].spec.volumes:
                            if v.persistent_volume_claim:
                                if v.persistent_volume_claim.claim_name:
                                    pvc=getPvcInfo(v.persistent_volume_claim.claim_name,event['object'].metadata.namespace)
                                    pvc['pod'] = event['object'].metadata.name
                                    put_pvc(pvc)

                logging.info("Event: %s %s" % (event['type'], event['object'].metadata.name))
        except (ReadTimeoutError, IOError):
            time.sleep(5)

def updateMetrics():
    global podOwners
    global podRelease
    global pvcRelease
    global lastPodAccess
    global cpu_seconds
    global mem_bytes
    global pvc_gb
    global lastrun
    promURL = os.getenv("PROMETHEUS_URL")
    while True:
        if lastrun > datetime.datetime.now() + datetime.timedelta(hours=-1):
            time.sleep(600)
            continue
        try:
            r = requests.get(
                promURL +
                '/api/v1/query?query=sum%20by%20(pod%2Cnamespace)%20(max_over_time(container_memory_usage_bytes%5B1h%5D))',
                timeout=5)
            jout = r.json()
            if "status" in jout:
                if jout["status"] == "success":
                    for line in jout["data"]["result"]:
                        if "pod" not in line["metric"]:
                            continue
                        value = 0
                        p = line["metric"]["pod"]
                        try:
                            value = int(float(line["value"][1]))    
                        except ValueError:
                            continue
                        put_metric(p,"container_memory_usage_bytes_hour",value)
                        if p in mem_bytes:
                                mem_bytes[p] += value
                        else:
                                mem_bytes[p] = value
        except Exception as e:
            logging.warning(f"Prometheus container_memory_usage_bytes  access error {e}")
        try:
            r = requests.get(
                promURL +
                '/api/v1/query?query=sum%20by%20(pod%2Cnamespace)%20(container_cpu_usage_seconds_total%7Bcpu%3D"total"%7D)',
                timeout=5)
            jout = r.json()
            if "status" in jout:
                if jout["status"] == "success":
                    for line in jout["data"]["result"]:
                        if "pod" not in line["metric"]:
                            continue
                        value = 0
                        p = line["metric"]["pod"]
                        try:
                            value = float(line["value"][1])    
                        except ValueError:
                            continue
                        put_metric(p,"container_cpu_usage_seconds_total",value)
                        if p in mem_bytes:
                                mem_bytes[p] += value
                        else:
                                mem_bytes[p] = value
        except Exception as e:
            logging.warning(f"Prometheus access error {e}")
        # get_list_PVC and put (pod, amount, type)     
        time.sleep(3600)

def getPodFromDB():
    global podOwners
    psqlURL = os.getenv("PSQL_URL")
    conn = None
    try:
        conn = psycopg2.connect(psqlURL)
        cur = conn.cursor()
        cur.execute("select pod_name,pod_owner,release from pod;")
        allrows = cur.fetchall()
        for row in allrows:
            podOwners[row[0]] = row[1]
            podRelease[row[0]] = row[2]
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.warning(error)
    finally:
        if conn is not None:
            conn.close()
            logging.warning(f"Loadet from DB {len(podOwners)} objects\n")


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        global podOwners
        global podRelease
        global mem_bytes
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
                            responce += rf"""cost_cpu_seconds_total{{pod="{p}",pod_owner="{podOwners[p]}", pod_release="{podRelease[p]}" }} {line["value"][1]} {nl}"""

        except Exception as e:
            logging.warning(f"Prometheus access error {e}")
        responce += """
# HELP cost_memory_usage_bytes_total number of bytes-hours used by pods.
# TYPE cost_memory_usage_bytes_total counter
              """ + nl
        for p in mem_bytes:
            if p in podOwners:
                responce += rf"""cost_memory_usage_bytes_total{{pod="{p}", pod_owner="{podOwners[p]}", pod_release="{podRelease[p]}" }} {mem_bytes[p]} {nl}"""
        self.write(responce)

def getPvcInfo(name,namespace):
    out= dict()
    out['name']=name
    out['namespace']=namespace
    global core_api
    global apis_api
    try:
        pvc = core_api.read_namespaced_persistent_volume_claim(name=name, namespace=namespace)
        if pvc.spec.storage_class_name:
            out['storage_class_name'] = pvc.spec.storage_class_name
        if pvc.status.capacity and "storage" in pvc.status.capacity:
            out['capacity'] = pvc.status.capacity["storage"]
        elif pvc.spec.resources.requests.storage:
            out['capacity'] = pvc.status.capacity.storage
        return out
    except ApiException as e:
        logging.warning("Get pvc info failed: {}".format(e))
        return out

def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
        (r"/metrics", MainHandler),
    ])

def getLastRun():
    psqlURL = os.getenv("PSQL_URL")
    conn = None
    dt=datetime.datetime.now() + datetime.timedelta(hours=-2)
    try:
        conn = psycopg2.connect(psqlURL)
        cur = conn.cursor()
        cur.execute("select created_at from metrics_hour order by created_at desc;")
        dt=cur.fetchone()[0]
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.warning(error)
    finally:
        if conn is not None:
            conn.close()
    return dt

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
    lastrun = getLastRun()
    podOwners = dict()
    podRelease = dict()
    pvcRelease = dict()
    lastPodAccess = dict()
    cpu_seconds = dict()
    mem_bytes = dict()
    pvc_gb = dict()
    getPodFromDB()
    t1 = threading.Thread(target=watch_grab_owner)
    t1.start()
    t2 = threading.Thread(target=updateMetrics)
    t2.start()
    app = make_app()
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()
