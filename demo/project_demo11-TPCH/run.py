import os
import time
import requests
import argparse
import subprocess
import pandas as pd
from itertools import islice
from shutil import which
from plumbum.cmd import rpk

# File locations
SCRIPT_DIR = os.path.join(os.path.dirname(__file__))
PROJECT_SQL = os.path.join(SCRIPT_DIR, "project.sql")
DIRECTORY_PATH = '/home/mpkostas/DatabaseSystems/TPC-H_V3.0.1/dbgen/exportFactor'

def main():
    # Command-line arguments
    parser = argparse.ArgumentParser()
    default_api_url = "http://localhost:8080"
    kafka_api_url = "http://localhost:9092"
    registry_url = "http://localhost:8081"
    parser.add_argument("--api-url", default=default_api_url, help=f"Feldera API URL (default: {default_api_url})")
    parser.add_argument("--kafka-url-for-connector", required=False, default=kafka_api_url,
                        help="Kafka URL from pipeline")
    parser.add_argument("--registry-url-for-connector", required=False, default=registry_url,
                        help="Schema registry URL from pipeline")
    parser.add_argument('--scaling-factor', default=1, type=int, required=False, help='TPCH-H scaling factor')

    args = parser.parse_args()
    directory_path = DIRECTORY_PATH + str(args.scaling_factor)
    prepare_redpanda(directory_path)
    prepare_feldera(args.api_url, args.kafka_url_for_connector)

PROGRAM_NAME = "tpc-h-program"
PIPELINE_NAME = "tpc-h-pipeline"

# NEEDS CHANGE
CONNECTORS = [
    ("tpch_nation", 'NATION', ["tpch_nation"], True),
    ("tpch_region", 'REGION', ["tpch_region"], True),
    ("tpch_part", 'PART', ["tpch_part"], True),
    ("tpch_supplier", 'SUPPLIER', ["tpch_supplier"], True),
    ("tpch_partsupp", 'PARTSUPP', ["tpch_partsupp"], True),
    ("tpch_customer", 'CUSTOMER', ["tpch_customer"], True),
    ("tpch_orders", 'ORDERS', "tpch_orders", True),
    ("tpch_lineitem", 'LINEITEM', "tpch_line_item", True)
]

def wait_for_status(api_url, pipeline_name, status):
    start = time.time()
    while requests.get(f"{api_url}/v0/pipelines/{pipeline_name}").json()["state"]["current_status"] != status:
        time.sleep(.1)
    return time.time() - start

def list_names(api_url, entity):
    return set([entity["name"] for entity in requests.get(f"{api_url}/v0/{entity}").json()])

def list_programs(api_url):
    return list_names(api_url, "programs")

def list_pipelines(api_url):
    return set([pipeline["descriptor"]["name"] for pipeline in requests.get(f"{api_url}/v0/pipelines").json()])

def list_connectors(api_url):
    return list_names(api_url, "connectors")

def stop_pipeline(api_url, pipeline_name, wait):
    requests.post(f"{api_url}/v0/pipelines/{pipeline_name}/shutdown").raise_for_status()
    if wait:
        return wait_for_status(api_url, pipeline_name, "Shutdown")

def start_pipeline(api_url, pipeline_name, wait):
    requests.post(f"{api_url}/v0/pipelines/{pipeline_name}/start").raise_for_status()
    if wait:
        return wait_for_status(api_url, pipeline_name, "Running")

def delete_pipeline(api_url, pipeline_name):
    requests.delete(f"{api_url}/v0/pipelines/{pipeline_name}").raise_for_status()

def delete_connector(api_url, connector_name):
    requests.delete(f"{api_url}/v0/connectors/{connector_name}").raise_for_status()

def delete_program(api_url, program_name):
    requests.delete(f"{api_url}/v0/programs/{program_name}").raise_for_status()

def prepare_redpanda(dir):
    # Prepare Kafka topics
    print("(Re-)creating Kafka topics...")
    rpk['topic', 'delete', 'tpch_nations']()
    rpk['topic', 'delete', 'tpch_regions']()
    rpk['topic', 'delete', 'tpch_parts']()
    rpk['topic', 'delete', 'tpch_suppliers']()
    rpk['topic', 'delete', 'tpch_partsupp']()
    rpk['topic', 'delete', 'tpch_customers']()
    rpk['topic', 'delete', 'tpch_orders']()
    rpk['topic', 'delete', 'tpch_lineitems']()

    rpk['topic', 'create', 'tpch_nations',
        '-c', 'retention.ms=-1', '-c', 'retention.bytes=-1']()
    rpk['topic', 'create', 'tpch_regions',
        '-c', 'retention.ms=-1', '-c', 'retention.bytes=-1']()
    rpk['topic', 'create', 'tpch_parts',
        '-c', 'retention.ms=-1', '-c', 'retention.bytes=-1']()
    rpk['topic', 'create', 'tpch_suppliers',
        '-c', 'retention.ms=-1', '-c', 'retention.bytes=-1']()
    rpk['topic', 'create', 'tpch_partsupp',
        '-c', 'retention.ms=-1', '-c', 'retention.bytes=-1']()
    rpk['topic', 'create', 'tpch_customers',
        '-c', 'retention.ms=-1', '-c', 'retention.bytes=-1']()
    rpk['topic', 'create', 'tpch_orders',
        '-c', 'retention.ms=-1', '-c', 'retention.bytes=-1']()
    rpk['topic', 'create', 'tpch_lineitems',
        '-c', 'retention.ms=-1', '-c', 'retention.bytes=-1']()

    print("(Re-)created Kafka topics")

    nations_csv = os.path.join(
        dir, 'nation.csv')
    regions_csv = os.path.join(
        dir, 'region.csv')
    parts_csv = os.path.join(
        dir, 'part.csv')
    suppliers_csv = os.path.join(
        dir, 'supplier.csv')
    partsupps_csv = os.path.join(
        dir, 'partsupp.csv')
    customers_csv = os.path.join(
        dir, 'customer.csv')
    orders_csv = os.path.join(
        dir, 'order.csv')
    lineitems_csv = os.path.join(
        dir, 'lineitem.csv')

    # Push test data to topics
    print('Pushing nations data to Kafka topic...')
    with open(nations_csv, 'r') as f:
        for n_lines in iter(lambda: tuple(islice(f, 1000)), ()):
            (rpk['topic', 'produce', 'tpch_nations',
             '-f', '%v'] << '\n'.join(n_lines))()

    print('Pushing regions data to Kafka topic...')
    with open(regions_csv, 'r') as f:
        for n_lines in iter(lambda: tuple(islice(f, 1000)), ()):
            (rpk['topic', 'produce',
                 'tpch_regions', '-f', '%v'] << '\n'.join(n_lines))()

    print('Pushing parts data to Kafka topic...')
    with open(parts_csv, 'r') as f:
        for n_lines in iter(lambda: tuple(islice(f, 1000)), ()):
            (rpk['topic', 'produce',
                 'tpch_parts', '-f', '%v'] << '\n'.join(n_lines))()

    print('Pushing suppliers data to Kafka topic...')
    with open(suppliers_csv, 'r') as f:
        for n_lines in iter(lambda: tuple(islice(f, 1000)), ()):
            (rpk['topic', 'produce',
                 'tpch_suppliers', '-f', '%v'] << '\n'.join(n_lines))()

    print('Pushing partsupps data to Kafka topic...')
    with open(partsupps_csv, 'r') as f:
        for n_lines in iter(lambda: tuple(islice(f, 1000)), ()):
            (rpk['topic', 'produce',
                 'tpch_partsupp', '-f', '%v'] << '\n'.join(n_lines))()

    print('Pushing customers data to Kafka topic...')
    with open(customers_csv, 'r') as f:
        for n_lines in iter(lambda: tuple(islice(f, 1000)), ()):
            (rpk['topic', 'produce',
                 'tpch_customers', '-f', '%v'] << '\n'.join(n_lines))()

    print('Pushing orders data to Kafka topic...')
    with open(orders_csv, 'r') as f:
        for n_lines in iter(lambda: tuple(islice(f, 1000)), ()):
            (rpk['topic', 'produce',
                 'tpch_orders', '-f', '%v'] << '\n'.join(n_lines))()

    print('Pushing lineitem data to Kafka topic...')
    with open(lineitems_csv, 'r') as f:
        for n_lines in iter(lambda: tuple(islice(f, 1000)), ()):
            (rpk['topic', 'produce',
                 'tpch_lineitems', '-f', '%v'] << '\n'.join(n_lines))()


def prepare_feldera(api_url, pipeline_to_redpanda_server):
    delete_extra = True
    if delete_extra:
        for pipeline in list_pipelines(api_url):
            stop_pipeline(api_url, pipeline, False)
        for pipeline in list_pipelines(api_url):
            stop_pipeline(api_url, pipeline, True)
        for pipeline in list_pipelines(api_url) - set([PIPELINE_NAME]):
            delete_pipeline(api_url, pipeline)
        for program in list_programs(api_url) - set([PROGRAM_NAME]):
            delete_program(api_url, program)
        for connector in list_connectors(api_url) - set([c[0] for c in CONNECTORS]):
            delete_connector(api_url, connector)

    # Create program
    program_sql = open(PROJECT_SQL).read()
    response = requests.put(f"{api_url}/v0/programs/{PROGRAM_NAME}", json={
        "description": "",
        "code": program_sql
    })
    response.raise_for_status()
    program_version = response.json()["version"]

    # Compile program
    print(f"Compiling program {PROGRAM_NAME} (version: {program_version})...")
    requests.post(f"{api_url}/v0/programs/{PROGRAM_NAME}/compile", json={"version": program_version}).raise_for_status()
    while True:
        status = requests.get(f"{api_url}/v0/programs/{PROGRAM_NAME}").json()["status"]
        print(f"Program status: {status}")
        if status == "Success":
            break
        elif status != "Pending" and status != "CompilingRust" and status != "CompilingSql":
            raise RuntimeError(f"Failed program compilation with status {status}")
        time.sleep(5)

    # Connectors
    connectors = []
    for (connector_name, stream, topic_topics, is_input) in CONNECTORS:
        # Create connector
        requests.put(f"{api_url}/v0/connectors/{connector_name}", json={
            "description": "",
            "config": {
                "format": {
                    "name": "csv",
                    "config": {}
                },
                "transport": {
                    "name": "kafka_" + ("input" if is_input else "output"),
                    "config": {
                        "bootstrap.servers": pipeline_to_redpanda_server,
                        "topic": topic_topics
                    }
                    if not is_input else
                    {
                        "bootstrap.servers": pipeline_to_redpanda_server,
                        "topics": topic_topics,
                        "auto.offset.reset": "earliest"
                    }
                }
            }
        })
        connectors.append({
            "connector_name": connector_name,
            "is_input": is_input,
            "name": connector_name,
            "relation_name": stream
        })

    # Create pipeline
    requests.put(f"{api_url}/v0/pipelines/{PIPELINE_NAME}", json={
        "description": "",
        "config": {"workers": 8},
        "program_name": PROGRAM_NAME,
        "connectors": connectors,
    }).raise_for_status()

    print("(Re)starting pipeline...")
    stop_pipeline()
    start_pipeline()
    print("Pipeline (re)started")

if __name__ == "__main__":
    main()