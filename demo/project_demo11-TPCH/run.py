import os
import time
import requests
import argparse
import subprocess
from shutil import which
from plumbum.cmd import rpk

# File locations
SCRIPT_DIR = os.path.join(os.path.dirname(__file__))
PROJECT_SQL = os.path.join(SCRIPT_DIR, "project.sql")

def main():
    # Command-line arguments
    parser = argparse.ArgumentParser()
    default_api_url = "http://localhost:8080"
    parser.add_argument("--api-url", default=default_api_url, help=f"Feldera API URL (default: {default_api_url})")
    parser.add_argument("--prepare-args", required=False, help="number of SecOps pipelines to simulate")
    parser.add_argument("--kafka-url-for-connector", required=False, default="redpanda:9092",
                        help="Kafka URL from pipeline")
    parser.add_argument("--registry-url-for-connector", required=False, default="http://redpanda:8081",
                        help="Schema registry URL from pipeline")
    parser.add_argument('--delete-extra', default=False, action=argparse.BooleanOptionalAction, help='delete other programs, pipelines, and connectors (default: --no-delete-extra)')

    args = parser.parse_args()
    prepare_redpanda()
    prepare_feldera(args.api_url, args.kafka_url_for_connector, args.registry_url_for_connector, args.delete_extra)

PROGRAM_NAME = "tpc-h-program"
PIPELINE_NAME = "tpc-h-pipeline"

# NEEDS CHANGE
CONNECTORS = [
    ("secops_pipeline", 'PIPELINE', ["secops_pipeline"], True),
    ("secops_pipeline_sources", 'PIPELINE_SOURCES', ["secops_pipeline_sources"], True),
    ("secops_artifact", 'ARTIFACT', ["secops_artifact"], True),
    ("secops_vulnerability", 'VULNERABILITY', ["secops_vulnerability"], True),
    ("secops_cluster", 'K8SCLUSTER', ["secops_cluster"], True),
    ("secops_k8sobject", 'K8SOBJECT', ["secops_k8sobject"], True),
    ("secops_vulnerability_stats", 'K8SCLUSTER_VULNERABILITY_STATS', "secops_vulnerability_stats", False),
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

#NEEDS CHANGE
def prepare_redpanda():
    # Prepare Kafka topics
    print("(Re-)creating Kafka topics...")
    rpk['topic', 'delete', 'fraud_demo_large_demographics']()
    rpk['topic', 'delete', 'fraud_demo_large_transactions']()
    rpk['topic', 'delete', 'fraud_demo_large_enriched']()
    rpk['topic', 'create', 'fraud_demo_large_demographics',
        '-c', 'retention.ms=-1', '-c', 'retention.bytes=-1']()
    rpk['topic', 'create', 'fraud_demo_large_transactions',
        '-c', 'retention.ms=-1', '-c', 'retention.bytes=-1']()
    rpk['topic', 'create', 'fraud_demo_large_enriched']()
    print("(Re-)created Kafka topics")

    transactions_csv = os.path.join(
        SCRIPT_DIR, 'transactions.csv')
    demographics_csv = os.path.join(
        SCRIPT_DIR, 'demographics.csv')

    if not os.path.exists(transactions_csv):
        from plumbum.cmd import gdown
        print("Downloading transactions.csv (~2 GiB)...")
        gdown['1YuiKl-MMbEujTOwPOyxEoVCh088y9jxI',
              '--output', transactions_csv]()

    # Push test data to topics
    print('Pushing demographics data to Kafka topic...')
    with open(demographics_csv, 'r') as f:
        for n_lines in iter(lambda: tuple(islice(f, 1000)), ()):
            (rpk['topic', 'produce', 'fraud_demo_large_demographics',
             '-f', '%v'] << '\n'.join(n_lines))()
    print('Pushing transaction data to Kafka topic...')
    with open(transactions_csv, 'r') as f:
        for n_lines in iter(lambda: tuple(islice(f, 8_000)), ()):
            (rpk['topic', 'produce',
                 'fraud_demo_large_transactions', '-f', '%v'] << '\n'.join(n_lines))()


def prepare_feldera(api_url, pipeline_to_redpanda_server, pipeline_to_schema_registry, delete_extra):
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
        requests.put(f"{api_url}/v0/connectors/{connector_name}", json={
            "description": "",
            "config": {
                "format": {
                    "name": "json",
                    "config": {
                        "update_format": "insert_delete"
                    }
                },
                "transport": {
                    "name": "kafka_" + ("input" if is_input else "output"),
                    "config": {
                        "bootstrap.servers": pipeline_to_redpanda_server,
                        "topic": topic_topics
                    } if not is_input else (
                        {
                            "bootstrap.servers": pipeline_to_redpanda_server,
                            "topics": topic_topics,
                            "auto.offset.reset": "earliest",
                            "group.id": "secops_pipeline_sources",
                            "enable.auto.commit": "true",
                            "enable.auto.offset.store": "true",
                        }
                        if stream == "PIPELINE_SOURCES" else
                        {
                            "bootstrap.servers": pipeline_to_redpanda_server,
                            "topics": topic_topics,
                            "auto.offset.reset": "earliest"
                        }
                    )
                }
            }
        })
        connectors.append({
            "connector_name": connector_name,
            "is_input": is_input,
            "name": connector_name,
            "relation_name": stream
        })

    #NEEDS CHANGE
    schema = """{
            "type": "record",
            "name": "k8scluster_vulnerability_stats",
            "fields": [
                { "name": "k8scluster_id", "type": "long" },
                { "name": "k8scluster_name", "type": "string" },
                { "name": "total_vulnerabilities", "type": "long" },
                { "name": "most_severe_vulnerability", "type": ["null","int"] }
            ]
        }"""

    #NEEDS CHANGE
    if pipeline_to_schema_registry:
        requests.put(f"{api_url}/v0/connectors/secops_vulnerability_stats_avro", json={
            "description": "",
            "config": {
                "format": {
                    "name": "avro",
                    "config": {
                        "schema": schema,
                        "registry_urls": [pipeline_to_schema_registry],
                    }
                },
                "transport": {
                    "name": "kafka_output",
                    "config": {
                        "bootstrap.servers": pipeline_to_redpanda_server,
                        "topic": "secops_vulnerability_stats_avro",
                        "headers": [{"key": "header1", "value": "this is a string"},
                                    {"key": "header2", "value": list(b'byte array')}]
                    }
                }
            }
        })
        connectors.append({
            "connector_name": "secops_vulnerability_stats_avro",
            "is_input": False,
            "name": "secops_vulnerability_stats_avro",
            "relation_name": "k8scluster_vulnerability_stats"
        })

    # Create pipeline
    requests.put(f"{api_url}/v0/pipelines/{PIPELINE_NAME}", json={
        "description": "",
        "config": {"workers": 8},
        "program_name": PROGRAM_NAME,
        "connectors": connectors,
    }).raise_for_status()

if __name__ == "__main__":
    main()