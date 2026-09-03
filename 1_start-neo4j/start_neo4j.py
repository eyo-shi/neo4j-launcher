import time

from utils.neo4j_utils import (
    deploy_neo4j_server,
    is_neo4j_server_up,
    print_connection_info,
    reset_neo4j_server,
    wait_for_external_endpoint,
    wait_for_neo4j_server,
)

if __name__ == "__main__":
    print("Starting Neo4j server...")

    if is_neo4j_server_up():
        print("Neo4j server is already running.")
    else:
        try:
            deploy_neo4j_server()
        except Exception:
            print("Deployment may already exist. Resetting Neo4j server...")
            reset_neo4j_server()

    wait_for_neo4j_server()
    wait_for_external_endpoint()
    print_connection_info()

    print("Neo4j is running. Press Ctrl+C to stop.")
    while True:
        if not is_neo4j_server_up():
            print("Neo4j server is down. Attempting to restart...")
            reset_neo4j_server()
            wait_for_neo4j_server()
            print_connection_info()
        time.sleep(30)
