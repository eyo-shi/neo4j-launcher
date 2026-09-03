import os
import time

from kubernetes import client, config
from neo4j import GraphDatabase

config.load_incluster_config()

NEO4J_IMAGE = os.getenv("NEO4J_IMAGE") or "neo4j:2026.07.1"
NEO4J_SERVICE_TYPE = os.getenv("NEO4J_SERVICE_TYPE") or "LoadBalancer"
NEO4J_NODE_PORT_BOLT = int(os.getenv("NEO4J_NODE_PORT_BOLT") or "30687")
NEO4J_NODE_PORT_HTTP = int(os.getenv("NEO4J_NODE_PORT_HTTP") or "30474")


def get_current_namespace() -> str:
    with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace", "r") as f:
        return f.read().strip()


def get_parent_pod_name() -> str:
    with open("/downward-api/pod.name", "r") as f:
        return f.read().strip()


def get_parent_pod_uid() -> str:
    with open("/downward-api/pod.uid", "r") as f:
        return f.read().strip()


def get_pvc_name_from_parent_pod() -> str:
    pod_name = get_parent_pod_name()
    pod_spec = client.CoreV1Api().read_namespaced_pod(
        name=pod_name, namespace=get_current_namespace()
    )
    volume_mounts = pod_spec.spec.containers[0].volume_mounts
    for volume_mount in volume_mounts:
        if volume_mount.mount_path == "/home/cdsw":
            return volume_mount.name
    raise RuntimeError("PVC volume mount for /home/cdsw not found")


def get_engine_id() -> str:
    return (os.getenv("CDSW_ENGINE_ID") or "local").strip()


def get_neo4j_credentials() -> dict:
    username = os.getenv("NEO4J_USERNAME") or "neo4j"
    password = os.getenv("NEO4J_PASSWORD") or "password"
    return {
        "username": username,
        "password": password,
        "uri": f"bolt://{get_neo4j_service_name()}.{get_current_namespace()}:7687",
        "database": "neo4j",
    }


def get_owner_reference() -> client.V1OwnerReference:
    return client.V1OwnerReference(
        api_version="v1",
        kind="Pod",
        name=get_parent_pod_name(),
        uid=get_parent_pod_uid(),
    )


def get_neo4j_service_name() -> str:
    return f"cml-neo4j-{get_engine_id()}"


def get_deployment_name() -> str:
    return f"neo4j-{get_engine_id()}"


def create_deployment_spec_for_neo4j() -> client.V1Deployment:
    namespace = get_current_namespace()
    engine_id = get_engine_id()
    credentials = get_neo4j_credentials()

    return client.V1Deployment(
        api_version="apps/v1",
        metadata=client.V1ObjectMeta(
            name=get_deployment_name(),
            labels={"app": get_deployment_name()},
            owner_references=[get_owner_reference()],
            namespace=namespace,
        ),
        spec=client.V1DeploymentSpec(
            replicas=1,
            selector=client.V1LabelSelector(match_labels={"app": get_deployment_name()}),
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(labels={"app": get_deployment_name()}),
                spec=client.V1PodSpec(
                    security_context=client.V1PodSecurityContext(
                        fs_group=8536,
                        run_as_group=8536,
                        run_as_user=8536,
                        run_as_non_root=True,
                    ),
                    containers=[
                        client.V1Container(
                            name="neo4j",
                            image=NEO4J_IMAGE,
                            ports=[
                                client.V1ContainerPort(
                                    container_port=7687, name="bolt"
                                ),
                                client.V1ContainerPort(
                                    container_port=7474, name="http"
                                ),
                            ],
                            env=[
                                client.V1EnvVar(
                                    name="NEO4J_AUTH",
                                    value=f"{credentials['username']}/{credentials['password']}",
                                ),
                                client.V1EnvVar(
                                    name="NEO4J_apoc_export_file_enabled", value="true"
                                ),
                                client.V1EnvVar(
                                    name="NEO4J_apoc_import_file_enabled", value="true"
                                ),
                                client.V1EnvVar(
                                    name="NEO4J_apoc_import_file_use__neo4j__config",
                                    value="true",
                                ),
                                client.V1EnvVar(
                                    name="NEO4J_PLUGINS",
                                    value='["apoc","graph-data-science"]',
                                ),
                                client.V1EnvVar(
                                    name="NEO4J_server_http_listen__address",
                                    value="0.0.0.0:7474",
                                ),
                                client.V1EnvVar(
                                    name="NEO4J_server_bolt_listen__address",
                                    value="0.0.0.0:7687",
                                ),
                            ],
                            resources=client.V1ResourceRequirements(
                                requests={"cpu": "1", "memory": "4Gi"},
                                limits={"cpu": "1", "memory": "4Gi"},
                            ),
                            volume_mounts=[
                                client.V1VolumeMount(
                                    name="neo4j-plugins",
                                    mount_path="/plugins",
                                ),
                                client.V1VolumeMount(
                                    name="filesystem-access",
                                    mount_path="/data",
                                    sub_path="neo4j-volume",
                                ),
                            ],
                        )
                    ],
                    volumes=[
                        client.V1Volume(
                            name="neo4j-plugins",
                            empty_dir=client.V1EmptyDirVolumeSource(),
                        ),
                        client.V1Volume(
                            name="filesystem-access",
                            persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                                claim_name=get_pvc_name_from_parent_pod()
                            ),
                        ),
                    ],
                ),
            ),
        ),
    )


def create_service_spec_for_neo4j() -> client.V1Service:
    namespace = get_current_namespace()
    service_name = get_neo4j_service_name()

    bolt_port = client.V1ServicePort(
        port=7687, target_port=7687, name="bolt", protocol="TCP"
    )
    http_port = client.V1ServicePort(
        port=7474, target_port=7474, name="http", protocol="TCP"
    )

    if NEO4J_SERVICE_TYPE == "NodePort":
        bolt_port.node_port = NEO4J_NODE_PORT_BOLT
        http_port.node_port = NEO4J_NODE_PORT_HTTP

    annotations = {}
    if NEO4J_SERVICE_TYPE == "LoadBalancer":
        annotations = {
            "service.beta.kubernetes.io/aws-load-balancer-healthcheck-protocol": "TCP",
            "service.beta.kubernetes.io/aws-load-balancer-healthcheck-port": "7687",
        }

    return client.V1Service(
        api_version="v1",
        metadata=client.V1ObjectMeta(
            name=service_name,
            labels={"app": get_deployment_name()},
            owner_references=[get_owner_reference()],
            namespace=namespace,
            annotations=annotations or None,
        ),
        spec=client.V1ServiceSpec(
            type=NEO4J_SERVICE_TYPE,
            selector={"app": get_deployment_name()},
            ports=[bolt_port, http_port],
        ),
    )


def deploy_neo4j_server() -> None:
    api_instance = client.AppsV1Api()
    service_api_instance = client.CoreV1Api()

    api_instance.create_namespaced_deployment(
        namespace=get_current_namespace(),
        body=create_deployment_spec_for_neo4j(),
    )
    service_api_instance.create_namespaced_service(
        namespace=get_current_namespace(),
        body=create_service_spec_for_neo4j(),
    )


def stop_neo4j_server() -> None:
    api_instance = client.AppsV1Api()
    service_api_instance = client.CoreV1Api()
    namespace = get_current_namespace()

    api_instance.delete_namespaced_deployment(
        name=get_deployment_name(),
        namespace=namespace,
    )
    service_api_instance.delete_namespaced_service(
        name=get_neo4j_service_name(),
        namespace=namespace,
    )


def reset_neo4j_server() -> None:
    try:
        stop_neo4j_server()
    except Exception as e:
        print(f"Failed to stop Neo4j server: {e}")
    deploy_neo4j_server()


def is_neo4j_server_up() -> bool:
    credentials = get_neo4j_credentials()
    with GraphDatabase.driver(
        credentials["uri"], auth=(credentials["username"], credentials["password"])
    ) as driver:
        try:
            driver.verify_connectivity()
            return True
        except Exception:
            return False


def wait_for_neo4j_server(max_retries: int = 30, sleep_duration: int = 10) -> None:
    credentials = get_neo4j_credentials()
    with GraphDatabase.driver(
        credentials["uri"], auth=(credentials["username"], credentials["password"])
    ) as driver:
        for _ in range(max_retries):
            try:
                driver.verify_connectivity()
                return
            except Exception as e:
                print(f"Neo4j server is not ready yet. Retrying... {e}")
                time.sleep(sleep_duration)
    raise RuntimeError("Neo4j server is not ready yet. Max retries exceeded.")


def get_external_endpoints() -> dict:
    service_api = client.CoreV1Api()
    service = service_api.read_namespaced_service(
        name=get_neo4j_service_name(),
        namespace=get_current_namespace(),
    )

    endpoints = {
        "service_type": service.spec.type,
        "internal_bolt": f"bolt://{get_neo4j_service_name()}.{get_current_namespace()}:7687",
        "internal_browser": f"http://{get_neo4j_service_name()}.{get_current_namespace()}:7474",
        "external_bolt": None,
        "external_browser": None,
    }

    if service.spec.type == "LoadBalancer":
        if service.status.load_balancer.ingress:
            host = (
                service.status.load_balancer.ingress[0].hostname
                or service.status.load_balancer.ingress[0].ip
            )
            if host:
                endpoints["external_bolt"] = f"bolt://{host}:7687"
                endpoints["external_browser"] = f"http://{host}:7474"
    elif service.spec.type == "NodePort":
        for port in service.spec.ports:
            if port.name == "bolt" and port.node_port:
                endpoints["external_bolt"] = f"bolt://<node-ip>:{port.node_port}"
            if port.name == "http" and port.node_port:
                endpoints["external_browser"] = f"http://<node-ip>:{port.node_port}"

    return endpoints


def wait_for_external_endpoint(max_retries: int = 30, sleep_duration: int = 10) -> dict:
    for _ in range(max_retries):
        endpoints = get_external_endpoints()
        if endpoints["external_bolt"] or endpoints["external_browser"]:
            return endpoints
        if endpoints["service_type"] == "ClusterIP":
            return endpoints
        print("Waiting for external endpoint to be assigned...")
        time.sleep(sleep_duration)
    return get_external_endpoints()


def _advertised_address(endpoint: str | None) -> str | None:
    if not endpoint:
        return None
    from urllib.parse import urlparse

    parsed = urlparse(endpoint)
    return parsed.netloc or None


def configure_external_advertised_addresses() -> None:
    endpoints = get_external_endpoints()
    http_address = _advertised_address(endpoints.get("external_browser"))
    bolt_address = _advertised_address(endpoints.get("external_bolt"))
    if not http_address and not bolt_address:
        return

    api_instance = client.AppsV1Api()
    deployment = api_instance.read_namespaced_deployment(
        name=get_deployment_name(),
        namespace=get_current_namespace(),
    )
    container = deployment.spec.template.spec.containers[0]
    env_by_name = {env.name: env for env in container.env}

    if http_address:
        env_by_name["NEO4J_server_http_advertised__address"] = client.V1EnvVar(
            name="NEO4J_server_http_advertised__address",
            value=http_address,
        )
    if bolt_address:
        env_by_name["NEO4J_server_bolt_advertised__address"] = client.V1EnvVar(
            name="NEO4J_server_bolt_advertised__address",
            value=bolt_address,
        )

    container.env = list(env_by_name.values())
    api_instance.patch_namespaced_deployment(
        name=get_deployment_name(),
        namespace=get_current_namespace(),
        body=deployment,
    )
    print(
        "Configured Neo4j advertised addresses for external access: "
        f"http={http_address}, bolt={bolt_address}"
    )


def get_cml_application_base_url() -> str | None:
    domain = os.getenv("CDSW_DOMAIN")
    engine_id = os.getenv("CDSW_ENGINE_ID")
    subdomain = os.getenv("NEO4J_LAUNCHER_SUBDOMAIN", "neo4j-launcher")
    if domain and engine_id:
        return f"https://{subdomain}-{engine_id}.{domain}"
    return None


def build_proxied_browser_path(_external_bolt: str | None = None) -> str:
    from urllib.parse import quote

    base_url = get_cml_application_base_url()
    if not base_url:
        return "/browser/"

    # Use HTTPS Query API via the CML application proxy instead of external Bolt.
    connect_url = f"{base_url}/"
    return f"/browser/?connectURL={quote(connect_url, safe='')}"


def get_internal_browser_url() -> str | None:
    try:
        return get_external_endpoints()["internal_browser"]
    except Exception:
        return None


def get_connection_info() -> dict:
    credentials = get_neo4j_credentials()
    info = {
        "status": "starting",
        "username": credentials["username"],
        "password": credentials["password"],
        "internal_bolt": None,
        "internal_browser": None,
        "external_bolt": None,
        "external_browser": None,
        "service_type": None,
        "port_forward_command": None,
        "proxied_browser_path": "/browser/",
        "http_api_connect_url": None,
    }

    try:
        endpoints = get_external_endpoints()
    except Exception as exc:
        info["message"] = f"Waiting for Neo4j service: {exc}"
        return info

    info.update(
        {
            "status": "running" if is_neo4j_server_up() else "starting",
            "internal_bolt": endpoints["internal_bolt"],
            "internal_browser": endpoints["internal_browser"],
            "external_bolt": endpoints["external_bolt"],
            "external_browser": endpoints["external_browser"],
            "service_type": endpoints["service_type"],
        }
    )
    if endpoints["service_type"] == "ClusterIP":
        info["port_forward_command"] = (
            f"kubectl port-forward svc/{get_neo4j_service_name()} 7474:7474 7687:7687"
        )
    info["proxied_browser_path"] = build_proxied_browser_path()
    info["http_api_connect_url"] = get_cml_application_base_url()
    if info["http_api_connect_url"]:
        info["http_api_connect_url"] = f"{info['http_api_connect_url']}/"
    return info


def print_connection_info() -> None:
    info = get_connection_info()

    print("\n=== Neo4j Connection Info ===")
    print(f"Username: {info['username']}")
    print(f"Password: {info['password']}")
    print(f"Internal Bolt URI: {info['internal_bolt']}")
    print(f"Internal Browser:  {info['internal_browser']}")
    if info["external_bolt"]:
        print(f"External Bolt URI: {info['external_bolt']}")
    if info["external_browser"]:
        print(f"External Browser:  {info['external_browser']}")
    print(f"Proxied Browser:   {info['proxied_browser_path']}")
    if info["http_api_connect_url"]:
        print(f"HTTP API Connect:  {info['http_api_connect_url']}")
    if info["port_forward_command"]:
        print("Service type is ClusterIP. Use port-forward for external access:")
        print(f"  {info['port_forward_command']}")
    print("=============================\n")


def run_neo4j_supervisor() -> None:
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
    endpoints = wait_for_external_endpoint()
    if endpoints.get("external_browser") or endpoints.get("external_bolt"):
        try:
            configure_external_advertised_addresses()
            wait_for_neo4j_server()
        except Exception as exc:
            print(f"Failed to configure external advertised addresses: {exc}")
    print_connection_info()

    print("Neo4j is running. Monitoring in background.")
    while True:
        if not is_neo4j_server_up():
            print("Neo4j server is down. Attempting to restart...")
            reset_neo4j_server()
            wait_for_neo4j_server()
            print_connection_info()
        time.sleep(30)
