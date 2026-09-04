import json
import os
import socket
import time
import urllib.error
import urllib.request

from kubernetes import client, config
from kubernetes.client.rest import ApiException
from neo4j import GraphDatabase

config.load_incluster_config()

_supervisor_state: dict = {
    "phase": "initializing",
    "error": None,
    "updated_at": None,
}
_pod_logs_last_printed_at = 0.0
POD_LOGS_PRINT_INTERVAL_SECONDS = 60

NEO4J_IMAGE = os.getenv("NEO4J_IMAGE") or "neo4j:2026.07.1"
NEO4J_SERVICE_TYPE = os.getenv("NEO4J_SERVICE_TYPE") or "LoadBalancer"
NEO4J_NODE_PORT_BOLT = int(os.getenv("NEO4J_NODE_PORT_BOLT") or "30687")
NEO4J_NODE_PORT_HTTP = int(os.getenv("NEO4J_NODE_PORT_HTTP") or "30474")
def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if not normalized:
        return default
    return normalized in ("1", "true", "yes", "on")


def _parse_neo4j_plugins() -> list[str]:
    raw = (os.getenv("NEO4J_PLUGINS") or "").strip()
    if not raw or raw.lower() in ("[]", "none", "false", "0", "null"):
        return []
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return [str(plugin).strip() for plugin in parsed if str(plugin).strip()]
    except json.JSONDecodeError:
        pass
    return [plugin.strip() for plugin in raw.split(",") if plugin.strip()]


def _neo4j_plugins_json() -> str | None:
    plugins = _parse_neo4j_plugins()
    if not plugins:
        return None
    return json.dumps(plugins)


def get_neo4j_plugins_display() -> str:
    plugins = _parse_neo4j_plugins()
    if not plugins:
        return "[] (disabled — NEO4J_PLUGINS env var omitted)"
    return json.dumps(plugins)


NEO4J_USE_PVC = _env_bool("NEO4J_USE_PVC", default=False)
NEO4J_STARTUP_TIMEOUT_SECONDS = int(
    os.getenv("NEO4J_STARTUP_TIMEOUT_SECONDS") or "1200"
)
NEO4J_CONTAINER_UID = 7474
NEO4J_CONTAINER_GID = 7474

POD_FAILURE_MARKERS = (
    "CrashLoopBackOff",
    "ImagePullBackOff",
    "ErrImagePull",
    "OOMKilled",
    "CreateContainerConfigError",
    "Init:CrashLoopBackOff",
    "Init:Error",
    "Error",
)
NO_POD_GRACE_SECONDS = 60


def get_current_namespace() -> str:
    with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace", "r") as f:
        return f.read().strip()


def _read_downward_api_file(path: str) -> str | None:
    try:
        with open(path, "r") as f:
            value = f.read().strip()
            return value or None
    except OSError:
        return None


def get_parent_pod_name() -> str:
    pod_name = _read_downward_api_file("/downward-api/pod.name")
    if pod_name:
        return pod_name
    hostname = (os.getenv("HOSTNAME") or "").strip()
    if hostname:
        return hostname
    raise RuntimeError(
        "Cannot determine parent pod name. "
        "Expected /downward-api/pod.name or HOSTNAME to be set."
    )


def get_parent_pod_uid() -> str:
    pod_uid = _read_downward_api_file("/downward-api/pod.uid")
    if pod_uid:
        return pod_uid
    pod = client.CoreV1Api().read_namespaced_pod(
        name=get_parent_pod_name(),
        namespace=get_current_namespace(),
    )
    if pod.metadata.uid:
        return pod.metadata.uid
    raise RuntimeError("Cannot determine parent pod UID.")


def get_pvc_name_from_parent_pod() -> str:
    pod_name = get_parent_pod_name()
    pod_spec = client.CoreV1Api().read_namespaced_pod(
        name=pod_name, namespace=get_current_namespace()
    )
    volume_mount_name = None
    for volume_mount in pod_spec.spec.containers[0].volume_mounts or []:
        if volume_mount.mount_path == "/home/cdsw":
            volume_mount_name = volume_mount.name
            break
    if not volume_mount_name:
        raise RuntimeError("PVC volume mount for /home/cdsw not found")

    for volume in pod_spec.spec.volumes or []:
        if volume.name == volume_mount_name and volume.persistent_volume_claim:
            return volume.persistent_volume_claim.claim_name

    raise RuntimeError(
        f"PVC claim name not found for volume mount '{volume_mount_name}'"
    )


def get_engine_id() -> str:
    return (os.getenv("CDSW_ENGINE_ID") or "local").strip()


def get_neo4j_credentials() -> dict:
    username = os.getenv("NEO4J_USERNAME") or "neo4j"
    password = os.getenv("NEO4J_PASSWORD") or "password"
    return {
        "username": username,
        "password": password,
        "uri": f"bolt://{get_neo4j_service_name()}:7687",
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


def get_cml_application_base_url() -> str | None:
    domain = os.getenv("CDSW_DOMAIN")
    engine_id = os.getenv("CDSW_ENGINE_ID")
    subdomain = os.getenv("NEO4J_LAUNCHER_SUBDOMAIN", "neo4j-launcher")
    if domain and engine_id:
        return f"https://{subdomain}-{engine_id}.{domain}"
    return None


def _cml_proxy_http_advertised_address() -> str | None:
    base_url = get_cml_application_base_url()
    if not base_url:
        return None

    from urllib.parse import urlparse

    parsed = urlparse(base_url)
    if not parsed.hostname:
        return None
    if parsed.port:
        return f"{parsed.hostname}:{parsed.port}"
    if parsed.scheme == "https":
        return f"{parsed.hostname}:443"
    return parsed.hostname


def _normalize_k8s_memory(value: str | None, default: str = "4Gi") -> str:
    if value is None:
        return default
    normalized = value.strip()
    if not normalized:
        return default
    if normalized.isdigit():
        return f"{normalized}Gi"
    upper = normalized.upper()
    if upper.endswith("G") and not upper.endswith("GI"):
        return f"{normalized[:-1]}Gi"
    if upper.endswith("M") and not upper.endswith("MI"):
        return f"{normalized[:-1]}Mi"
    return normalized


def get_neo4j_memory() -> str:
    return _normalize_k8s_memory(os.getenv("NEO4J_MEMORY"), "4Gi")


def _parse_memory_to_mib(memory: str) -> int:
    value = memory.strip()
    if value.endswith("Gi"):
        return int(float(value[:-2]) * 1024)
    if value.endswith("G"):
        return int(float(value[:-1]) * 1024)
    if value.endswith("Mi"):
        return int(value[:-2])
    if value.endswith("M"):
        return int(value[:-1])
    raise ValueError(f"Unsupported memory value: {memory}")


def _neo4j_memory_settings() -> dict[str, str]:
    heap_initial = os.getenv("NEO4J_HEAP_INITIAL")
    heap_max = os.getenv("NEO4J_HEAP_MAX")
    pagecache = os.getenv("NEO4J_PAGECACHE")
    if heap_initial and heap_max and pagecache:
        return {
            "heap_initial": heap_initial,
            "heap_max": heap_max,
            "pagecache": pagecache,
        }

    total_mib = _parse_memory_to_mib(get_neo4j_memory())
    if total_mib <= 2304:
        return {
            "heap_initial": "512m",
            "heap_max": "512m",
            "pagecache": "256m",
        }

    if total_mib <= 5120:
        # 4Gi Pod: heap 2Gi + pagecache 1Gi + ~1Gi native/JVM headroom.
        return {
            "heap_initial": "2048m",
            "heap_max": "2048m",
            "pagecache": "1024m",
        }

    overhead_mib = 1024
    usable = max(512, total_mib - overhead_mib)
    heap_mib = min(4096, max(2048, int(usable * 0.65)))
    pagecache_mib = min(2048, max(1024, usable - heap_mib))
    return {
        "heap_initial": f"{heap_mib}m",
        "heap_max": f"{heap_mib}m",
        "pagecache": f"{pagecache_mib}m",
    }


def _plugins_include_apoc() -> bool:
    return any("apoc" in plugin.lower() for plugin in _parse_neo4j_plugins())


def _neo4j_container_env(credentials: dict) -> list[client.V1EnvVar]:
    memory = _neo4j_memory_settings()
    env = [
        client.V1EnvVar(
            name="NEO4J_AUTH",
            value=f"{credentials['username']}/{credentials['password']}",
        ),
        client.V1EnvVar(
            name="NEO4J_server_http_listen__address",
            value="0.0.0.0:7474",
        ),
        client.V1EnvVar(
            name="NEO4J_server_bolt_listen__address",
            value="0.0.0.0:7687",
        ),
        client.V1EnvVar(
            name="NEO4J_server_http_x__forward__enabled",
            value="true",
        ),
        client.V1EnvVar(
            name="NEO4J_server_directories_data",
            value="/data",
        ),
        client.V1EnvVar(
            name="NEO4J_server_directories_logs",
            value="/logs",
        ),
        client.V1EnvVar(
            name="NEO4J_server_memory_heap_initial__size",
            value=memory["heap_initial"],
        ),
        client.V1EnvVar(
            name="NEO4J_server_memory_heap_max__size",
            value=memory["heap_max"],
        ),
        client.V1EnvVar(
            name="NEO4J_server_memory_pagecache_size",
            value=memory["pagecache"],
        ),
    ]
    plugins_json = _neo4j_plugins_json()
    if plugins_json:
        env.append(client.V1EnvVar(name="NEO4J_PLUGINS", value=plugins_json))
    if _plugins_include_apoc():
        env.extend(
            [
                client.V1EnvVar(name="NEO4J_apoc_export_file_enabled", value="true"),
                client.V1EnvVar(name="NEO4J_apoc_import_file_enabled", value="true"),
                client.V1EnvVar(
                    name="NEO4J_apoc_import_file_use__neo4j__config",
                    value="true",
                ),
            ]
        )
    return env


def create_deployment_spec_for_neo4j() -> client.V1Deployment:
    neo4j_memory = get_neo4j_memory()
    namespace = get_current_namespace()
    credentials = get_neo4j_credentials()

    data_volume = client.V1Volume(name="neo4j-data")
    if NEO4J_USE_PVC:
        data_volume.persistent_volume_claim = (
            client.V1PersistentVolumeClaimVolumeSource(
                claim_name=get_pvc_name_from_parent_pod()
            )
        )
    else:
        data_volume.empty_dir = client.V1EmptyDirVolumeSource()

    data_mount = client.V1VolumeMount(name="neo4j-data", mount_path="/data")
    if NEO4J_USE_PVC:
        data_mount.sub_path = "neo4j-volume"
    logs_mount = client.V1VolumeMount(name="neo4j-logs", mount_path="/logs")

    pod_spec = client.V1PodSpec(
        security_context=client.V1PodSecurityContext(
            fs_group=NEO4J_CONTAINER_GID,
            fs_group_change_policy="Always",
        ),
        containers=[
            client.V1Container(
                name="neo4j",
                image=NEO4J_IMAGE,
                image_pull_policy="IfNotPresent",
                security_context=client.V1SecurityContext(
                    allow_privilege_escalation=False,
                    run_as_non_root=True,
                    run_as_user=NEO4J_CONTAINER_UID,
                    run_as_group=NEO4J_CONTAINER_GID,
                ),
                ports=[
                    client.V1ContainerPort(
                        container_port=7687, name="bolt"
                    ),
                    client.V1ContainerPort(
                        container_port=7474, name="http"
                    ),
                ],
                env=_neo4j_container_env(credentials),
                resources=client.V1ResourceRequirements(
                    requests={"cpu": "500m", "memory": neo4j_memory},
                    limits={"cpu": "2", "memory": neo4j_memory},
                ),
                startup_probe=client.V1Probe(
                    tcp_socket=client.V1TCPSocketAction(port=7687),
                    period_seconds=10,
                    failure_threshold=60,
                ),
                readiness_probe=client.V1Probe(
                    tcp_socket=client.V1TCPSocketAction(port=7687),
                    period_seconds=10,
                    failure_threshold=20,
                ),
                volume_mounts=[data_mount, logs_mount],
            )
        ],
        volumes=[
            data_volume,
            client.V1Volume(
                name="neo4j-logs",
                empty_dir=client.V1EmptyDirVolumeSource(),
            ),
        ],
    )

    return client.V1Deployment(
        api_version="apps/v1",
        metadata=client.V1ObjectMeta(
            name=get_deployment_name(),
            labels={"app": get_deployment_name()},
            namespace=namespace,
        ),
        spec=client.V1DeploymentSpec(
            replicas=1,
            progress_deadline_seconds=600,
            selector=client.V1LabelSelector(match_labels={"app": get_deployment_name()}),
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(
                    labels={"app": get_deployment_name()},
                    annotations={
                        "sidecar.istio.io/inject": "false",
                    },
                ),
                spec=pod_spec,
            ),
        ),
    )


def _deployment_uses_pvc(deployment: client.V1Deployment) -> bool:
    for volume in deployment.spec.template.spec.volumes or []:
        if volume.name == "neo4j-data":
            return volume.persistent_volume_claim is not None
    return False


def _deployment_plugins_env(deployment: client.V1Deployment) -> str | None:
    container = deployment.spec.template.spec.containers[0]
    env_by_name = {env.name: env.value for env in container.env or []}
    return env_by_name.get("NEO4J_PLUGINS")


def _deployment_config_matches() -> bool:
    deployment = _get_deployment()
    if deployment is None:
        return False

    container = deployment.spec.template.spec.containers[0]
    expected = _neo4j_memory_settings()
    env_by_name = {env.name: env.value for env in container.env or []}
    limits = container.resources.limits or {}
    return (
        limits.get("memory") == get_neo4j_memory()
        and env_by_name.get("NEO4J_server_memory_heap_max__size")
        == expected["heap_max"]
        and env_by_name.get("NEO4J_server_memory_pagecache_size")
        == expected["pagecache"]
        and _deployment_uses_pvc(deployment) == NEO4J_USE_PVC
        and _deployment_plugins_env(deployment) == _neo4j_plugins_json()
    )


def _create_deployment_if_missing() -> None:
    api_instance = client.AppsV1Api()
    namespace = get_current_namespace()
    if _get_deployment() is not None:
        print(f"Using existing deployment {get_deployment_name()}.")
        return

    _create_with_conflict_retry(
        lambda: api_instance.create_namespaced_deployment(
            namespace=namespace,
            body=create_deployment_spec_for_neo4j(),
        ),
        _get_deployment,
        lambda: api_instance.delete_namespaced_deployment(
            name=get_deployment_name(),
            namespace=namespace,
            body=client.V1DeleteOptions(propagation_policy="Foreground"),
        ),
        get_deployment_name(),
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
            namespace=namespace,
            annotations=annotations or None,
        ),
        spec=client.V1ServiceSpec(
            type=NEO4J_SERVICE_TYPE,
            selector={"app": get_deployment_name()},
            ports=[bolt_port, http_port],
        ),
    )


def _set_supervisor_state(phase: str, error: str | None = None) -> None:
    _supervisor_state["phase"] = phase
    _supervisor_state["error"] = error
    _supervisor_state["updated_at"] = time.time()


def get_supervisor_state() -> dict:
    return dict(_supervisor_state)


def service_exists() -> bool:
    try:
        client.CoreV1Api().read_namespaced_service(
            name=get_neo4j_service_name(),
            namespace=get_current_namespace(),
        )
        return True
    except Exception:
        return False


def _format_k8s_error(exc: Exception) -> str:
    body = getattr(exc, "body", None)
    if body:
        return f"{exc} | body={body}"
    return str(exc)


def _get_deployment() -> client.V1Deployment | None:
    try:
        return client.AppsV1Api().read_namespaced_deployment(
            name=get_deployment_name(),
            namespace=get_current_namespace(),
        )
    except ApiException as exc:
        if exc.status == 404:
            return None
        raise


def _get_service() -> client.V1Service | None:
    try:
        return client.CoreV1Api().read_namespaced_service(
            name=get_neo4j_service_name(),
            namespace=get_current_namespace(),
        )
    except ApiException as exc:
        if exc.status == 404:
            return None
        raise


def _delete_if_exists(delete_fn, resource_name: str) -> None:
    try:
        delete_fn()
        print(f"Deleted {resource_name}")
    except ApiException as exc:
        if exc.status != 404:
            print(f"Failed to delete {resource_name}: {_format_k8s_error(exc)}")
            raise


def _wait_until_gone(
    getter,
    resource_name: str,
    timeout_seconds: int = 180,
    sleep_seconds: int = 5,
) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            obj = getter()
            if obj is None:
                return
            deleting = bool(obj.metadata.deletion_timestamp)
            print(
                f"Waiting for {resource_name} to be removed "
                f"(deleting={deleting})..."
            )
        except ApiException as exc:
            if exc.status == 404:
                return
            raise
        time.sleep(sleep_seconds)
    raise RuntimeError(
        f"Timed out waiting for {resource_name} to be deleted after "
        f"{timeout_seconds}s"
    )


def _delete_all_neo4j_resources() -> None:
    namespace = get_current_namespace()
    apps_api = client.AppsV1Api()
    core_api = client.CoreV1Api()
    delete_options = client.V1DeleteOptions(propagation_policy="Foreground")

    _delete_if_exists(
        lambda: apps_api.delete_namespaced_deployment(
            name=get_deployment_name(),
            namespace=namespace,
            body=delete_options,
        ),
        get_deployment_name(),
    )
    _delete_if_exists(
        lambda: core_api.delete_namespaced_service(
            name=get_neo4j_service_name(),
            namespace=namespace,
        ),
        get_neo4j_service_name(),
    )


def _wait_until_all_neo4j_resources_gone() -> None:
    _wait_until_gone(_get_deployment, get_deployment_name())
    _wait_until_gone(_get_service, get_neo4j_service_name())


def _format_container_status(name: str, status: client.V1ContainerStatus) -> str:
    state = status.state
    if state.waiting:
        return (
            f"{name}: waiting reason={state.waiting.reason or ''} "
            f"message={state.waiting.message or ''}"
        )
    if state.terminated:
        return (
            f"{name}: terminated reason={state.terminated.reason or ''} "
            f"exit={state.terminated.exit_code} "
            f"message={state.terminated.message or ''}"
        )
    if state.running:
        return f"{name}: running"
    return f"{name}: unknown"


def _describe_pod(pod: client.V1Pod) -> str:
    parts = [f"{pod.metadata.name}: phase={pod.status.phase}"]
    for status in pod.status.init_container_statuses or []:
        parts.append(_format_container_status(status.name, status))
    for status in pod.status.container_statuses or []:
        parts.append(_format_container_status(status.name, status))
    return " | ".join(parts)


def _get_recent_deployment_events(limit: int = 8) -> str | None:
    core_api = client.CoreV1Api()
    namespace = get_current_namespace()
    try:
        events = core_api.list_namespaced_event(
            namespace=namespace,
            field_selector=f"involvedObject.name={get_deployment_name()}",
        )
        lines = []
        for event in sorted(
            events.items,
            key=lambda item: item.last_timestamp or item.event_time,
        )[-limit:]:
            lines.append(
                f"{event.type} {event.reason}: {event.message}"
            )
        return "\n".join(lines) if lines else None
    except ApiException as exc:
        if exc.status == 403:
            return None
        return f"error reading events: {_format_k8s_error(exc)}"
    except Exception as exc:
        return f"error reading events: {_format_k8s_error(exc)}"


def _get_neo4j_pod_logs(tail_lines: int = 200) -> str | None:
    core_api = client.CoreV1Api()
    try:
        pods = core_api.list_namespaced_pod(
            namespace=get_current_namespace(),
            label_selector=f"app={get_deployment_name()}",
        )
        if not pods.items:
            events = _get_recent_deployment_events()
            return events or "no pods found and no recent deployment events"
        pod = pods.items[0]
        log_parts: list[str] = [_describe_pod(pod)]
        for previous in (True, False):
            try:
                log = core_api.read_namespaced_pod_log(
                    name=pod.metadata.name,
                    namespace=get_current_namespace(),
                    tail_lines=tail_lines,
                    container="neo4j",
                    previous=previous,
                )
                if log:
                    suffix = " (previous)" if previous else ""
                    log_parts.append(f"=== neo4j{suffix} ===\n{log}")
            except ApiException:
                continue
        if len(log_parts) > 1:
            return "\n\n".join(log_parts)
        return _get_recent_deployment_events()
    except Exception as exc:
        return f"error reading logs: {_format_k8s_error(exc)}"


def _get_neo4j_pod_status_text() -> str:
    return get_deployment_diagnostics().get("neo4j_pod_status") or ""


def _pod_is_in_failure_state(
    pod_status: str | None = None,
    deploy_started_at: float | None = None,
) -> bool:
    status = pod_status if pod_status is not None else _get_neo4j_pod_status_text()
    if any(marker in status for marker in POD_FAILURE_MARKERS):
        return True
    if "no pods found" in status:
        if deploy_started_at is None:
            return True
        return time.time() - deploy_started_at > NO_POD_GRACE_SECONDS
    return False


def _deployment_should_be_reused(
    deployment: client.V1Deployment | None,
    service: client.V1Service | None,
    pod_status: str,
) -> bool:
    if deployment is None or service is None:
        return False
    if is_neo4j_server_up() or is_neo4j_http_up():
        return True

    failure_or_stuck = (
        "no pods found",
        "FailedScheduling",
        "CrashLoopBackOff",
        "ImagePullBackOff",
        "ErrImagePull",
        "OOMKilled",
        "Error",
        "CreateContainerConfigError",
        "InvalidImageName",
    )
    if any(marker in pod_status for marker in failure_or_stuck):
        return False

    starting_markers = ("ContainerCreating", "PodInitializing", "Running")
    return any(marker in pod_status for marker in starting_markers)


def _ensure_clean_slate() -> None:
    deployment = _get_deployment()
    service = _get_service()

    if deployment is None and service is None:
        return

    pod_status = get_deployment_diagnostics().get("neo4j_pod_status") or ""
    if _deployment_should_be_reused(deployment, service, pod_status):
        print(
            "Reusing existing Neo4j resources. "
            f"Pod status: {pod_status}"
        )
        return

    print(
        "Cleaning up Neo4j resources before redeploying "
        f"(deployment={'yes' if deployment else 'no'}, "
        f"service={'yes' if service else 'no'}, "
        f"pod_status={pod_status})..."
    )
    _delete_all_neo4j_resources()
    _wait_until_all_neo4j_resources_gone()


def _create_with_conflict_retry(
    create_fn,
    getter,
    delete_fn,
    resource_name: str,
) -> None:
    for attempt in range(36):
        try:
            create_fn()
            return
        except ApiException as exc:
            if exc.status != 409:
                raise RuntimeError(
                    f"Failed to create {resource_name}: {_format_k8s_error(exc)}"
                ) from exc

            existing = getter()
            if existing is not None and not existing.metadata.deletion_timestamp:
                print(f"{resource_name} already exists. Deleting before recreate...")
                delete_fn()

            print(
                f"{resource_name} create conflict (attempt {attempt + 1}/36). "
                "Waiting for deletion to finish..."
            )
            _wait_until_gone(getter, resource_name, timeout_seconds=60)
            time.sleep(2)
    raise RuntimeError(
        f"Failed to create {resource_name} after repeated 409 conflicts"
    )


def get_deployment_diagnostics() -> dict:
    namespace = get_current_namespace()
    diagnostics = {
        "namespace": namespace,
        "engine_id": get_engine_id(),
        "deployment_name": get_deployment_name(),
        "service_name": get_neo4j_service_name(),
        "parent_pod": None,
        "pvc_claim": None,
        "deployment_status": None,
        "service_status": None,
        "neo4j_pod_status": None,
    }

    try:
        diagnostics["parent_pod"] = get_parent_pod_name()
    except Exception as exc:
        diagnostics["parent_pod"] = f"error: {exc}"

    try:
        diagnostics["pvc_claim"] = get_pvc_name_from_parent_pod()
    except Exception as exc:
        diagnostics["pvc_claim"] = f"error: {exc}"

    apps_api = client.AppsV1Api()
    core_api = client.CoreV1Api()

    try:
        deployment = apps_api.read_namespaced_deployment(
            name=get_deployment_name(),
            namespace=namespace,
        )
        available = deployment.status.available_replicas or 0
        desired = deployment.spec.replicas or 0
        deleting = bool(deployment.metadata.deletion_timestamp)
        diagnostics["deployment_status"] = (
            f"{available}/{desired} ready "
            f"(deleting={deleting}, conditions={deployment.status.conditions})"
        )
    except ApiException as exc:
        if exc.status == 404:
            diagnostics["deployment_status"] = "not found"
        else:
            diagnostics["deployment_status"] = f"error: {_format_k8s_error(exc)}"
    except Exception as exc:
        diagnostics["deployment_status"] = f"error: {_format_k8s_error(exc)}"

    try:
        service = core_api.read_namespaced_service(
            name=get_neo4j_service_name(),
            namespace=namespace,
        )
        deleting = bool(service.metadata.deletion_timestamp)
        diagnostics["service_status"] = (
            f"type={service.spec.type}, cluster_ip={service.spec.cluster_ip}, "
            f"deleting={deleting}"
        )
    except ApiException as exc:
        if exc.status == 404:
            diagnostics["service_status"] = "not found"
        else:
            diagnostics["service_status"] = f"error: {_format_k8s_error(exc)}"
    except Exception as exc:
        diagnostics["service_status"] = f"error: {_format_k8s_error(exc)}"

    try:
        pods = core_api.list_namespaced_pod(
            namespace=namespace,
            label_selector=f"app={get_deployment_name()}",
        )
        if not pods.items:
            diagnostics["neo4j_pod_status"] = "no pods found"
        else:
            pod_lines = [_describe_pod(pod) for pod in pods.items]
            diagnostics["neo4j_pod_status"] = " | ".join(pod_lines)
    except Exception as exc:
        diagnostics["neo4j_pod_status"] = f"error: {_format_k8s_error(exc)}"

    diagnostics["neo4j_pod_logs"] = _get_neo4j_pod_logs()
    diagnostics["k8s_events"] = _get_recent_deployment_events()

    return diagnostics


def deploy_neo4j_server() -> None:
    if is_neo4j_server_up():
        print("Neo4j is already reachable. Skipping deploy.")
        return

    _ensure_clean_slate()

    service_api_instance = client.CoreV1Api()
    namespace = get_current_namespace()

    deployment = _get_deployment()
    if deployment is None:
        _create_deployment_if_missing()
    elif not _deployment_config_matches():
        print(
            "Neo4j deployment config is outdated. "
            "Recreating deployment with current memory settings..."
        )
        _delete_all_neo4j_resources()
        _wait_until_all_neo4j_resources_gone()
        _create_deployment_if_missing()
    else:
        print(f"Using existing deployment {get_deployment_name()}.")

    if _get_service() is None:
        _create_with_conflict_retry(
            lambda: service_api_instance.create_namespaced_service(
                namespace=namespace,
                body=create_service_spec_for_neo4j(),
            ),
            _get_service,
            lambda: service_api_instance.delete_namespaced_service(
                name=get_neo4j_service_name(),
                namespace=namespace,
            ),
            get_neo4j_service_name(),
        )


def stop_neo4j_server() -> None:
    _delete_all_neo4j_resources()
    _wait_until_all_neo4j_resources_gone()


def reset_neo4j_server() -> None:
    try:
        stop_neo4j_server()
    except Exception as exc:
        print(f"Failed to stop Neo4j server: {exc}")
    deploy_neo4j_server()


def is_neo4j_http_up() -> bool:
    return _first_reachable_http_url() is not None


def wait_for_neo4j_http(
    max_retries: int = 60,
    sleep_duration: int = 5,
) -> None:
    for attempt in range(max_retries):
        if is_neo4j_http_up():
            return
        print(
            f"Neo4j HTTP is not ready yet "
            f"({attempt + 1}/{max_retries})"
        )
        time.sleep(sleep_duration)
    raise RuntimeError("Neo4j HTTP endpoint is not reachable.")


def _all_http_url_candidates() -> list[str]:
    candidates = list(_internal_browser_url_candidates())
    core_api = client.CoreV1Api()
    try:
        endpoints = core_api.read_namespaced_endpoints(
            name=get_neo4j_service_name(),
            namespace=get_current_namespace(),
        )
        for subset in endpoints.subsets or []:
            for address in subset.addresses or []:
                if address.ip:
                    candidates.append(f"http://{address.ip}:7474")
    except ApiException:
        pass

    unique_candidates: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        if candidate not in seen:
            seen.add(candidate)
            unique_candidates.append(candidate)
    return unique_candidates


def _first_reachable_http_url() -> str | None:
    if not service_exists():
        return None
    for candidate in _all_http_url_candidates():
        try:
            urllib.request.urlopen(f"{candidate.rstrip('/')}/", timeout=5)
            return candidate
        except (urllib.error.URLError, TimeoutError, OSError):
            continue
    return None


def is_neo4j_server_up() -> bool:
    credentials = get_neo4j_credentials()
    try:
        with GraphDatabase.driver(
            credentials["uri"],
            auth=(credentials["username"], credentials["password"]),
            connection_timeout=5,
        ) as driver:
            driver.verify_connectivity()
            return True
    except Exception:
        return False


def wait_for_neo4j_server(
    max_retries: int | None = None,
    sleep_duration: int = 10,
    deploy_started_at: float | None = None,
) -> None:
    if max_retries is None:
        max_retries = max(1, NEO4J_STARTUP_TIMEOUT_SECONDS // sleep_duration)
    if deploy_started_at is None:
        deploy_started_at = time.time()

    credentials = get_neo4j_credentials()
    consecutive_pod_failures = 0
    with GraphDatabase.driver(
        credentials["uri"],
        auth=(credentials["username"], credentials["password"]),
        connection_timeout=5,
    ) as driver:
        for attempt in range(max_retries):
            pod_status = _get_neo4j_pod_status_text()
            if _pod_is_in_failure_state(pod_status, deploy_started_at):
                consecutive_pod_failures += 1
                logs = _get_neo4j_pod_logs(tail_lines=80)
                events = _get_recent_deployment_events()
                print(
                    "Neo4j pod failure detected "
                    f"({consecutive_pod_failures}): {pod_status}"
                )
                if logs:
                    print(f"Recent pod logs/events:\n{logs[-3000:]}")
                if events:
                    print(f"Recent deployment events:\n{events[-2000:]}")
                if consecutive_pod_failures >= 3:
                    raise RuntimeError(
                        "Neo4j pod failed to start. "
                        f"Pod status: {pod_status}"
                    )
                time.sleep(sleep_duration)
                continue
            else:
                consecutive_pod_failures = 0

            try:
                driver.verify_connectivity()
                return
            except Exception as exc:
                print(
                    f"Neo4j server is not ready yet "
                    f"({attempt + 1}/{max_retries}): {exc}"
                )
                if attempt % 6 == 5:
                    print(f"Current pod status: {pod_status}")
                time.sleep(sleep_duration)
    diagnostics = get_deployment_diagnostics()
    raise RuntimeError(
        "Neo4j server is not ready yet. Max retries exceeded. "
        f"Pod status: {diagnostics.get('neo4j_pod_status')}"
    )


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

    _patch_neo4j_advertised_addresses(
        http_address=http_address,
        bolt_address=bolt_address,
    )
    print(
        "Configured Neo4j advertised addresses for external access: "
        f"http={http_address}, bolt={bolt_address}"
    )


def configure_connectivity_addresses() -> None:
    proxy_http_address = _cml_proxy_http_advertised_address()
    if not proxy_http_address:
        configure_external_advertised_addresses()
        return

    deployment = client.AppsV1Api().read_namespaced_deployment(
        name=get_deployment_name(),
        namespace=get_current_namespace(),
    )
    container = deployment.spec.template.spec.containers[0]
    env_by_name = {env.name: env for env in container.env}
    current_http = env_by_name.get("NEO4J_server_http_advertised__address")
    if current_http and current_http.value == proxy_http_address:
        print("Neo4j HTTP advertised address already configured.")
        return

    _patch_neo4j_advertised_addresses(
        http_address=proxy_http_address,
        bolt_address=None,
    )
    print(
        "Configured Neo4j HTTP advertised address for CML application proxy: "
        f"http={proxy_http_address}"
    )


def _patch_neo4j_advertised_addresses(
    http_address: str | None,
    bolt_address: str | None,
) -> None:
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
        env_by_name["NEO4J_server_http_x__forward__enabled"] = client.V1EnvVar(
            name="NEO4J_server_http_x__forward__enabled",
            value="true",
        )
    else:
        env_by_name.pop("NEO4J_server_http_advertised__address", None)

    if bolt_address:
        env_by_name["NEO4J_server_bolt_advertised__address"] = client.V1EnvVar(
            name="NEO4J_server_bolt_advertised__address",
            value=bolt_address,
        )
    else:
        env_by_name.pop("NEO4J_server_bolt_advertised__address", None)

    container.env = list(env_by_name.values())
    api_instance.patch_namespaced_deployment(
        name=get_deployment_name(),
        namespace=get_current_namespace(),
        body=deployment,
    )


def build_proxied_browser_path(_external_bolt: str | None = None) -> str:
    from urllib.parse import quote

    base_url = get_cml_application_base_url()
    if not base_url:
        return "/browser/"

    # Use HTTPS Query API via the CML application proxy instead of external Bolt.
    connect_url = f"{base_url}/"
    return f"/browser/?connectURL={quote(connect_url, safe='')}"


def _internal_browser_url_candidates() -> list[str]:
    service_name = get_neo4j_service_name()
    namespace = get_current_namespace()
    return [
        f"http://{service_name}:7474",
        f"http://{service_name}.{namespace}:7474",
        f"http://{service_name}.{namespace}.svc.cluster.local:7474",
    ]


def _can_resolve_host(host: str) -> bool:
    try:
        socket.getaddrinfo(host, 7474, type=socket.SOCK_STREAM)
        return True
    except OSError:
        return False


def get_internal_browser_url() -> str | None:
    return _first_reachable_http_url()


def get_proxy_unavailable_reason() -> str:
    diagnostics = get_deployment_diagnostics()
    supervisor = get_supervisor_state()
    parts: list[str] = []
    if supervisor.get("error"):
        parts.append(f"Supervisor error: {supervisor['error']}")
    if diagnostics.get("deployment_status"):
        parts.append(f"Deployment: {diagnostics['deployment_status']}")
    if diagnostics.get("service_status"):
        parts.append(f"Service: {diagnostics['service_status']}")
    if diagnostics.get("neo4j_pod_status"):
        parts.append(f"Pod: {diagnostics['neo4j_pod_status']}")
    logs = diagnostics.get("neo4j_pod_logs")
    if logs:
        tail = logs.strip().splitlines()[-3:]
        parts.append("Recent logs: " + " | ".join(line.strip() for line in tail))
    return " ".join(parts) if parts else "Neo4j HTTP endpoint is not reachable yet."


def _maybe_print_pod_logs(logs: str | None, reason: str) -> None:
    global _pod_logs_last_printed_at
    if not logs:
        return
    now = time.time()
    if now - _pod_logs_last_printed_at < POD_LOGS_PRINT_INTERVAL_SECONDS:
        return
    _pod_logs_last_printed_at = now
    print(f"=== Neo4j Pod Logs ({reason}) ===\n{logs[-8000:]}")


def get_connection_info() -> dict:
    credentials = get_neo4j_credentials()
    supervisor = get_supervisor_state()
    diagnostics = get_deployment_diagnostics()
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
        "proxied_browser_path": None,
        "http_api_connect_url": None,
        "supervisor_phase": supervisor.get("phase"),
        "deployment_status": diagnostics.get("deployment_status"),
        "service_status": diagnostics.get("service_status"),
        "neo4j_pod_status": diagnostics.get("neo4j_pod_status"),
        "neo4j_pod_logs": diagnostics.get("neo4j_pod_logs"),
        "k8s_events": diagnostics.get("k8s_events"),
        "pvc_claim": diagnostics.get("pvc_claim"),
        "parent_pod": diagnostics.get("parent_pod"),
    }

    if supervisor.get("error"):
        info["status"] = "error"
        info["message"] = f"Deployment error: {supervisor['error']}"
        _maybe_print_pod_logs(info.get("neo4j_pod_logs"), "supervisor error")

    pod_status = info.get("neo4j_pod_status") or ""
    if _pod_is_in_failure_state(pod_status):
        _maybe_print_pod_logs(info.get("neo4j_pod_logs"), pod_status)

    try:
        endpoints = get_external_endpoints()
    except Exception as exc:
        if not info.get("message"):
            info["message"] = (
                "Waiting for Neo4j Kubernetes service to be created. "
                f"Details: {exc}"
            )
        return info

    info.update(
        {
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

    http_ready = is_neo4j_http_up()
    bolt_ready = is_neo4j_server_up()

    if not bolt_ready or not http_ready:
        if info["status"] == "starting" and _pod_is_in_failure_state(pod_status):
            info["status"] = "error"
        if not info.get("message"):
            if bolt_ready and not http_ready:
                info["message"] = (
                    "Neo4j Bolt is up but HTTP is not ready yet. "
                    "Browser proxy will work once HTTP responds on port 7474."
                )
            elif http_ready and not bolt_ready:
                info["message"] = (
                    "Neo4j HTTP is up but Bolt is not ready yet. "
                    "APOC/GDS plugin download can take several minutes on first start."
                )
            else:
                info["message"] = get_proxy_unavailable_reason()
        if http_ready:
            info["proxied_browser_path"] = build_proxied_browser_path()
            info["http_api_connect_url"] = get_cml_application_base_url()
            if info["http_api_connect_url"]:
                info["http_api_connect_url"] = f"{info['http_api_connect_url']}/"
        return info

    info["status"] = "running"
    info["message"] = None
    info["proxied_browser_path"] = build_proxied_browser_path()
    info["http_api_connect_url"] = get_cml_application_base_url()
    if info["http_api_connect_url"]:
        info["http_api_connect_url"] = f"{info['http_api_connect_url']}/"
    return info


def print_connection_info() -> None:
    info = get_connection_info()

    print("=== POD LOGS START ===")
    print(info.get("neo4j_pod_logs"))
    print("=== POD LOGS END ===")

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


def _bootstrap_neo4j() -> None:
    _set_supervisor_state("deploying")
    memory = _neo4j_memory_settings()
    print("Running deployment preflight checks...")
    print(f"  namespace={get_current_namespace()}")
    print(f"  parent_pod={get_parent_pod_name()}")
    print(f"  pvc_claim={get_pvc_name_from_parent_pod()}")
    print(f"  deployment={get_deployment_name()}")
    print(f"  service={get_neo4j_service_name()}")
    print(f"  neo4j_memory={get_neo4j_memory()} (raw={os.getenv('NEO4J_MEMORY')!r})")
    print(f"  neo4j_plugins={get_neo4j_plugins_display()}")
    print(
        "  neo4j_data_volume="
        f"{'pvc:' + get_pvc_name_from_parent_pod() if NEO4J_USE_PVC else 'emptyDir (ephemeral, no PVC)'}"
    )
    print(f"  neo4j_use_pvc={NEO4J_USE_PVC} (raw={os.getenv('NEO4J_USE_PVC')!r})")
    print(
        "  neo4j_heap/pagecache="
        f"{memory['heap_max']}/{memory['pagecache']}"
    )

    if is_neo4j_server_up():
        print("Neo4j server is already running.")
        return

    deploy_started_at = time.time()
    deploy_neo4j_server()

    _set_supervisor_state("waiting_for_neo4j")
    try:
        wait_for_neo4j_server(deploy_started_at=deploy_started_at)
    except RuntimeError as exc:
        if is_neo4j_http_up():
            print(
                "Bolt is not ready yet, but Neo4j HTTP is responding. "
                f"Continuing startup: {exc}"
            )
        else:
            raise

    wait_for_external_endpoint()
    try:
        configure_connectivity_addresses()
        if not is_neo4j_server_up():
            wait_for_neo4j_server(max_retries=30)
        try:
            wait_for_neo4j_http(max_retries=60)
        except RuntimeError as exc:
            print(
                f"Warning: {exc} Browser proxy will become available once "
                "Neo4j HTTP responds on port 7474."
            )
    except Exception as exc:
        print(f"Failed to finalize Neo4j connectivity: {exc}")
    print_connection_info()


def run_neo4j_supervisor() -> None:
    print("Starting Neo4j server...")
    try:
        _set_supervisor_state("initializing")
        _bootstrap_neo4j()
        _set_supervisor_state("running")
        print("Neo4j is running. Monitoring in background.")
        while True:
            if not is_neo4j_server_up():
                if is_neo4j_http_up():
                    time.sleep(30)
                    continue
                print("Neo4j server is down. Attempting to restart...")
                _set_supervisor_state("restarting")
                reset_neo4j_server()
                wait_for_neo4j_server()
                print_connection_info()
                _set_supervisor_state("running")
            time.sleep(30)
    except Exception as exc:
        logs = _get_neo4j_pod_logs(tail_lines=200)
        print(f"Neo4j supervisor error: {exc}")
        if logs:
            print(f"Latest Neo4j pod logs:\n{logs[-5000:]}")
        _set_supervisor_state("error", str(exc))
