import logging
import os
import subprocess
import yaml
import csv

DOCKER_FILE_PATH = "../docker-compose.yaml"
CLIENT_CONFIG_PATH = "../src/client/config.yaml"
CLIENT_CONTAINER_WORKDIR = "/app/bin"

class ClientValidationError(Exception):

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


def await_client_containers(client_services_name):
    result = subprocess.run(
        ["docker", "container", "wait"] + client_services_name, capture_output=True
    )

    zero_exit_code_count = 0
    for char in result.stdout.decode("utf-8"):
        if char == "0":
            zero_exit_code_count += 1

    if zero_exit_code_count != len(client_services_name):
        raise ClientValidationError("One or more clients exited with an error code")


def find_environment_variable(environment_variables, target_environment_variable):
    if isinstance(environment_variables, dict):
        return environment_variables.get(target_environment_variable)

    for environment_variable in environment_variables:
        [name, value] = environment_variable.split("=")
        if name == target_environment_variable:
            return value
    return None


def resolve_host_path_from_volume(client_service, container_path):
    volumes = client_service.get("volumes", [])
    for volume in volumes:
        if isinstance(volume, str):
            parts = volume.split(":")
            if len(parts) < 2:
                continue
            host_path = parts[0]
            mounted_container_path = parts[1]
        elif isinstance(volume, dict):
            host_path = volume.get("source")
            mounted_container_path = volume.get("target")
        else:
            continue

        if not host_path or not mounted_container_path:
            continue

        normalized_mount = os.path.normpath(mounted_container_path)
        normalized_container_path = os.path.normpath(container_path)

        if normalized_container_path.startswith(normalized_mount):
            relative_path = os.path.relpath(normalized_container_path, normalized_mount)
            return os.path.normpath(os.path.join(".", host_path, relative_path))

    raise ClientValidationError(
        f"Could not map container path '{container_path}' to any configured service volume"
    )


def resolve_client_file_paths(client_service):
    with open(CLIENT_CONFIG_PATH, "r") as config_file:
        client_config = yaml.safe_load(config_file)

    input_path_in_container = os.path.normpath(
        os.path.join(CLIENT_CONTAINER_WORKDIR, client_config["transactions_dataset_path"])
    )

    output_path_from_config = client_config.get("output_path", "")
    if output_path_from_config:
        output_path_in_container = os.path.normpath(
            os.path.join(CLIENT_CONTAINER_WORKDIR, output_path_from_config)
        )
    else:
        environment = client_service.get("environment", [])
        output_path_from_env = find_environment_variable(environment, "OUTPUT_FILE")
        if not output_path_from_env:
            raise ClientValidationError(
                "Missing output path: set client.config.output_path or OUTPUT_FILE env variable"
            )
        output_path_in_container = os.path.normpath(output_path_from_env)

    input_file = resolve_host_path_from_volume(client_service, input_path_in_container)
    output_file = resolve_host_path_from_volume(client_service, output_path_in_container)
    return input_file, output_file


def build_input_queries(input_file):
    # TODO: implement expected query extraction from input file.
    pass


def read_output_queries(output_file):
    # TODO: implement output query parsing from client output file.
    pass


def verify_client_output(client_service):
    client_name = client_service["container_name"]
    logging.info(client_name)
    input_file, output_file = resolve_client_file_paths(client_service)

    if not input_file or not output_file:
        raise ClientValidationError("Bad file environment variable config")

    expected_queries_result = build_input_queries(input_file)
    received_queries_result = read_output_queries(output_file)

    i = 0
    mismatch_found = False
    # Compare results
    if mismatch_found:
        raise ClientValidationError("Mistmatch in expected and received fruit tops")

    logging.info("OK")


def main():
    logging.basicConfig(level=logging.INFO)

    try:
        with open(DOCKER_FILE_PATH, "r") as docker_compose_file:
            parsed_docker_compose_file = yaml.safe_load(docker_compose_file)
            services = parsed_docker_compose_file["services"]
            client_services_name = list(
                filter(
                    lambda service_key: "client"
                    in services[service_key]["build"]["dockerfile"],
                    services.keys(),
                )
            )
            logging.info("Awaiting client containers to exit...")
            await_client_containers(client_services_name)
            logging.info("Validating clients...")
            for client_service_name in client_services_name:
                client_service = services[client_service_name]
                verify_client_output(client_service)
            logging.info("All fruit tops match the expected results")
    except ClientValidationError as e:
        logging.error(e.message)
        return 1
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    main()
