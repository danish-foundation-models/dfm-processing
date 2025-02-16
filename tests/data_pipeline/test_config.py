import pytest
from pydantic import ValidationError
from pathlib import Path
import yaml
from dfm_processing.data_pipeline.config import (
    Dataset,
    ExecutorConfig,
    ClusterConfig,
    PipelineConfig,
    load_yml_config,
)

# Sample valid data for each model
VALID_DATASET = {
    "name": "test",
    "input_dir": "input",
    "output_dir": "output",
    "exclusion_dir": "exclude",
    "logging_dir": "logs",
}

VALID_EXECUTOR = {"n_workers": 2, "n_tasks": 3}

VALID_CLUSTER = {"type": "distributed"}

VALID_PIPELINE = {
    "datasets": [VALID_DATASET],
    "executor": VALID_EXECUTOR,
    "sent_dedup": True,
    "dedup_dir": "dedup",
    "cluster": VALID_CLUSTER,
}


# Tests for Dataset model
def test_dataset_valid():
    dataset = Dataset(**VALID_DATASET, debug=True)
    assert dataset.name == "test"
    assert dataset.debug is True


@pytest.mark.parametrize(
    "missing_field", ["name", "input_dir", "output_dir", "exclusion_dir", "logging_dir"]
)
def test_dataset_missing_required_fields(missing_field):
    data = VALID_DATASET.copy()
    del data[missing_field]
    with pytest.raises(ValidationError):
        Dataset(**data)


def test_dataset_invalid_debug_type():
    data = VALID_DATASET.copy()
    data["debug"] = "not_a_boolean"
    with pytest.raises(ValidationError):
        Dataset(**data)


# Tests for ExecutorConfig model
def test_executor_defaults():
    executor = ExecutorConfig()
    assert executor.n_workers == 1
    assert executor.n_tasks == 1
    assert executor.debug is False


@pytest.mark.parametrize("field, value", [("n_workers", "two"), ("n_tasks", 3.14)])
def test_executor_invalid_types(field, value):
    data = VALID_EXECUTOR.copy()
    data[field] = value
    with pytest.raises(ValidationError):
        ExecutorConfig(**data)


# Tests for ClusterConfig model
def test_cluster_defaults():
    cluster = ClusterConfig()
    assert cluster.type == "local"
    assert cluster.n_workers == 5


@pytest.mark.parametrize("cluster_type", ["local", "distributed", None])
def test_cluster_valid_types(cluster_type):
    if cluster_type:
        cluster = ClusterConfig(type=cluster_type)
    else:
        cluster = ClusterConfig()
    assert cluster.type == (cluster_type or "local")


def test_cluster_invalid_type():
    with pytest.raises(ValidationError):
        ClusterConfig(type="invalid")


def test_cluster_port_type():
    with pytest.raises(ValidationError):
        ClusterConfig(scheduler_port="not_an_integer")


# Tests for PipelineConfig model
def test_pipeline_valid():
    pipeline = PipelineConfig(**VALID_PIPELINE)
    assert len(pipeline.datasets) == 1
    assert pipeline.executor.n_workers == 2
    assert pipeline.sent_dedup is True


def test_pipeline_missing_required_fields():
    required_fields = ["datasets", "executor", "dedup_dir", "cluster"]
    for field in required_fields:
        data = VALID_PIPELINE.copy()
        del data[field]
        with pytest.raises(ValidationError):
            PipelineConfig(**data)


def test_pipeline_invalid_nested_model():
    data = VALID_PIPELINE.copy()
    data["datasets"] = [{"name": "invalid_dataset"}]  # Missing other fields
    with pytest.raises(ValidationError):
        PipelineConfig(**data)


# Tests for YAML loading and integration
def test_load_yml_config_valid(tmp_path):
    config_file = tmp_path / "config.yml"
    config_file.write_text(yaml.dump(VALID_PIPELINE))
    data = load_yml_config(config_file)
    assert isinstance(data, dict)
    assert "datasets" in data


def test_load_yml_config_missing_file():
    with pytest.raises(FileNotFoundError):
        load_yml_config(Path("nonexistent.yml"))


def test_pipeline_from_yml(tmp_path):
    config_file = tmp_path / "config.yml"
    config_file.write_text(yaml.dump(VALID_PIPELINE))
    data = load_yml_config(config_file)
    pipeline = PipelineConfig(**data)
    assert pipeline.datasets[0].name == "test"


def test_invalid_yaml_syntax(tmp_path):
    config_file = tmp_path / "config.yml"
    config_file.write_text("invalid: yaml: here")
    with pytest.raises(yaml.YAMLError):
        load_yml_config(config_file)


# Test dedup_dir is required
def test_pipeline_missing_dedup_dir():
    data = VALID_PIPELINE.copy()
    del data["dedup_dir"]
    with pytest.raises(ValidationError):
        PipelineConfig(**data)


# Test sent_dedup default
def test_sent_dedup_default():
    data = VALID_PIPELINE.copy()
    del data["sent_dedup"]
    pipeline = PipelineConfig(**data)
    assert pipeline.sent_dedup is False
