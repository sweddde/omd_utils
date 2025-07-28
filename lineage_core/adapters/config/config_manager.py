import json
import logging
from abc import (
    ABC,
    abstractmethod,
)
from datetime import datetime
from typing import Any

from airflow.exceptions import AirflowException
from airflow.models import Variable

from omd_airflow_utils.lineage_core.domain.models import Settings

logger = logging.getLogger(__name__)


class ConfigStorage(ABC):
    """Abstract interface for configuration storage."""

    @abstractmethod
    def get(self, key: str, default: Any = None) -> str:
        raise NotImplementedError

    @abstractmethod
    def set(self, key: str, value: str) -> None:
        raise NotImplementedError


class AirflowStorage(ConfigStorage):
    """Airflow Variable-based configuration storage."""

    def get(self, key: str, default: Any = None) -> str:
        return Variable.get(key, default_var=default)

    def set(self, key: str, value: str) -> None:
        Variable.set(key, value)


class ConfigManager:
    """Manages application configuration with persistence."""

    DEFAULT_VARIABLE_NAME = 'lineage_config'

    def __init__(self, variable_name: str = None, storage: ConfigStorage = None):
        self.variable_name = variable_name or self.DEFAULT_VARIABLE_NAME
        self.storage = storage or AirflowStorage()

    def load_settings(self) -> Settings:
        """Loads and parses configuration settings."""
        try:
            config_json = self.storage.get(self.variable_name, default=None)
            config_data = json.loads(config_json) if config_json else {}
            return Settings(**config_data)
        except json.JSONDecodeError as e:
            raise AirflowException(f'Invalid JSON in {self.variable_name}: {e}')
        except Exception as e:
            logger.error('Error loading config: %s', e)
            raise

    def update_last_executed(self, timestamp: datetime) -> None:
        """Updates the last execution timestamp in configuration."""
        try:
            settings = self.load_settings()
            settings.last_executed = timestamp
            self._save_settings(settings)
        except Exception as e:
            logger.error('Failed to update last_executed: %s', e)
            raise

    def update_config_flag(self, flag_name: str, value: Any) -> None:
        """Updates a specific configuration flag."""
        try:
            settings = self.load_settings()
            if not hasattr(settings, flag_name):
                logger.warning('Config flag "%s" does not exist', flag_name)
                return
            setattr(settings, flag_name, value)
            self._save_settings(settings)
        except Exception as e:
            logger.error('Failed to update config flag "%s": %s', flag_name, e)
            raise

    def _save_settings(self, settings: Settings) -> None:
        """Saves settings to storage."""
        config_json = settings.model_dump_json(indent=2)
        self.storage.set(self.variable_name, config_json)