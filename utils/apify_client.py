"""
Apify SDK-based wrapper utilities.

This wrapper uses the official `apify-client` Python SDK to start actor runs,
wait for completion, and fetch dataset items.

Usage:
    from utils.apify_client import get_client, run_actor_and_save
    result = run_actor_and_save("amazon", input_=...)

"""

from __future__ import annotations

import os
import json
from typing import Any, Dict, Iterable, Optional

from apify_client import ApifyClient

from config.settings import settings
from utils.logger import get_logger


class ApifySdkClient:
    def __init__(self, token: Optional[str] = None, actor_key: Optional[str] = None):
        token = token or settings.APIFY_API_KEY
        # Initialize the official ApifyClient
        self._client = ApifyClient(token)

        if actor_key:
            self.logger = get_logger(actor_key)
        else:
            self.logger = get_logger("main")

    def call_actor(
        self,
        actor_id: str,
        input_: Optional[Dict[str, Any]] = None,
        memory_mbytes: Optional[int] = None,
        build: Optional[str] = None,
        wait_for_finish: Optional[bool] = None,
        wait_for_finish_timeout: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Start (call) an actor and optionally wait for it to finish.

        Returns the run object returned by the SDK. The SDK `call` method
        waits until the run finishes and returns the run object by default.
        """
        if not actor_id:
            raise ValueError("actor_id is required")

        run_options: Dict[str, Any] = {}
        if input_ is not None:
            run_options["run_input"] = input_
        if memory_mbytes is not None:
            run_options["memory_mbytes"] = memory_mbytes
        if build is not None:
            run_options["build"] = build

        # The ApifyClient.actor(actor_id).call(...) will wait by default.
        # If the caller requested not to wait, use start() instead.
        if wait_for_finish is False:
            self.logger.info(
                "Starting actor (non-blocking)", extra={"actor_id": actor_id}
            )
            run = self._client.actor(actor_id).start(
                run_input=input_, memory_mbytes=memory_mbytes
            )
            return run

        timeout = wait_for_finish_timeout or settings.DEFAULT_WAIT_FOR_FINISH_TIMEOUT
        self.logger.info(
            "Calling actor (blocking)", extra={"actor_id": actor_id, "timeout": timeout}
        )
        # call() waits for completion and returns the run object
        run = self._client.actor(actor_id).call(
            run_input=input_,
            memory_mbytes=memory_mbytes,
            wait_secs=timeout,
            logger=self.logger,
        )
        self.logger.debug(
            "Actor call finished", extra={"actor_id": actor_id, "run": run}
        )
        return run  # type: ignore

    def download_dataset_to_file(self, dataset_id: str, output_path: str) -> None:
        """Download dataset as JSON array and save to a file.

        This will stream results to avoid loading everything into memory.
        """
        self.logger.info(
            "Downloading dataset", extra={"dataset_id": dataset_id, "path": output_path}
        )
        ds_client = self._client.dataset(dataset_id)
        # The SDK provides list_items, iterate_items and other helpers.
        items = list(ds_client.iterate_items())
        with open(output_path, "w", encoding="utf-8") as fh:
            json.dump(str(items), fh, ensure_ascii=False, indent=2)
        self.logger.info(
            "Saved dataset to file", extra={"path": output_path, "count": len(items)}
        )


# Singleton client
_client: Optional[ApifySdkClient] = None


def get_client(actor_key: Optional[str] = None) -> ApifySdkClient:
    global _client
    if _client is None:
        _client = ApifySdkClient(actor_key=actor_key)
    return _client


def run_actor_and_save(
    actor_key: str,
    input_: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Start actor by actor_key (one of 'amazon','etsy','shopify','ebay') as defined
    in config.ACTORS and optionally save the run object and dataset results to a file.

    Returns the run object. If the run produced a default dataset and `output_path`
    ends with `.json`, the dataset will be saved there.
    """
    from config.settings import settings

    actor_id = settings.ACTORS.get(actor_key)
    if not actor_id:
        raise ValueError(f"Actor id for key '{actor_key}' not configured")

    client = get_client(actor_key)
    logger = get_logger(actor_key)

    run = client.call_actor(
        actor_id=actor_id,
        input_=input_,
        memory_mbytes=settings.DEFAULT_MEMORY_MBYTES,
        wait_for_finish=settings.DEFAULT_WAIT_FOR_FINISH,
        wait_for_finish_timeout=settings.DEFAULT_WAIT_FOR_FINISH_TIMEOUT,
    )

    # Save run metadata
    output_path = os.path.join(settings.DATA_DIR, actor_key, f"{actor_key}.json")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    try:
        with open(output_path, "w", encoding="utf-8") as fh:
            json.dump(run, fh, ensure_ascii=False, indent=2)
        logger.info("Saved actor run metadata", extra={"path": output_path})
    except Exception:
        logger.exception(
            "Failed to save actor run metadata", extra={"path": output_path}
        )

    # If run contains a dataset id, download dataset
    dataset_id = run.get("defaultDatasetId") or (run.get("data") or {}).get(
        "defaultDatasetId"
    )
    if dataset_id and output_path and output_path.endswith(".json"):
        # Prefer saving dataset in a sibling file with suffix
        dataset_path = os.path.join(
            settings.DATA_DIR, actor_key, "raw", f"{actor_key}.dataset.json"
        )
        os.makedirs(os.path.dirname(dataset_path), exist_ok=True)
        try:
            client.download_dataset_to_file(dataset_id, dataset_path)
        except Exception:
            logger.exception(
                "Failed to download dataset",
                extra={"dataset_id": dataset_id, "path": dataset_path},
            )

    return run
