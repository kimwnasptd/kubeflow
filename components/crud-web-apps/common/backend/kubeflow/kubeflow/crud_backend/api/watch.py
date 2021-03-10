import json
import logging

from flask import stream_with_context
from kubernetes import client, watch

from .. import settings

api = client.ApiClient()
log = logging.getLogger(__name__)


def serialize(obj):
    """Convert a k8s api object to serializable dict"""
    return json.dumps(api.sanitize_for_serialization(obj))


@stream_with_context
def list_sse(event_name, list_func):
    """
    Perform a WATCH on a list of objects and emit them via Server-Side-Events.
    The initial list is retrieved and send in chunks of APP_WATCH_CHUNK_LIMIT
    size.
    """
    log.info("New sse connection")
    w = watch.Watch()

    obj_list = list_func(limit=settings.CHUNK_LIMIT)
    rv = obj_list.metadata.resource_version
    json_list = serialize(obj_list.items)

    yield "event: %s\n" % event_name
    yield "data: {\"listInit\": %s}\n\n" % json_list

    # keep fetching chunks of the entire list
    _continue = obj_list.metadata._continue
    while _continue is not None:
        obj_list = list_func(_continue=_continue, limit=settings.CHUNK_LIMIT)
        log.info(obj_list.to_dict())
        json_list = serialize(obj_list.items)

        yield "event: %s\n" % event_name
        yield "data: {\"listPage\": %s}\n\n" % json_list

        _continue = obj_list.metadata._continue

    # start a persistent connection with the k8s api server and
    # emit sse event to the frontend for every event of the stream
    for event in w.stream(list_func, resource_version=rv):
        log.info("Watch event: %s", event["type"])
        rv = event["object"].metadata.resource_version
        event_json = serialize(event)

        yield "event: %s\n" % event_name
        yield "data: {\"event\": %s}\n\n" % event_json


@stream_with_context
def cr_list_sse(list_func, *args, **kwargs):
    """
    Perform a WATCH on a list of CRs and emit them via Server-Side-Events.
    The initial list is retrieved and send in chunks of APP_CHUNK_LIMIT size.
    """
    log.info("New sse connection")
    w = watch.Watch()

    obj_list = list_func(*args, **kwargs, limit=settings.CHUNK_LIMIT)
    rv = obj_list["metadata"]["resourceVersion"]
    json_list = serialize(obj_list["items"])
    log.info("Found initial list of objects: %s", json_list)

    yield "event: list\n"
    yield "data: %s\n\n" % json_list

    # keep fetching chunks of the entire list
    _continue = obj_list["metadata"]["continue"]
    while _continue != "":
        log.info("Fetching another %s CR objects.", settings.CHUNK_LIMIT)
        obj_list = list_func(*args, **kwargs, _continue=_continue,
                             limit=settings.CHUNK_LIMIT)
        json_list = serialize(obj_list["items"])

        yield "event: page\n"
        yield "data: %s\n\n" % json_list

        _continue = obj_list["metadata"]["continue"]

    # start a persistent connection with the k8s api server and
    # emit sse event to the frontend for every WatchEvent of the stream
    for event in w.stream(list_func, *args, **kwargs, resource_version=rv):
        log.info("Watch event: %s", event["type"])
        rv = event["object"]["metadata"]["resourceVersion"]

        yield "event: update\n"
        yield "data: %s\n\n" % serialize(event)
