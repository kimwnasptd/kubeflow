from flask import Response
from kubernetes import client

from .. import authz
from . import custom_api, watch


def create_custom_rsrc(group, version, kind, data, namespace):
    authz.ensure_authorized("create", group, version, kind, namespace)
    return custom_api.create_namespaced_custom_object(group, version,
                                                      namespace, kind, data)


def delete_custom_rsrc(group, version, kind, name, namespace, foreground=True):
    del_policy = client.V1DeleteOptions()
    if foreground:
        del_policy = client.V1DeleteOptions(propagation_policy="Foreground")

    authz.ensure_authorized("delete", group, version, kind, namespace)
    return custom_api.delete_namespaced_custom_object(group, version,
                                                      namespace, kind, name,
                                                      del_policy)


def list_custom_rsrc(group, version, kind, namespace):
    authz.ensure_authorized("list", group, version, kind, namespace)
    return custom_api.list_namespaced_custom_object(group, version, namespace,
                                                    kind)


def get_custom_rsrc(group, version, kind, namespace, name):
    authz.ensure_authorized("get", group, version, kind, namespace)

    return custom_api.get_namespaced_custom_object(group, version, namespace,
                                                   kind, name)


def watch_custom_rsrc(group, version, kind, namespace):
    authz.ensure_authorized("list", group, version, kind, namespace)

    list_fn = custom_api.list_namespaced_custom_object
    sse_stream = watch.cr_list_sse(list_fn, group, version, namespace, kind)

    return Response(sse_stream, mimetype="text/event-stream")
