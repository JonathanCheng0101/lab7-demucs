#!/usr/bin/env python3

import requests
import json
import jsonpickle
import os
import sys
import base64
import glob

#
# REST endpoint for requests from your local shell.
# Usually this is localhost:5000 when using:
# kubectl port-forward svc/rest 5000:5000
#
REST = os.getenv("REST") or "localhost:5000"

#
# Callback URL for the worker running inside Kubernetes.
# The worker must call the REST service by service name, not localhost.
#
CALLBACK_URL = os.getenv("CALLBACK_URL") or "http://rest:5000"


def mkReq(reqmethod, endpoint, data, verbose=True):
    print(f"Response to http://{REST}/{endpoint} request is {type(data)}")
    jsonData = jsonpickle.encode(data)

    if verbose and data is not None:
        print(f"Make request http://{REST}/{endpoint} with json {data.keys()}")
        if "mp3" in data:
            print(f"mp3 is of type {type(data['mp3'])} and length {len(data['mp3'])}")

    response = reqmethod(
        f"http://{REST}/{endpoint}",
        data=jsonData,
        headers={"Content-type": "application/json"},
    )

    if response.status_code == 200:
        jsonResponse = json.dumps(response.json(), indent=4, sort_keys=True)
        print(jsonResponse)
        return response.json()
    else:
        print(f"response code is {response.status_code}, raw response is {response.text}")
        return response.text


for mp3 in glob.glob("data/*.mp3"):
    print(f"Separate {mp3}")
    mkReq(
        requests.post,
        "apiv1/separate",
        data={
            "mp3": base64.b64encode(open(mp3, "rb").read()).decode("utf-8"),
            "callback": {
                "url": CALLBACK_URL,
                "data": {
                    "mp3": mp3,
                    "data": "to be returned"
                }
            }
        },
        verbose=True
    )

    print("Cache from server is")
    mkReq(requests.get, "apiv1/queue", data=None, verbose=False)

sys.exit(0)
