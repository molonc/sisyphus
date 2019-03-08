#!/usr/bin/env bash

 python setup.py develop
 python docker/docker_populate_tantalus_db.py
 python docker/docker_test.py