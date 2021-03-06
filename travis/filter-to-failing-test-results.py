#
# Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use
# this file except in compliance with the License. You may obtain a copy of the
# License at  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied.
#

#
# Python script used to filter the list of junit XML test results
# to those that contain errors or failures.
#

#!/usr/bin/python

import sys
import fileinput
import xml.etree.ElementTree

for line in fileinput.input():
    suite = xml.etree.ElementTree.parse(line.rstrip()).getroot()
    errors = suite.get("errors")
    failures = suite.get("failures")
    if (errors is not None and int(errors) > 0) or (failures is not None and int(failures) > 0):
        sys.stdout.write(line)

sys.exit(0)