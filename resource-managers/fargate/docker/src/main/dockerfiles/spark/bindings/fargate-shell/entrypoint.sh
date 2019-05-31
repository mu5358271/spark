#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

# generate host key
ssh-keygen -A

mkdir -p /home/fargate/.ssh
chmod 700 /home/fargate/.ssh

echo "PATH=$SPARK_HOME/bin:$PATH" >> /home/fargate/.ssh/environment
echo "PATH=$SPARK_HOME/bin:$PATH" >> /home/fargate/.bash_profile

# inherit all AWS environment variables for the user
env | grep -E '^AWS_' >> /home/fargate/.ssh/environment
env | grep -E '^AWS_' >> /home/fargate/.bash_profile

# setup public key access
echo "$1" > /home/fargate/.ssh/authorized_keys
chmod 600 /home/fargate/.ssh/authorized_keys
shift 1

chown -R fargate:fargate /home/fargate

# do not detach (-D), log to stderr (-e), passthrough other arguments
exec /sbin/tini -s -- /usr/sbin/sshd -D -e "$@"
