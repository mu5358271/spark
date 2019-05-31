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

FROM alpine:3.9

RUN apk add --no-cache openssh bash tini shadow && \
    sed -i 's/#\?ChallengeResponse.*/ChallengeResponseAuthentication\ no/' /etc/ssh/sshd_config && \
    sed -i 's/#\?PasswordAuthentication.*/PasswordAuthentication\ no/' /etc/ssh/sshd_config && \
    sed -i 's/#\?PermitRootLogin.*/PermitRootLogin\ no/' /etc/ssh/sshd_config && \
    sed -i 's/#\?RSAAuthentication.*/RSAAuthentication\ yes/' /etc/ssh/sshd_config && \
    sed -i 's/#\?AllowAgentForwarding.*/AllowAgentForwarding\ yes/' /etc/ssh/sshd_config && \
    sed -i 's/#\?AllowTcpForwarding.*/AllowTcpForwarding\ yes/' /etc/ssh/sshd_config && \
    sed -i 's/#\?PubkeyAuthentication.*/PubkeyAuthentication\ yes/' /etc/ssh/sshd_config && \
    addgroup fargate && \
    adduser -s /bin/bash -D -G fargate fargate && \
    usermod -p '*' fargate

COPY fargate/dockerfiles/spark/bindings/bastion/entrypoint.sh /opt/

ENTRYPOINT [ "/opt/entrypoint.sh" ]
