# Serverless Spark on Fargate

[AWS Fargate](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ECS_GetStarted.html) is a compute engine for [Amazon ECS](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/Welcome.html) that allows you to run containers without having to manage servers or clusters

This fork implements a cluster manager using AWS Fargate as the scheduler backend, and allows you to spin up a Spark cluster and run Spark applications in real time without the need to manage servers.

## Building Spark

### Building the distribution
    
    ./dev/make-distribution.sh -Phadoop-3.2 -Pfargate && cd dist

### Building the Docker images
 
From the top level directory of the Spark distribution
 
    docker build -t bastion -f fargate/dockerfiles/spark/bindings/bastion/Dockerfile .
    docker build -t spark-fargate -f fargate/dockerfiles/spark/bindings/fargate/Dockerfile .
    docker build -t spark-fargate-shell -f fargate/dockerfiles/spark/bindings/fargate-shell/Dockerfile .

To use these images, push the images to a registry accessible by Fargate containers, e.g. [AWS ECR](https://docs.aws.amazon.com/AmazonECR/latest/userguide/what-is-ecr.html)

## Running Spark on Fargate

### AWS Infrastructure Setup
You would need to have the [VPCs](https://docs.aws.amazon.com/vpc/latest/userguide/what-is-amazon-vpc.html), [Subnets](https://docs.aws.amazon.com/vpc/latest/userguide/VPC_Subnets.html), [Security Groups](https://docs.aws.amazon.com/vpc/latest/userguide/VPC_SecurityGroups.html), and [IAM Roles](https://docs.aws.amazon.com/IAM/latest/UserGuide/introduction.html) (more specifically ECS [Task Roles](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_IAM_role.html) and [Execution Roles](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_execution_IAM_role.html)) ready before proceeding to the next sections. Please consult AWS documentation on how to set these up. 

The [First Run Guide](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ECS_GetStarted.html) gives a taste on running a container based service on AWS Fargate, and would be a good place to start.

Here are a list of things you need  

    # bastion security groups allowing the bastion to communicate with the client machine and driver
    bastion_sgs=
    # driver security groups allowing the driver to communicate with the bastion and executors
    driver_sgs=
    # executor security groups allowing the executors to communicate with the driver and other executors
    executor_sgs=
    
    # should be in the public subnet
    bastion_subnets=
    # should be in the private subnet
    driver_subnets=
    # should be in the private subnet
    executor_subnets=
    
    # the role with permission to access container images, create and drive logs
    execution_role=
    # the role with no permission
    bastion_task_role=
    # the role with permission to call ecs services and perform other driver tasks
    driver_task_role=
    # the role with permission to perform executor tasks
    executor_task_role=
    
    # the bastion container image uri. this should be where the bastion image is pushed to
    bastion_image=
    # the driver container image uri. this should be where the spark-fargate-shell image is pushed to
    driver_image=
    # the executor container image uri. this should be where the spark-fargate image is pushed to
    executor_image=

Optionally, you may specify a KMS key for authentication secret sharing and enable network and io encryption

    # KMS key used to generate and encrypt authentication secret on the driver, and decrypt it on the executors 
    kms_key_id=
    
    --conf spark.authenticate=true \
    --conf spark.authenticate.kms.key=${kms_key_id} \
    --conf spark.network.crypto.enabled=true \
    --conf spark.io.encryption.enabled=true \

You can also try out the s3 backed shuffle client and dynamic allocation. WARNING: s3 backed shuffle is not optimized, fault tolerant or speculation safe. You are strong discouraged from depending on it.

    # The bucket to put shuffle files
    shuffle_s3_bucket=
    # The prefix to use for shuffle files, defaults to '.sparkStaging'. The shuffle files are stored under ${shuffle_s3_bucket}/${shuffle_s3_prefix}/${spark.app.id}/shuffle/
    shuffle_s3_prefix=
    
    --conf spark.shuffle.s3.bucket=${shuffle_s3_bucket} \
    --conf spark.shuffle.s3.enabled=true \
    --conf spark.shuffle.s3.prefix=${shuffle_s3_prefix} \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.initialExecutors=0 \
    --conf spark.dynamicAllocation.maxExecutors=8 \
    --conf spark.dynamicAllocation.minExecutors=0 \

### Cluster Mode

You should be able to specify jar dependencies in any HDFS compatible file system that is accessible within the VPC. 
e.g. s3a://my-bucket/my-jar.jar

    bin/spark-submit \
    --master fargate \
    --deploy-mode cluster \
    --conf spark.fargate.container.image=${executor_image} \
    --conf spark.fargate.driver.securityGroups=${driver_sgs} \
    --conf spark.fargate.executor.securityGroups=${executor_sgs} \
    --conf spark.fargate.driver.subnets=${driver_subnets} \
    --conf spark.fargate.executor.subnets=${executor_subnets} \
    --conf spark.driver.memory=8g \
    --conf spark.driver.cores=4 \
    --conf spark.executor.instances=2 \
    --conf spark.executor.memory=8g \
    --conf spark.executor.cores=4 \
    --conf spark.fargate.report.interval=5s \
    --conf spark.fargate.executionRole=${execution_role} \
    --conf spark.fargate.driver.taskRole=${driver_task_role} \
    --conf spark.fargate.executor.taskRole=${execution_role} \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
    --class org.apache.spark.examples.SparkPi \
    local:///opt/spark/examples/jars/spark-examples_2.12-3.0.0-SNAPSHOT.jar 20


### Client Mode/Interactive Scala Shell

If you encounter issues with SSH, you might need to add the necessary identities to you SSH agent

    ssh-add ~/.ssh/id_rsa

Fill in the security groups, subnets, and roles and enjoy your serverless spark shell

    timestamp=$(date +%s000)
    
    bastion_task_definition=$(aws ecs register-task-definition \
    --family bastion-${timestamp} \
    --network-mode awsvpc \
    --execution-role-arn ${execution_role} \
    --task-role-arn ${bastion_task_role} \
    --requires-compatibilities FARGATE \
    --cpu 1024 \
    --memory 2048 \
    --container-definitions "$(cat << EOF
    [
      {
        "name": "bastion",
        "image": "${bastion_image}",
        "command": [
          "$(cat ~/.ssh/id_rsa.pub | awk '{print $1" "$2" (redacted)"}')"
        ]
      }
    ]
    EOF
    )" \
    --output text \
    --query 'taskDefinition.taskDefinitionArn'
    )
    
    driver_task_definition=$(aws ecs register-task-definition \
    --family spark-fargate-shell-${timestamp} \
    --network-mode awsvpc \
    --execution-role-arn ${execution_role} \
    --task-role-arn ${driver_task_role} \
    --requires-compatibilities FARGATE \
    --cpu 4096 \
    --memory 9216 \
    --container-definitions "$(cat << EOF
    [
      {
        "name": "spark-fargate-shell",
        "image": "${driver_image}",
        "command": [
          "$(cat ~/.ssh/id_rsa.pub | awk '{print $1" "$2" (redacted)"}')"
        ]
      }
    ]
    EOF
    )" \
    --output text \
    --query 'taskDefinition.taskDefinitionArn'
    )
    
    bastion_task=$(aws ecs run-task \
    --task-definition bastion-${timestamp} \
    --count 1 \
    --launch-type FARGATE \
    --network-configuration "awsvpcConfiguration={subnets=[${bastion_subnets}],securityGroups=[${bastion_sgs}],assignPublicIp=ENABLED}" \
    --output text \
    --query 'tasks | [0].taskArn')
    
    driver_task=$(aws ecs run-task \
    --task-definition spark-fargate-shell-${timestamp} \
    --count 1 \
    --launch-type FARGATE \
    --network-configuration "awsvpcConfiguration={subnets=[${driver_subnets}],securityGroups=[${driver_sgs}],assignPublicIp=DISABLED}" \
    --output text \
    --query 'tasks | [0].taskArn')
    
    aws ecs wait tasks-running --tasks ${bastion_task},${driver_task}
    
    bastion_eni=$(aws ecs describe-tasks \
    --tasks ${bastion_task} \
    --output text \
    --query 'tasks | [0].attachments | [0].details[?name==`networkInterfaceId`] | [0].value'
    )
    
    bastion_ip=$(aws ec2 describe-network-interfaces \
    --network-interface-ids ${bastion_eni} \
    --output text \
    --query 'NetworkInterfaces | [0].Association.PublicIp'
    )
    
    driver_ip=$(aws ecs describe-tasks \
    --tasks ${driver_task} \
    --output text \
    --query 'tasks | [0].containers[?taskArn==`'${driver_task}'`] | [0].networkInterfaces | [0].privateIpv4Address')
    
    ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no \
    -o ProxyCommand="ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -W %h:%p fargate@${bastion_ip}" \
    fargate@${driver_ip} -t spark-shell --master fargate \
     --conf spark.fargate.container.image=${executor_image} \
     --conf spark.fargate.executor.securityGroups=${executor_sgs} \
     --conf spark.fargate.executor.subnets=${executor_subnets} \
     --conf spark.driver.memory=8g \
     --conf spark.driver.cores=4 \
     --conf spark.executor.instances=2 \
     --conf spark.executor.memory=8g \
     --conf spark.executor.cores=4 \
     --conf spark.fargate.executionRole=${execution_role} \
     --conf spark.fargate.executor.taskRole=${executor_task_role} \
     --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain

Cleaning Up
    
    aws ecs stop-task --task ${driver_task} > /dev/null
    aws ecs stop-task --task ${bastion_task} > /dev/null
    aws ecs deregister-task-definition --task-definition ${driver_task_definition} > /dev/null
    aws ecs deregister-task-definition --task-definition ${bastion_task_definition} > /dev/null

# Apache Spark

[![Jenkins Build](https://amplab.cs.berkeley.edu/jenkins/job/spark-master-test-sbt-hadoop-2.7/badge/icon)](https://amplab.cs.berkeley.edu/jenkins/job/spark-master-test-sbt-hadoop-2.7)
[![AppVeyor Build](https://img.shields.io/appveyor/ci/ApacheSoftwareFoundation/spark/master.svg?style=plastic&logo=appveyor)](https://ci.appveyor.com/project/ApacheSoftwareFoundation/spark)
[![PySpark Coverage](https://img.shields.io/badge/dynamic/xml.svg?label=pyspark%20coverage&url=https%3A%2F%2Fspark-test.github.io%2Fpyspark-coverage-site&query=%2Fhtml%2Fbody%2Fdiv%5B1%5D%2Fdiv%2Fh1%2Fspan&colorB=brightgreen&style=plastic)](https://spark-test.github.io/pyspark-coverage-site)

Spark is a fast and general cluster computing system for Big Data. It provides
high-level APIs in Scala, Java, Python, and R, and an optimized engine that
supports general computation graphs for data analysis. It also supports a
rich set of higher-level tools including Spark SQL for SQL and DataFrames,
MLlib for machine learning, GraphX for graph processing,
and Spark Streaming for stream processing.

<http://spark.apache.org/>


## Online Documentation

You can find the latest Spark documentation, including a programming
guide, on the [project web page](http://spark.apache.org/documentation.html).
This README file only contains basic setup instructions.

## Building Spark

Spark is built using [Apache Maven](http://maven.apache.org/).
To build Spark and its example programs, run:

    build/mvn -DskipTests clean package

(You do not need to do this if you downloaded a pre-built package.)

You can build Spark using more than one thread by using the -T option with Maven, see ["Parallel builds in Maven 3"](https://cwiki.apache.org/confluence/display/MAVEN/Parallel+builds+in+Maven+3).
More detailed documentation is available from the project site, at
["Building Spark"](http://spark.apache.org/docs/latest/building-spark.html).

For general development tips, including info on developing Spark using an IDE, see ["Useful Developer Tools"](http://spark.apache.org/developer-tools.html).

## Interactive Scala Shell

The easiest way to start using Spark is through the Scala shell:

    ./bin/spark-shell

Try the following command, which should return 1000:

    scala> sc.parallelize(1 to 1000).count()

## Interactive Python Shell

Alternatively, if you prefer Python, you can use the Python shell:

    ./bin/pyspark

And run the following command, which should also return 1000:

    >>> sc.parallelize(range(1000)).count()

## Example Programs

Spark also comes with several sample programs in the `examples` directory.
To run one of them, use `./bin/run-example <class> [params]`. For example:

    ./bin/run-example SparkPi

will run the Pi example locally.

You can set the MASTER environment variable when running examples to submit
examples to a cluster. This can be a mesos:// or spark:// URL,
"yarn" to run on YARN, and "local" to run
locally with one thread, or "local[N]" to run locally with N threads. You
can also use an abbreviated class name if the class is in the `examples`
package. For instance:

    MASTER=spark://host:7077 ./bin/run-example SparkPi

Many of the example programs print usage help if no params are given.

## Running Tests

Testing first requires [building Spark](#building-spark). Once Spark is built, tests
can be run using:

    ./dev/run-tests

Please see the guidance on how to
[run tests for a module, or individual tests](http://spark.apache.org/developer-tools.html#individual-tests).

There is also a Kubernetes integration test, see resource-managers/kubernetes/integration-tests/README.md

## A Note About Hadoop Versions

Spark uses the Hadoop core library to talk to HDFS and other Hadoop-supported
storage systems. Because the protocols have changed in different versions of
Hadoop, you must build Spark against the same version that your cluster runs.

Please refer to the build documentation at
["Specifying the Hadoop Version and Enabling YARN"](http://spark.apache.org/docs/latest/building-spark.html#specifying-the-hadoop-version-and-enabling-yarn)
for detailed guidance on building for a particular distribution of Hadoop, including
building for particular Hive and Hive Thriftserver distributions.

## Configuration

Please refer to the [Configuration Guide](http://spark.apache.org/docs/latest/configuration.html)
in the online documentation for an overview on how to configure Spark.

## Contributing

Please review the [Contribution to Spark guide](http://spark.apache.org/contributing.html)
for information on how to get started contributing to the project.
