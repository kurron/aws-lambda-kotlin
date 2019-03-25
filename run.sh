#!/usr/bin/env bash

# run the project, requiring only Docker to be installed. All other dependencies are contained in the container.

DOCKER_GROUP_ID=$(cut -d: -f3 < <(getent group docker))
USER_ID=$(id -u $(whoami))
GROUP_ID=$(id -g $(whoami))
HOME_DIR=$(cut -d: -f6 < <(getent passwd ${USER_ID}))

CMD="docker run --hostname aws-lambda-kotlin \
                --group-add ${DOCKER_GROUP_ID} \
                --env HOME=${HOME_DIR} \
                --interactive \
                --net host \
                --name aws-lambda-kotlin \
                --rm \
                --tty \
                --user=${USER_ID}:${GROUP_ID} \
                --volume $(pwd):$(pwd) \
                --volume ${HOME_DIR}:${HOME_DIR} \
                --volume /etc/passwd:/etc/passwd \
                --volume /etc/group:/etc/group \
                --workdir $(pwd) \
                kurron/docker-amazon-jdk-8 \
		./gradlew --console=plain \
                          --no-daemon \
                          --stacktrace \
                          bootRun"
echo $CMD
$CMD

