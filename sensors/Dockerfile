FROM openjdk:11.0-jdk

ARG SERVICE_NAME=myservice
ENTRYPOINT ["/usr/bin/java", "-jar", "/${SERVICE_NAME}.jar"]

# Add the service itself
ARG JAR_FILE
ADD target/${JAR_FILE} /${SERVICE_NAME}.jar