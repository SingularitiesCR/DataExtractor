FROM maven:3.5-jdk-8 AS build
WORKDIR /usr/src/app
COPY pom.xml .
RUN mvn dependency:go-offline
COPY src .
RUN mvn package

FROM openjdk:8
ENV ARTIFACT_NAME=dataextractor.jar
WORKDIR /usr/app
COPY --from=build /usr/src/app/target/${ARTIFACT_NAME} .
ENTRYPOINT [ \
  "java", "-cp", "${ARTIFACT_NAME}", \
  "com.singularities.dataextractor.DataExtractor" ]
