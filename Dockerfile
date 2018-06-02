FROM maven:3.5-jdk-8 AS build
WORKDIR /usr/src/app
COPY pom.xml pom.xml
RUN mvn dependency:go-offline
COPY src src
RUN mvn package

FROM openjdk:8
WORKDIR /usr/app
COPY --from=build /usr/src/app/target/dataextractor.jar app.jar
ENTRYPOINT [ \
  "java", "-cp", "app.jar", \
  "com.singularities.dataextractor.DataExtractor" ]
