FROM maven:3.9.9-eclipse-temurin-17 AS build

WORKDIR /workspace

COPY pom.xml .
COPY event-contracts/pom.xml event-contracts/pom.xml
COPY inventory-service/pom.xml inventory-service/pom.xml
COPY order-service/pom.xml order-service/pom.xml
COPY payment-service/pom.xml payment-service/pom.xml

COPY event-contracts/src event-contracts/src
COPY inventory-service/src inventory-service/src

RUN mvn -pl inventory-service -am clean package -DskipTests

RUN cp inventory-service/target/*.jar app.jar


FROM eclipse-temurin:17-jre

WORKDIR /app

COPY --from=build /workspace/app.jar app.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]