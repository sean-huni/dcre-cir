FROM eclipse-temurin:25-jre-alpine
COPY build/libs/cir-2.0.1.jar /app.jar
ENTRYPOINT ["java","-jar","/app.jar"]
