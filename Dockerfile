FROM eclipse-temurin:25-jre-alpine
COPY build/libs/dcre-cir-1.0.jar /app.jar
ENTRYPOINT ["java","-jar","/app.jar"]
