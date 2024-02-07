plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.amazonaws:aws-lambda-java-core:1.2.3")
    //implementation("com.amazonaws:aws-java-sdk-glue:1.12.651")
    implementation("com.amazonaws:aws-java-sdk-kinesis:1.12.651")
    implementation("com.amazonaws:aws-lambda-java-events:3.11.4")
    implementation("com.amazonaws:aws-java-sdk-s3:1.12.647")
    implementation("software.amazon.glue:schema-registry-serde:1.1.18")

    implementation("com.fasterxml.jackson.core:jackson-databind:2.16.1")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-xml:2.16.1")

    compileOnly("org.projectlombok:lombok:1.18.30")

    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}

tasks.register<Zip>("packageJar") {
    into("lib") {
        from(tasks.named("jar"))
        from(configurations.runtimeClasspath)
    }
}