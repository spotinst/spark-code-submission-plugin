plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.spark:spark-core_2.12:3.3.2")
    implementation("org.apache.spark:spark-sql_2.12:3.3.2")
    implementation("io.undertow:undertow-core:2.3.4.Final")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.14.2")
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}